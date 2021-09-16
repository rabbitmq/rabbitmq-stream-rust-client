use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc,
    },
};

use futures::{future::BoxFuture, FutureExt};
use rabbitmq_stream_protocol::{message::Message, Response, ResponseCode, ResponseKind};
use std::future::Future;
use tokio::sync::{
    oneshot::{channel, Receiver, Sender},
    Mutex,
};

use crate::{
    client::Client,
    environment::Environment,
    error::{ClientError, ProducerCloseError, ProducerCreateError, ProducerPublishError},
};
use crate::{client::MessageHandler, RabbitMQStreamResult};

type WaiterMap = Arc<Mutex<HashMap<u64, ProducerMessageWaiter>>>;

pub struct ProducerInternal {
    client: Client,
    stream: String,
    producer_id: u8,
    publish_sequence: Arc<AtomicU64>,
    waiting_confirmations: WaiterMap,
    closed: Arc<AtomicBool>,
}

/// API for publising messages to RabbitMQ stream
#[derive(Clone)]
pub struct Producer(Arc<ProducerInternal>);

/// Builder for [`Producer`]
pub struct ProducerBuilder {
    pub(crate) environment: Environment,
    pub(crate) name: Option<String>,
}

impl ProducerBuilder {
    pub async fn build(self, stream: &str) -> Result<Producer, ProducerCreateError> {
        let client = self.environment.create_client().await?;

        let waiting_confirmations: WaiterMap = Arc::new(Mutex::new(HashMap::new()));

        let confirm_handler = ProducerConfirmHandler {
            waiting_confirmations: waiting_confirmations.clone(),
        };

        client.set_handler(confirm_handler).await;

        let producer_id = 1;
        let response = client
            .declare_publisher(producer_id, self.name.clone(), stream)
            .await?;

        let publish_sequence = if let Some(name) = self.name {
            let sequence = client.query_publisher_sequence(&name, stream).await?;
            Arc::new(AtomicU64::new(sequence))
        } else {
            Arc::new(AtomicU64::new(0))
        };

        if response.is_ok() {
            let producer = ProducerInternal {
                producer_id,
                stream: stream.to_string(),
                client,
                publish_sequence,
                waiting_confirmations,
                closed: Arc::new(AtomicBool::new(false)),
            };
            Ok(Producer(Arc::new(producer)))
        } else {
            Err(ProducerCreateError::Create {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }
}

impl Producer {
    pub async fn send(&self, message: Message) -> Result<u64, ProducerPublishError> {
        let (publishing_id, rx) = self.internal_send(message).await?;

        rx.await
            .map_err(|err| ClientError::GenericError(Box::new(err)))?
            .map(|_| publishing_id)
    }

    pub async fn send_with_callback<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<u64, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync,
    {
        let (publishing_id, rx) = self.internal_send(message).await?;
        tokio::task::spawn(async move {
            let result = rx.await;
            match result {
                Ok(confirm_status) => cb(confirm_status.map(|_| publishing_id)).await,
                Err(err) => cb(Err(ClientError::GenericError(Box::new(err)).into())).await,
            };
        });
        Ok(())
    }

    async fn internal_send(
        &self,
        mut message: Message,
    ) -> Result<(u64, Receiver<Result<(), ProducerPublishError>>), ProducerPublishError> {
        if self.is_closed() {
            return Err(ProducerPublishError::Closed);
        }
        let publishing_id = match message.publishing_id() {
            Some(publishing_id) => *publishing_id,
            None => self.0.publish_sequence.fetch_add(1, Relaxed),
        };
        message.set_publishing_id(publishing_id);

        let (rx, waiter) = ProducerMessageWaiter::waiter(self.0.stream.clone(), self.0.producer_id);
        let mut waiting_confirmation = self.0.waiting_confirmations.lock().await;
        waiting_confirmation.insert(publishing_id, waiter);
        drop(waiting_confirmation);
        self.0.client.publish(self.0.producer_id, message).await?;

        Ok((publishing_id, rx))
    }

    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Relaxed)
    }
    // TODO handle producer state after close
    pub async fn close(self) -> Result<(), ProducerCloseError> {
        if self.is_closed() {
            return Err(ProducerCloseError::AlreadyClosed);
        }
        let response = self.0.client.delete_publisher(self.0.producer_id).await?;

        if response.is_ok() {
            self.0.closed.store(true, Relaxed);
            Ok(())
        } else {
            Err(ProducerCloseError::Close {
                status: response.code().clone(),
                stream: self.0.stream.clone(),
            })
        }
    }
}

struct ProducerConfirmHandler {
    waiting_confirmations: WaiterMap,
}

impl ProducerConfirmHandler {
    async fn with_waiter(
        &self,
        publishing_id: u64,
        cb: impl FnOnce(ProducerMessageWaiter) -> BoxFuture<'static, ()>,
    ) {
        let mut conf_guard = self.waiting_confirmations.lock().await;
        match conf_guard.remove(&publishing_id) {
            Some(confirm_sender) => cb(confirm_sender).await,
            None => todo!(),
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for ProducerConfirmHandler {
    async fn handle_message(&self, response: Response) -> RabbitMQStreamResult<()> {
        match response.kind() {
            ResponseKind::PublishConfirm(confirm) => {
                for publishing_id in &confirm.publishing_ids {
                    self.with_waiter(*publishing_id, |waiter| {
                        async {
                            let _ = waiter.handle_confirm().await;
                        }
                        .boxed()
                    })
                    .await;
                }
            }
            ResponseKind::PublishError(error) => {
                for err in &error.publishing_errors {
                    let code = err.error_code.clone();
                    self.with_waiter(err.publishing_id, move |waiter| {
                        async {
                            let _ = waiter.handle_error(code).await;
                        }
                        .boxed()
                    })
                    .await;
                }
            }
            _ => {}
        };
        Ok(())
    }
}

struct ProducerMessageWaiter {
    stream: String,
    publisher_id: u8,
    tx: Sender<Result<(), ProducerPublishError>>,
}

impl ProducerMessageWaiter {
    fn waiter(
        stream: String,
        publisher_id: u8,
    ) -> (Receiver<Result<(), ProducerPublishError>>, Self) {
        let (tx, rx) = channel();

        (
            rx,
            Self {
                stream,
                publisher_id,
                tx,
            },
        )
    }

    async fn handle_confirm(self) -> RabbitMQStreamResult<()> {
        let _ = self.tx.send(Ok(()));
        Ok(())
    }
    async fn handle_error(self, status: ResponseCode) -> RabbitMQStreamResult<()> {
        let _ = self.tx.send(Err(ProducerPublishError::Create {
            stream: self.stream,
            publisher_id: self.publisher_id,
            status,
        }));
        Ok(())
    }
}
