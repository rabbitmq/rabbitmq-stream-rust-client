use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{
            AtomicBool,
            Ordering::{Relaxed, SeqCst},
        },
        Arc,
    },
    task::{Context, Poll},
};

use rabbitmq_stream_protocol::{
    commands::subscribe::OffsetSpecification, message::Message, ResponseKind,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::trace;

use crate::{
    client::{MessageHandler, MessageResult},
    error::{ConsumerCloseError, ConsumerCreateError, ConsumerDeliveryError},
    Client, Environment,
};
use futures::{task::AtomicWaker, Stream};

/// API for consuming RabbitMQ stream messages
pub struct Consumer {
    receiver: Receiver<Result<Delivery, ConsumerDeliveryError>>,
    internal: Arc<ConsumerInternal>,
}

struct ConsumerInternal {
    client: Client,
    stream: String,
    subscription_id: u8,
    sender: Sender<Result<Delivery, ConsumerDeliveryError>>,
    closed: Arc<AtomicBool>,
    waker: AtomicWaker,
}

impl ConsumerInternal {
    fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

/// Builder for [`Consumer`]
pub struct ConsumerBuilder {
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
}

impl ConsumerBuilder {
    pub async fn build(self, stream: &str) -> Result<Consumer, ConsumerCreateError> {
        let client = self.environment.create_client().await?;

        let subscription_id = 1;
        let response = client
            .subscribe(
                subscription_id,
                stream,
                self.offset_specification,
                1,
                HashMap::new(),
            )
            .await?;

        if response.is_ok() {
            let (tx, rx) = channel(10000);
            let consumer = Arc::new(ConsumerInternal {
                subscription_id,
                stream: stream.to_string(),
                client: client.clone(),
                sender: tx,
                closed: Arc::new(AtomicBool::new(false)),
                waker: AtomicWaker::new(),
            });

            let msg_handler = ConsumerMessageHandler(consumer.clone());
            client.set_handler(msg_handler).await;

            Ok(Consumer {
                receiver: rx,
                internal: consumer,
            })
        } else {
            Err(ConsumerCreateError::Create {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub fn offset(mut self, offset_specification: OffsetSpecification) -> Self {
        self.offset_specification = offset_specification;
        self
    }
}

impl Consumer {
    /// Return an handle for current [`Consumer`]
    pub fn handle(&self) -> ConsumerHandle {
        ConsumerHandle(self.internal.clone())
    }

    /// Check if the consumer is closed
    pub fn is_closed(&self) -> bool {
        self.internal.is_closed()
    }
}

impl Stream for Consumer {
    type Item = Result<Delivery, ConsumerDeliveryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.internal.waker.register(cx.waker());
        let poll = Pin::new(&mut self.receiver).poll_recv(cx);
        match (self.is_closed(), poll.is_ready()) {
            (true, false) => Poll::Ready(None),
            _ => poll,
        }
    }
}

/// Handler API for [`Consumer`]
pub struct ConsumerHandle(Arc<ConsumerInternal>);

impl ConsumerHandle {
    /// Close the [`Consumer`] associated to this handle
    pub async fn close(self) -> Result<(), ConsumerCloseError> {
        match self.0.closed.compare_exchange(false, true, SeqCst, SeqCst) {
            Ok(false) => {
                let response = self.0.client.unsubscribe(self.0.subscription_id).await?;
                if response.is_ok() {
                    self.0.waker.wake();
                    Ok(())
                } else {
                    Err(ConsumerCloseError::Close {
                        stream: self.0.stream.clone(),
                        status: response.code().clone(),
                    })
                }
            }
            _ => Err(ConsumerCloseError::AlreadyClosed),
        }
    }
    /// Check if the consumer is closed
    pub async fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

struct ConsumerMessageHandler(Arc<ConsumerInternal>);

#[async_trait::async_trait]
impl MessageHandler for ConsumerMessageHandler {
    async fn handle_message(&self, item: MessageResult) -> crate::RabbitMQStreamResult<()> {
        match item {
            Some(Ok(response)) => {
                if let ResponseKind::Deliver(delivery) = response.kind() {
                    for message in delivery.messages {
                        let _ = self
                            .0
                            .sender
                            .send(Ok(Delivery {
                                subscription_id: self.0.subscription_id,
                                message,
                            }))
                            .await;
                    }
                }
                // TODO handle credit fail
                let _ = self.0.client.credit(self.0.subscription_id, 1).await;
            }
            Some(Err(err)) => {
                let _ = self.0.sender.send(Err(err.into())).await;
            }
            None => {
                trace!("Closing consumer");
                self.0.closed.store(true, Relaxed);
                self.0.waker.wake();
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
pub struct Delivery {
    pub subscription_id: u8,
    pub message: Message,
}
