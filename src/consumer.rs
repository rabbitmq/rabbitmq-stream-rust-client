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
    Client, ClientOptions, Environment,
};
use futures::{task::AtomicWaker, Stream};

use rand::seq::SliceRandom;

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
        // Connect to the user specified node first, then look for a random replica to connect to instead.
        // This is recommended for load balancing purposes.
        let mut client = self.environment.create_client().await?;
        if let Some(metadata) = client.metadata(vec![stream.to_string()]).await?.get(stream) {
            // If there are no replicas we do not reassign client, meaning we just keep reading from the leader.
            // This is desired behavior in case there is only one node in the cluster.
            if let Some(replica) = metadata.replicas.choose(&mut rand::thread_rng()) {
                tracing::debug!(
                    "Picked replica {:?} out of possible candidates {:?} for stream {}",
                    replica,
                    metadata.replicas,
                    stream
                );
                client = Client::connect(ClientOptions {
                    host: replica.host.clone(),
                    port: replica.port as u16,
                    ..self.environment.options.client_options
                })
                .await?;
            }
        } else {
            return Err(ConsumerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

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
                    let mut offset = delivery.chunk_first_offset;

                    trace!("Got delivery with messages {}", delivery.messages.len());
                    for message in delivery.messages {
                        let _ = self
                            .0
                            .sender
                            .send(Ok(Delivery {
                                subscription_id: self.0.subscription_id,
                                message,
                                offset,
                            }))
                            .await;
                        offset += 1;
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
    subscription_id: u8,
    message: Message,
    offset: u64,
}

impl Delivery {
    /// Get a reference to the delivery's subscription id.
    pub fn subscription_id(&self) -> u8 {
        self.subscription_id
    }

    /// Get a reference to the delivery's message.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Get a reference to the delivery's offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}
