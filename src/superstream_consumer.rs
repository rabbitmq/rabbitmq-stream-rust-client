use crate::client::Client;
use crate::consumer::Delivery;
use crate::error::{ConsumerCloseError, ConsumerDeliveryError};
use crate::superstream::DefaultSuperStreamMetadata;
use crate::{error::ConsumerCreateError, ConsumerHandle, Environment, FilterConfiguration};
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt};
use rabbitmq_stream_protocol::commands::subscribe::OffsetSpecification;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task;

/// API for consuming RabbitMQ stream messages
pub struct SuperStreamConsumer {
    internal: Arc<SuperStreamConsumerInternal>,
    receiver: Receiver<Result<Delivery, ConsumerDeliveryError>>,
}

struct SuperStreamConsumerInternal {
    closed: Arc<AtomicBool>,
    handlers: Vec<ConsumerHandle>,
    waker: AtomicWaker,
    client: Client,
}

/// Builder for [`Consumer`]
pub struct SuperStreamConsumerBuilder {
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
    pub(crate) filter_configuration: Option<FilterConfiguration>,
    pub(crate) client_provided_name: String,
}

impl SuperStreamConsumerBuilder {
    pub async fn build(
        self,
        super_stream: &str,
    ) -> Result<SuperStreamConsumer, ConsumerCreateError> {
        // Connect to the user specified node first, then look for a random replica to connect to instead.
        // This is recommended for load balancing purposes.
        let client = self.environment.create_client().await?;
        let (tx, rx) = channel(10000);

        let mut super_stream_metadata = DefaultSuperStreamMetadata {
            super_stream: super_stream.to_string(),
            client: client.clone(),
            partitions: Vec::new(),
            routes: Vec::new(),
        };
        let partitions = super_stream_metadata.partitions().await;

        let mut handlers = Vec::<ConsumerHandle>::new();
        for partition in partitions.into_iter() {
            let tx_cloned = tx.clone();
            let mut consumer = self
                .environment
                .consumer()
                .offset(self.offset_specification.clone())
                .client_provided_name(self.client_provided_name.as_str())
                .filter_input(self.filter_configuration.clone())
                .build(partition.as_str())
                .await
                .unwrap();

            handlers.push(consumer.handle());

            task::spawn(async move {
                while let Some(d) = consumer.next().await {
                    _ = tx_cloned.send(d).await;
                }
            });
        }

        let super_stream_consumer_internal = SuperStreamConsumerInternal {
            closed: Arc::new(AtomicBool::new(false)),
            handlers,
            waker: AtomicWaker::new(),
            client,
        };

        Ok(SuperStreamConsumer {
            internal: Arc::new(super_stream_consumer_internal),
            receiver: rx,
        })
    }

    pub fn offset(mut self, offset_specification: OffsetSpecification) -> Self {
        self.offset_specification = offset_specification;
        self
    }

    pub fn filter_input(mut self, filter_configuration: Option<FilterConfiguration>) -> Self {
        self.filter_configuration = filter_configuration;
        self
    }

    pub fn client_provided_name(mut self, name: &str) -> Self {
        self.client_provided_name = String::from(name);
        self
    }
}

impl Stream for SuperStreamConsumer {
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

impl SuperStreamConsumer {
    /// Check if the consumer is closed
    pub fn is_closed(&self) -> bool {
        self.internal.is_closed()
    }

    pub fn handle(&self) -> SuperStreamConsumerHandle {
        SuperStreamConsumerHandle(self.internal.clone())
    }
}

impl SuperStreamConsumerInternal {
    fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

pub struct SuperStreamConsumerHandle(Arc<SuperStreamConsumerInternal>);

impl SuperStreamConsumerHandle {
    /// Close the [`Consumer`] associated to this handle
    pub async fn close(self) -> Result<(), ConsumerCloseError> {
        self.0.waker.wake();
        match self.0.closed.compare_exchange(false, true, SeqCst, SeqCst) {
            Ok(false) => {
                for handle in &self.0.handlers {
                    handle.internal_close().await.unwrap();
                }
                self.0.client.close().await?;
                Ok(())
            }
            _ => Err(ConsumerCloseError::AlreadyClosed),
        }
    }
}
