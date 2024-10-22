use crate::consumer::Delivery;
use crate::error::ConsumerDeliveryError;
use crate::superstream::DefaultSuperStreamMetadata;
use crate::{error::ConsumerCreateError, Environment};
use futures::{Stream, StreamExt};
use rabbitmq_stream_protocol::commands::subscribe::OffsetSpecification;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task;

//type FilterPredicate = Option<Arc<dyn Fn(&Message) -> bool + Send + Sync>>;

/// API for consuming RabbitMQ stream messages
pub struct SuperStreamConsumer {
    internal: SuperStreamConsumerInternal,
}

struct SuperStreamConsumerInternal {
    receiver: Receiver<Result<Delivery, ConsumerDeliveryError>>,
}

/// Builder for [`Consumer`]
pub struct SuperStreamConsumerBuilder {
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
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

        for partition in partitions.into_iter() {
            let tx_cloned = tx.clone();
            let mut consumer = self
                .environment
                .consumer()
                .offset(self.offset_specification.clone())
                .build(partition.as_str())
                .await
                .unwrap();

            task::spawn(async move {
                while let Some(d) = consumer.next().await {
                    _ = tx_cloned.send(d).await;
                }
            });
        }

        let super_stream_consumer_internal = SuperStreamConsumerInternal { receiver: rx };

        Ok(SuperStreamConsumer {
            internal: super_stream_consumer_internal,
        })
    }

    pub fn offset(mut self, offset_specification: OffsetSpecification) -> Self {
        self.offset_specification = offset_specification;
        self
    }
}

impl Stream for SuperStreamConsumer {
    type Item = Result<Delivery, ConsumerDeliveryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.internal.receiver).poll_recv(cx)
    }
}
