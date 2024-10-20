use crate::superstream::DefaultSuperStreamMetadata;
use crate::{error::ConsumerCreateError, Consumer, Environment};
use rabbitmq_stream_protocol::commands::subscribe::OffsetSpecification;
use std::sync::Arc;
//type FilterPredicate = Option<Arc<dyn Fn(&Message) -> bool + Send + Sync>>;

/// API for consuming RabbitMQ stream messages
#[derive(Clone)]
pub struct SuperStreamConsumer {
    internal: Arc<SuperStreamConsumerInternal>,
}

struct SuperStreamConsumerInternal {
    offset_specification: OffsetSpecification,
    consumers: Vec<Consumer>,
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

        let mut super_stream_metadata = DefaultSuperStreamMetadata {
            super_stream: super_stream.to_string(),
            client: client.clone(),
            partitions: Vec::new(),
            routes: Vec::new(),
        };
        let partitions = super_stream_metadata.partitions().await;
        let mut consumers: Vec<Consumer> = Vec::new();

        for partition in partitions.into_iter() {
            let consumer = self
                .environment
                .consumer()
                .offset(self.offset_specification.clone())
                .build(partition.as_str())
                .await
                .unwrap();

            consumers.push(consumer);
        }

        let super_stream_consumer_internal = Arc::new(SuperStreamConsumerInternal {
            offset_specification: self.offset_specification.clone(),
            consumers,
        });

        Ok(SuperStreamConsumer {
            internal: super_stream_consumer_internal,
        })
    }

    pub fn offset(mut self, offset_specification: OffsetSpecification) -> Self {
        self.offset_specification = offset_specification;
        self
    }
}

impl SuperStreamConsumer {
    pub async fn get_consumer(&self, i: usize) -> &Consumer {
        return self.internal.consumers.get(i).unwrap();
    }

    pub async fn get_consumers(&mut self) -> Vec<Consumer> {
        self.internal.consumers.clone()
    }
}
