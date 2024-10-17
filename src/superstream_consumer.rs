use std::sync::Arc;

use rabbitmq_stream_protocol::commands::subscribe::OffsetSpecification;

use crate::superstream::DefaultSuperStreamMetadata;
use crate::{error::ConsumerCreateError, Client, Consumer, Environment};

//type FilterPredicate = Option<Arc<dyn Fn(&Message) -> bool + Send + Sync>>;

/// API for consuming RabbitMQ stream messages
#[derive(Clone)]
pub struct SuperStreamConsumer {
    internal: Arc<SuperStreamConsumerInternal>,
}

struct SuperStreamConsumerInternal {
    client: Client,
    super_stream: String,
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
            super_stream: super_stream.to_string(),
            client: client.clone(),
            offset_specification: self.offset_specification.clone(),
            consumers,
        });

        Ok(SuperStreamConsumer {
            internal: super_stream_consumer_internal,
        })
    }

    pub async fn offset(mut self, offset_specification: OffsetSpecification) -> Self {
        self.offset_specification = offset_specification;
        self
    }
}

impl SuperStreamConsumer {
    pub async fn get_consumers(&self) -> &Vec<Consumer> {
        return &self.internal.consumers;
    }
}
