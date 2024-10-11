use crate::{
    client::Client,
    environment::Environment,
    error::{ProducerCreateError, ProducerPublishError},
    producer::{ConfirmationStatus, Producer, NoDedup},
    superstream::{DefaultSuperStreamMetadata, RoutingStrategy},
};
use rabbitmq_stream_protocol::message::Message;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

type FilterValueExtractor = Arc<dyn Fn(&Message) -> String + 'static + Send + Sync>;

#[derive(Clone)]
pub struct SuperStreamProducer<T>(Arc<SuperStreamProducerInternal<T>>, PhantomData<T>);

/// Builder for [`SuperStreamProducer`]
pub struct SuperStreamProducerBuilder<T> {
    pub(crate) environment: Environment,
    pub(crate) name: Option<String>,
    pub filter_value_extractor: Option<FilterValueExtractor>,
    pub routing_strategy: RoutingStrategy,
    pub(crate) data: PhantomData<T>,
}

pub struct SuperStreamProducerInternal<T> {
    pub(crate) environment: Environment,
    client: Client,
    super_stream: String,
    publish_version: u16,
    producers: HashMap<String, Producer<T>>,
    filter_value_extractor: Option<FilterValueExtractor>,
    super_stream_metadata: DefaultSuperStreamMetadata,
    routing_strategy: RoutingStrategy,
}

impl SuperStreamProducer<NoDedup> {
    pub async fn send<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static + Clone,
    ) where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        let routes = match self.0.routing_strategy.clone() {
            RoutingStrategy::HashRoutingStrategy(routing_strategy) => {
                routing_strategy
                    .routes(message.clone(), &mut self.0.super_stream_metadata.clone())
                    .await
            }
            RoutingStrategy::RoutingKeyStrategy(routing_strategy) => {
                routing_strategy
                    .routes(message.clone(), &mut self.0.super_stream_metadata.clone())
                    .await
            }
        };

        for route in routes.into_iter() {
            if !self.0.producers.contains_key(route.as_str()) {
                let producer = self.0.environment.producer().build(route.as_str()).await;
                self.0.producers.clone().insert(route.clone(), producer.unwrap().clone());
            }

            let producer = self.0.producers.get(route.as_str()).unwrap();
            _ = producer
                .send(message.clone(), cb.clone())
                .await;
        }
    }
}

impl<T> SuperStreamProducerBuilder<T> {
    pub async fn build(
        self,
        super_stream: &str,
        route_type: RoutingStrategy,
    ) -> Result<SuperStreamProducer<T>, ProducerCreateError> {
        // Connect to the user specified node first, then look for the stream leader.
        // The leader is the recommended node for writing, because writing to a replica will redundantly pass these messages
        // to the leader anyway - it is the only one capable of writing.
        let client = self.environment.create_client().await?;

        let mut publish_version = 1;

        if self.filter_value_extractor.is_some() {
            if client.filtering_supported() {
                publish_version = 2
            } else {
                return Err(ProducerCreateError::FilteringNotSupport);
            }
        }

        let producers = HashMap::new();

        let super_stream_producer = SuperStreamProducerInternal {
            environment: self.environment.clone(),
            super_stream: super_stream.to_string(),
            client,
            publish_version,
            filter_value_extractor: self.filter_value_extractor,
            routing_strategy: route_type,
            super_stream_metadata: DefaultSuperStreamMetadata {
                super_stream: super_stream.to_string(),
                client: self.environment.create_client().await?,
                partitions: Vec::new(),
                routes: Vec::new(),
            },
            producers,
        };

        let internal_producer = Arc::new(super_stream_producer);
        let super_stream_producer = SuperStreamProducer(internal_producer.clone(), PhantomData);

        Ok(super_stream_producer)
    }
}
