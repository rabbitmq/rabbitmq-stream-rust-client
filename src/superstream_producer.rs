use crate::{
    client::Client,
    environment::Environment,
    error::{ProducerCreateError, ProducerPublishError},
    producer::{ConfirmationStatus, Producer},
    superstream::{RouteType, RoutingStrategy},
};
use rabbitmq_stream_protocol::message::Message;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

type FilterValueExtractor = Arc<dyn Fn(&Message) -> String + 'static + Send + Sync>;

#[derive(Clone)]
pub struct NoDedup {}

pub struct Dedup {}

#[derive(Clone)]
pub struct SuperStreamProducer<T>(Arc<SuperStreamProducerInternal>, PhantomData<T>);

/// Builder for [`SuperStreamProducer`]
pub struct SuperStreamProducerBuilder<'a> {
    pub(crate) environment: Environment,
    pub(crate) name: Option<String>,
    pub filter_value_extractor: Option<FilterValueExtractor>,
    pub routing_strategy: &'a dyn RoutingStrategy,
}

pub struct SuperStreamProducerInternal<'a> {
    client: Client,
    super_stream: String,
    publish_version: u16,
    producers: Vec<Producer()>,
    filter_value_extractor: Option<FilterValueExtractor>,
    routing_strategy: &'a dyn RoutingStrategy,
}

impl SuperStreamProducer<NoDedup> {
    pub async fn send<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.do_send(message, cb).await
    }
}

impl SuperStreamProducer<Dedup> {
    pub async fn send<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.do_send(message, cb).await
    }
}

impl<T> SuperStreamProducerBuilder<T> {
    pub async fn build(
        self,
        super_stream: &str,
        route_type: RouteType,
    ) -> Result<SuperStreamProducer<T>, ProducerCreateError> {
        // Connect to the user specified node first, then look for the stream leader.
        // The leader is the recommended node for writing, because writing to a replica will redundantly pass these messages
        // to the leader anyway - it is the only one capable of writing.
        let mut client = self.environment.create_client().await?;

        let mut publish_version = 1;

        if self.filter_value_extractor.is_some() {
            if client.filtering_supported() {
                publish_version = 2
            } else {
                return Err(ProducerCreateError::FilteringNotSupport);
            }
        }

        let producers = Vec::new();

        let super_stream_producer = SuperStreamProducerInternal {
            super_stream: super_stream.to_string(),
            client,
            publish_version,
            filter_value_extractor: self.filter_value_extractor,
            routing_strategy: route_type,
            producers,
        };

        let internal_producer = Arc::new(super_stream_producer);
        let super_stream_producer = SuperStreamProducer(internal_producer.clone(), PhantomData);

        Ok(super_stream_producer)
    }
}
