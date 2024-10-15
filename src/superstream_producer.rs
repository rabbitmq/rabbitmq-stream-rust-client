use crate::error::ProducerCloseError;
use crate::{
    client::Client,
    environment::Environment,
    error::{ProducerCreateError, ProducerPublishError},
    producer::{ConfirmationStatus, NoDedup, Producer},
    superstream::{DefaultSuperStreamMetadata, RoutingStrategy},
};
use rabbitmq_stream_protocol::message::Message;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

type FilterValueExtractor = Arc<dyn Fn(&Message) -> String + 'static + Send + Sync>;

#[derive(Clone)]
pub struct SuperStreamProducer<T>(
    Arc<SuperStreamProducerInternal>,
    HashMap<String, Producer<T>>,
    DefaultSuperStreamMetadata,
    PhantomData<T>,
);

/// Builder for [`SuperStreamProducer`]
pub struct SuperStreamProducerBuilder<T> {
    pub(crate) environment: Environment,
    //pub filter_value_extractor: Option<FilterValueExtractor>,
    pub routing_strategy: RoutingStrategy,
    pub(crate) data: PhantomData<T>,
}

pub struct SuperStreamProducerInternal {
    pub(crate) environment: Environment,
    client: Client,
    // TODO: implement filtering for superstream
    //filter_value_extractor: Option<FilterValueExtractor>,
    routing_strategy: RoutingStrategy,
}

impl SuperStreamProducer<NoDedup> {
    pub async fn send<Fut>(
        &mut self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut
            + Send
            + Sync
            + 'static
            + Clone,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        let routes = match self.0.routing_strategy.clone() {
            RoutingStrategy::HashRoutingStrategy(routing_strategy) => {
                routing_strategy.routes(message.clone(), &mut self.2).await
            }
            RoutingStrategy::RoutingKeyStrategy(routing_strategy) => {
                routing_strategy.routes(message.clone(), &mut self.2).await
            }
        };

        for route in routes.into_iter() {
            if !self.1.contains_key(route.as_str()) {
                    let producer = self.0.environment.producer().build(route.as_str()).await;
                    self.1.insert(route.clone(), producer.unwrap());
            }

            let producer = self.1.get(route.as_str()).unwrap();
            let result = producer.send(message.clone(), cb.clone()).await?;
        }
        Ok(())
    }


    pub async fn close(self) -> Result<(), ProducerCloseError> {
        self.0.client.close().await?;

        let mut err: Option<ProducerCloseError> = None;
        let mut is_error = false;
        for (_, producer) in self.1.into_iter() {
            let close = producer.close().await;
            match close {
                Err(e) => {
                    is_error = true;
                    err = Some(e);
                }
                _ => (),
            }
        }

        if is_error == false {
            return Ok(());
        } else {
            return Err(err.unwrap());
        }
    }
}

impl<T> SuperStreamProducerBuilder<T> {
    pub async fn build(
        self,
        super_stream: &str,
    ) -> Result<SuperStreamProducer<T>, ProducerCreateError> {
        // Connect to the user specified node first, then look for the stream leader.
        // The leader is the recommended node for writing, because writing to a replica will redundantly pass these messages
        // to the leader anyway - it is the only one capable of writing.
        let client = self.environment.create_client().await?;

        let producers = HashMap::new();

        let super_stream_metadata = DefaultSuperStreamMetadata {
            super_stream: super_stream.to_string(),
            client: self.environment.create_client().await?,
            partitions: Vec::new(),
            routes: Vec::new(),
        };

        let super_stream_producer = SuperStreamProducerInternal {
            environment: self.environment.clone(),
            client,
            //filter_value_extractor: self.filter_value_extractor,
            routing_strategy: self.routing_strategy,
        };

        let internal_producer = Arc::new(super_stream_producer);
        let super_stream_producer = SuperStreamProducer(
            internal_producer.clone(),
            producers,
            super_stream_metadata,
            PhantomData,
        );

        Ok(super_stream_producer)
    }
}
