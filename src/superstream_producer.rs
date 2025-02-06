use crate::error::ProducerCloseError;
use crate::{
    client::Client,
    environment::Environment,
    error::{ProducerCreateError, ProducerPublishError, SuperStreamProducerPublishError},
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
    RoutingStrategy,
);

/// Builder for [`SuperStreamProducer`]
pub struct SuperStreamProducerBuilder<T> {
    pub(crate) environment: Environment,
    pub filter_value_extractor: Option<FilterValueExtractor>,
    pub route_strategy: RoutingStrategy,
    pub(crate) data: PhantomData<T>,
    pub(crate) client_provided_name: String,
}

pub struct SuperStreamProducerInternal {
    pub(crate) environment: Environment,
    client: Client,
    // TODO: implement filtering for superstream
    filter_value_extractor: Option<FilterValueExtractor>,
    client_provided_name: String,
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
    ) -> Result<(), SuperStreamProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        let routes = match &self.4 {
            RoutingStrategy::HashRoutingStrategy(routing_strategy) => {
                routing_strategy.routes(&message, &mut self.2).await
            }
            RoutingStrategy::RoutingKeyStrategy(routing_strategy) => {
                routing_strategy.routes(&message, &mut self.2).await
            }
        };

        if routes.is_empty() {
            return Err(crate::error::SuperStreamProducerPublishError::ProducerCreateError());
        }

        for route in routes.into_iter() {
            if !self.1.contains_key(route.as_str()) {
                let producer = self
                    .0
                    .environment
                    .producer()
                    .client_provided_name(self.0.client_provided_name.as_str())
                    .filter_value_extractor_arc(self.0.filter_value_extractor.clone())
                    .build(route.as_str())
                    .await?;
                self.1.insert(route.clone(), producer);
            }

            let producer = self.1.get(&route).unwrap();
            producer.send(message.clone(), cb.clone()).await?;
        }
        Ok(())
    }

    pub async fn close(self) -> Result<(), ProducerCloseError> {
        self.0.client.close().await?;

        let mut err: Option<ProducerCloseError> = None;
        let mut is_error = false;
        for (_, producer) in self.1.into_iter() {
            let close = producer.close().await;
            if let Err(e) = close {
                is_error = true;
                err = Some(e);
            }
        }

        if !is_error {
            Ok(())
        } else {
            Err(err.unwrap())
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
            client: client.clone(),
            partitions: Vec::new(),
            routes: HashMap::new(),
        };

        let super_stream_producer = SuperStreamProducerInternal {
            environment: self.environment.clone(),
            client,
            filter_value_extractor: self.filter_value_extractor,
            client_provided_name: self.client_provided_name,
        };

        let internal_producer = Arc::new(super_stream_producer);
        let super_stream_producer = SuperStreamProducer(
            internal_producer.clone(),
            producers,
            super_stream_metadata,
            PhantomData,
            self.route_strategy,
        );

        Ok(super_stream_producer)
    }

    pub fn filter_value_extractor(
        mut self,
        filter_value_extractor: impl Fn(&Message) -> String + Send + Sync + 'static,
    ) -> Self {
        let f = Arc::new(filter_value_extractor);
        self.filter_value_extractor = Some(f);
        self
    }

    pub fn client_provided_name(mut self, name: &str) -> Self {
        self.client_provided_name = String::from(name);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::Dedup;

    use super::*;

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<SuperStreamProducer<NoDedup>>();
        assert_send_sync::<SuperStreamProducer<Dedup>>();
    }
}
