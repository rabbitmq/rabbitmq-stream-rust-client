use crate::client::Client;
use crate::consumer::{ConsumerUpdateListener, Delivery};
use crate::error::{ConsumerCloseError, ConsumerDeliveryError};
use crate::superstream::DefaultSuperStreamMetadata;
use crate::{
    error::ConsumerCreateError, ConsumerHandle, Environment, FilterConfiguration, MessageContext,
};
use futures::task::AtomicWaker;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use rabbitmq_stream_protocol::commands::subscribe::OffsetSpecification;
use std::collections::HashMap;
use std::future::Future;
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
    pub(crate) super_stream_consumer_name: Option<String>,
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
    pub(crate) filter_configuration: Option<FilterConfiguration>,
    pub(crate) consumer_update_listener: Option<ConsumerUpdateListener>,
    pub(crate) client_provided_name: String,
    pub(crate) is_single_active_consumer: bool,
    pub(crate) properties: HashMap<String, String>,
}

impl SuperStreamConsumerBuilder {
    pub async fn build(
        &mut self,
        super_stream: &str,
    ) -> Result<SuperStreamConsumer, ConsumerCreateError> {
        // Connect to the user specified node first, then look for a random replica to connect to instead.
        // This is recommended for load balancing purposes.
        if (self.is_single_active_consumer
            || self.properties.contains_key("single-active-consumer"))
            && self.super_stream_consumer_name.is_none()
        {
            return Err(ConsumerCreateError::SingleActiveConsumerNotSupported);
        }

        let client = self.environment.create_client().await?;
        let (tx, rx) = channel(10000);

        let mut super_stream_metadata = DefaultSuperStreamMetadata {
            super_stream: super_stream.to_string(),
            client: client.clone(),
            partitions: Vec::new(),
            routes: HashMap::new(),
        };
        let partitions = super_stream_metadata.partitions().await;

        if self.is_single_active_consumer {
            self.properties
                .insert("super-stream".to_string(), super_stream.to_string());
        }

        let mut handlers = Vec::<ConsumerHandle>::new();
        for partition in partitions.into_iter() {
            let tx_cloned = tx.clone();
            let mut consumer = self
                .environment
                .consumer()
                .name_optional(self.super_stream_consumer_name.clone())
                .offset(self.offset_specification.clone())
                .client_provided_name(self.client_provided_name.as_str())
                .filter_input(self.filter_configuration.clone())
                .consumer_update_arc(self.consumer_update_listener.clone())
                .properties(self.properties.clone())
                .enable_single_active_consumer(self.is_single_active_consumer)
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

    pub fn name(mut self, consumer_name: &str) -> Self {
        self.super_stream_consumer_name = Some(String::from(consumer_name));
        self
    }

    pub fn enable_single_active_consumer(mut self, is_single_active_consumer: bool) -> Self {
        self.is_single_active_consumer = is_single_active_consumer;
        self
    }

    pub fn filter_input(mut self, filter_configuration: Option<FilterConfiguration>) -> Self {
        self.filter_configuration = filter_configuration;
        self
    }
    pub fn consumer_update<Fut>(
        mut self,
        consumer_update_listener: impl Fn(u8, MessageContext) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = OffsetSpecification> + Send + Sync + 'static,
    {
        let f = Arc::new(move |a, b| consumer_update_listener(a, b).boxed());
        self.consumer_update_listener = Some(f);
        self
    }

    pub fn client_provided_name(mut self, name: &str) -> Self {
        self.client_provided_name = String::from(name);
        self
    }

    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
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

    pub fn client(&self) -> Client {
        self.internal.client.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SuperStreamConsumer>();
    }
}
