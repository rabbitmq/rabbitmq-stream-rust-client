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

use core::option::Option::None;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::trace;

use crate::error::ConsumerStoreOffsetError;

use crate::{
    client::{MessageHandler, MessageResult},
    error::{ConsumerCloseError, ConsumerCreateError, ConsumerDeliveryError},
    Client, ClientOptions, Environment, MetricsCollector,
};
use futures::{task::AtomicWaker, Stream};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};

type FilterPredicate = Option<Arc<dyn Fn(&Message) -> bool + Send + Sync>>;

pub type ConsumerUpdateListener =
    Arc<dyn Fn(u8, &MessageContext) -> OffsetSpecification + Send + Sync>;

/// API for consuming RabbitMQ stream messages
pub struct Consumer {
    // Mandatory in case of manual offset tracking
    name: Option<String>,
    receiver: Receiver<Result<Delivery, ConsumerDeliveryError>>,
    internal: Arc<ConsumerInternal>,
}

struct ConsumerInternal {
    name: Option<String>,
    client: Client,
    stream: String,
    offset_specification: OffsetSpecification,
    subscription_id: u8,
    sender: Sender<Result<Delivery, ConsumerDeliveryError>>,
    closed: Arc<AtomicBool>,
    waker: AtomicWaker,
    metrics_collector: Arc<dyn MetricsCollector>,
    filter_configuration: Option<FilterConfiguration>,
    consumer_update_listener: Option<ConsumerUpdateListener>,
}

impl ConsumerInternal {
    fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

#[derive(Clone)]
pub struct FilterConfiguration {
    filter_values: Vec<String>,
    pub predicate: FilterPredicate,
    match_unfiltered: bool,
}

impl FilterConfiguration {
    pub fn new(filter_values: Vec<String>, match_unfiltered: bool) -> Self {
        Self {
            filter_values,
            match_unfiltered,
            predicate: None,
        }
    }

    pub fn post_filter(
        mut self,
        predicate: impl Fn(&Message) -> bool + 'static + Send + Sync,
    ) -> FilterConfiguration {
        self.predicate = Some(Arc::new(predicate));
        self
    }
}

pub struct MessageContext {
    consumer_name: Option<String>,
    stream: String,
}

impl MessageContext {
    pub fn get_name(self) -> Option<String> {
        self.consumer_name
    }

    pub fn get_stream(&self) -> String {
        self.stream.clone()
    }
}

/// Builder for [`Consumer`]
pub struct ConsumerBuilder {
    pub(crate) consumer_name: Option<String>,
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
    pub(crate) filter_configuration: Option<FilterConfiguration>,
    pub(crate) consumer_update_listener: Option<ConsumerUpdateListener>,
    pub(crate) client_provided_name: String,
    pub(crate) properties: HashMap<String, String>,
}

impl ConsumerBuilder {
    pub async fn build(mut self, stream: &str) -> Result<Consumer, ConsumerCreateError> {
        // Connect to the user specified node first, then look for a random replica to connect to instead.
        // This is recommended for load balancing purposes

        let mut opt_with_client_provided_name = self.environment.options.client_options.clone();
        opt_with_client_provided_name.client_provided_name = self.client_provided_name.clone();

        let mut client = self
            .environment
            .create_client_with_options(opt_with_client_provided_name)
            .await?;
        let collector = self.environment.options.client_options.collector.clone();
        if let Some(metadata) = client.metadata(vec![stream.to_string()]).await?.get(stream) {
            // If there are no replicas we do not reassign client, meaning we just keep reading from the leader.
            // This is desired behavior in case there is only one node in the cluster.
            if let Some(replica) = metadata.replicas.choose(&mut StdRng::from_entropy()) {
                tracing::debug!(
                    "Picked replica {:?} out of possible candidates {:?} for stream {}",
                    replica,
                    metadata.replicas,
                    stream
                );
                let load_balancer_mode = self.environment.options.client_options.load_balancer_mode;
                if load_balancer_mode {
                    let options = self.environment.options.client_options.clone();
                    loop {
                        let temp_client = Client::connect(options.clone()).await?;
                        let mapping = temp_client.connection_properties().await;
                        if let Some(advertised_host) = mapping.get("advertised_host") {
                            if *advertised_host == replica.host.clone() {
                                client.close().await?;
                                client = temp_client;
                                break;
                            }
                        }
                        temp_client.close().await?;
                    }
                } else {
                    client.close().await?;
                    client = Client::connect(ClientOptions {
                        host: replica.host.clone(),
                        port: replica.port as u16,
                        ..self.environment.options.client_options
                    })
                    .await?;
                }
            }
        } else {
            return Err(ConsumerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

        let subscription_id = 1;
        let (tx, rx) = channel(10000);
        let consumer = Arc::new(ConsumerInternal {
            name: self.consumer_name.clone(),
            subscription_id,
            stream: stream.to_string(),
            client: client.clone(),
            offset_specification: self.offset_specification.clone(),
            sender: tx,
            closed: Arc::new(AtomicBool::new(false)),
            waker: AtomicWaker::new(),
            metrics_collector: collector,
            filter_configuration: self.filter_configuration.clone(),
            consumer_update_listener: self.consumer_update_listener.clone(),
        });
        let msg_handler = ConsumerMessageHandler(consumer.clone());
        client.set_handler(msg_handler).await;

        if let Some(filter_input) = self.filter_configuration {
            if !client.filtering_supported() {
                return Err(ConsumerCreateError::FilteringNotSupport);
            }
            for (index, item) in filter_input.filter_values.iter().enumerate() {
                let key = format!("filter.{}", index);
                self.properties.insert(key, item.to_owned());
            }

            let match_unfiltered_key = "match-unfiltered".to_string();
            self.properties.insert(
                match_unfiltered_key,
                filter_input.match_unfiltered.to_string(),
            );
        }

        let response = client
            .subscribe(
                subscription_id,
                stream,
                self.offset_specification,
                1,
                self.properties.clone(),
            )
            .await?;

        if response.is_ok() {
            Ok(Consumer {
                name: self.consumer_name.clone(),
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

    pub fn client_provided_name(mut self, name: &str) -> Self {
        self.client_provided_name = String::from(name);
        self
    }

    pub fn name(mut self, consumer_name: &str) -> Self {
        self.consumer_name = Some(String::from(consumer_name));
        self
    }

    pub fn filter_input(mut self, filter_configuration: Option<FilterConfiguration>) -> Self {
        self.filter_configuration = filter_configuration;
        self
    }

    pub fn consumer_update(
        mut self,
        consumer_update_listener: impl Fn(u8, &MessageContext) -> OffsetSpecification
            + Send
            + Sync
            + 'static,
    ) -> Self {
        let f = Arc::new(consumer_update_listener);
        self.consumer_update_listener = Some(f);
        self
    }

    pub fn consumer_update_arc(
        mut self,
        consumer_update_listener: Option<crate::consumer::ConsumerUpdateListener>,
    ) -> Self {
        self.consumer_update_listener = consumer_update_listener;
        self
    }

    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
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

    pub async fn store_offset(&self, offset: u64) -> Result<(), ConsumerStoreOffsetError> {
        if let Some(name) = &self.name {
            self.internal
                .client
                .store_offset(name.as_str(), self.internal.stream.as_str(), offset)
                .await
                .map(Ok)?
        } else {
            Err(ConsumerStoreOffsetError::NameMissing)
        }
    }

    pub async fn query_offset(&self) -> Result<u64, ConsumerStoreOffsetError> {
        if let Some(name) = &self.name {
            self.internal
                .client
                .query_offset(name.clone(), self.internal.stream.as_str())
                .await
                .map(Ok)?
        } else {
            Err(ConsumerStoreOffsetError::NameMissing)
        }
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
        self.internal_close().await
    }

    pub(crate) async fn internal_close(&self) -> Result<(), ConsumerCloseError> {
        match self.0.closed.compare_exchange(false, true, SeqCst, SeqCst) {
            Ok(false) => {
                let response = self.0.client.unsubscribe(self.0.subscription_id).await?;
                if response.is_ok() {
                    self.0.waker.wake();
                    self.0.client.close().await?;
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
                if let ResponseKind::Deliver(delivery) = response.kind_ref() {
                    let mut offset = delivery.chunk_first_offset;

                    let len = delivery.messages.len();
                    let d = delivery.clone();
                    trace!("Got delivery with messages {}", len);

                    // // client filter
                    let messages = match &self.0.filter_configuration {
                        Some(filter_input) => {
                            if let Some(f) = &filter_input.predicate {
                                d.messages
                                    .into_iter()
                                    .filter(|message| f(message))
                                    .collect::<Vec<Message>>()
                            } else {
                                d.messages
                            }
                        }

                        None => d.messages,
                    };

                    for message in messages {
                        if let OffsetSpecification::Offset(offset_) = self.0.offset_specification {
                            if offset_ > offset {
                                offset += 1;
                                continue;
                            }
                        }
                        let _ = self
                            .0
                            .sender
                            .send(Ok(Delivery {
                                stream: self.0.stream.clone(),
                                subscription_id: self.0.subscription_id,
                                message,
                                offset,
                            }))
                            .await;
                        offset += 1;
                    }

                    // TODO handle credit fail
                    let _ = self.0.client.credit(self.0.subscription_id, 1).await;
                    self.0.metrics_collector.consume(len as u64).await;
                } else if let ResponseKind::ConsumerUpdate(consumer_update) = response.kind_ref() {
                    trace!("Received a ConsumerUpdate message");
                    // If no callback is provided by the user we will restart from Next by protocol
                    // We need to respond to the server too
                    if self.0.consumer_update_listener.is_none() {
                        trace!("User defined callback is not provided");
                        let offset_specification = OffsetSpecification::Next;
                        let _ = self
                            .0
                            .client
                            .consumer_update(
                                consumer_update.get_correlation_id(),
                                offset_specification,
                            )
                            .await;
                    } else {
                        // Otherwise the Offset specification is returned by the user callback
                        let is_active = consumer_update.is_active();
                        let message_context = MessageContext {
                            consumer_name: self.0.name.clone(),
                            stream: self.0.stream.clone(),
                        };
                        let consumer_update_listener_callback =
                            self.0.consumer_update_listener.clone().unwrap();
                        let offset_specification =
                            consumer_update_listener_callback(is_active, &message_context);
                        let _ = self
                            .0
                            .client
                            .consumer_update(
                                consumer_update.get_correlation_id(),
                                offset_specification,
                            )
                            .await;
                    }
                }
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

/// Envelope from incoming message
#[derive(Debug)]
pub struct Delivery {
    stream: String,
    subscription_id: u8,
    message: Message,
    offset: u64,
}

impl Delivery {
    /// Get a reference to the delivery's subscription id.
    pub fn subscription_id(&self) -> u8 {
        self.subscription_id
    }

    /// Get a reference to the delivery's stream name.
    pub fn stream(&self) -> &String {
        &self.stream
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
