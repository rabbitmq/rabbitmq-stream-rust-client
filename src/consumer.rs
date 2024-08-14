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

/// API for consuming RabbitMQ stream messages
pub struct Consumer {
    // Mandatory in case of manual offset tracking
    name: Option<String>,
    receiver: Receiver<Result<Delivery, ConsumerDeliveryError>>,
    internal: Arc<ConsumerInternal>,
}

struct ConsumerInternal {
    client: Client,
    stream: String,
    offset_specification: OffsetSpecification,
    subscription_id: u8,
    sender: Sender<Result<Delivery, ConsumerDeliveryError>>,
    closed: Arc<AtomicBool>,
    waker: AtomicWaker,
    metrics_collector: Arc<dyn MetricsCollector>,
    filter_configuration: Option<FilterConfiguration>,
}

impl ConsumerInternal {
    fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

#[derive(Clone)]
pub struct FilterConfiguration {
    filter_values: Vec<String>,
    pub predicate: Arc<dyn Fn(&Message) -> bool + Send + Sync>,
    match_unfiltered: bool,
}

impl FilterConfiguration {
    pub fn new(
        filter_values: Vec<String>,
        predicate: impl Fn(&Message) -> bool + 'static + Send + Sync,
        match_unfiltered: bool,
    ) -> Self {
        let f = Arc::new(predicate);
        Self {
            filter_values,
            match_unfiltered,
            predicate: f,
        }
    }
}

/// Builder for [`Consumer`]
pub struct ConsumerBuilder {
    pub(crate) consumer_name: Option<String>,
    pub(crate) environment: Environment,
    pub(crate) offset_specification: OffsetSpecification,
    pub(crate) filter_configuration: Option<FilterConfiguration>,
}

impl ConsumerBuilder {
    pub async fn build(self, stream: &str) -> Result<Consumer, ConsumerCreateError> {
        // Connect to the user specified node first, then look for a random replica to connect to instead.
        // This is recommended for load balancing purposes.
        let mut client = self.environment.create_client().await?;
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
                                client = temp_client;
                                break;
                            }
                        }
                    }
                } else {
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
            subscription_id,
            stream: stream.to_string(),
            client: client.clone(),
            offset_specification: self.offset_specification.clone(),
            sender: tx,
            closed: Arc::new(AtomicBool::new(false)),
            waker: AtomicWaker::new(),
            metrics_collector: collector,
            filter_configuration: self.filter_configuration.clone(),
        });
        let msg_handler = ConsumerMessageHandler(consumer.clone());
        client.set_handler(msg_handler).await;

        let mut properties = HashMap::new();
        if let Some(filter_input) = self.filter_configuration {
            if !client.filtering_supported() {
                return Err(ConsumerCreateError::FilteringNotSupport);
            }
            for (index, item) in filter_input.filter_values.iter().enumerate() {
                let key = format!("filter.{}", index);
                properties.insert(key, item.to_owned());
            }

            let match_unfiltered_key = "match-unfiltered".to_string();
            properties.insert(
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
                properties,
            )
            .await?;

        if response.is_ok() {
            Ok(Consumer {
                name: self.consumer_name,
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

    pub fn name(mut self, consumer_name: &str) -> Self {
        self.consumer_name = Some(String::from(consumer_name));
        self
    }

    pub fn filter_input(mut self, filter_configuration: Option<FilterConfiguration>) -> Self {
        self.filter_configuration = filter_configuration;
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
        match self.0.closed.compare_exchange(false, true, SeqCst, SeqCst) {
            Ok(false) => {
                let response = self.0.client.unsubscribe(self.0.subscription_id).await?;
                if response.is_ok() {
                    self.0.waker.wake();
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
                if let ResponseKind::Deliver(delivery) = response.kind() {
                    let mut offset = delivery.chunk_first_offset;

                    let len = delivery.messages.len();
                    trace!("Got delivery with messages {}", len);

                    // // client filter
                    let messages = match &self.0.filter_configuration {
                        Some(filter_input) => delivery
                            .messages
                            .into_iter()
                            .filter(|message| filter_input.predicate.as_ref()(message))
                            .collect::<Vec<Message>>(),
                        None => delivery.messages,
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
    subscription_id: u8,
    message: Message,
    offset: u64,
}

impl Delivery {
    /// Get a reference to the delivery's subscription id.
    pub fn subscription_id(&self) -> u8 {
        self.subscription_id
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
