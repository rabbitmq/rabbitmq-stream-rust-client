use std::future::Future;
use std::vec;
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::channel;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, trace};

use rabbitmq_stream_protocol::{message::Message, ResponseCode, ResponseKind};

use crate::client::ClientMessage;
use crate::MetricsCollector;
use crate::{client::MessageHandler, ClientOptions, RabbitMQStreamResult};
use crate::{
    client::{Client, MessageResult},
    environment::Environment,
    error::{ClientError, ProducerCloseError, ProducerCreateError, ProducerPublishError},
};

type WaiterMap = Arc<DashMap<u64, ProducerMessageWaiter>>;
type FilterValueExtractor = Arc<dyn Fn(&Message) -> String + 'static + Send + Sync>;

type ConfirmCallback = Arc<
    dyn Fn(Result<ConfirmationStatus, ProducerPublishError>) -> BoxFuture<'static, ()>
        + Send
        + Sync,
>;

#[derive(Debug)]
pub struct ConfirmationStatus {
    publishing_id: u64,
    confirmed: bool,
    status: ResponseCode,
    message: Message,
}

impl ConfirmationStatus {
    /// Get a reference to the confirmation status's confirmed.
    pub fn confirmed(&self) -> bool {
        self.confirmed
    }

    /// Get a reference to the confirmation status's publishing id.
    pub fn publishing_id(&self) -> u64 {
        self.publishing_id
    }

    /// Get a reference to the confirmation status's status.
    pub fn status(&self) -> &ResponseCode {
        &self.status
    }

    /// Get a reference to the confirmation status's message.
    pub fn message(&self) -> &Message {
        &self.message
    }
}

pub struct ProducerInternal {
    client: Client,
    stream: String,
    producer_id: u8,
    batch_size: usize,
    publish_sequence: Arc<AtomicU64>,
    waiting_confirmations: WaiterMap,
    closed: Arc<AtomicBool>,
    accumulator: MessageAccumulator,
    publish_version: u16,
    filter_value_extractor: Option<FilterValueExtractor>,
}

impl ProducerInternal {
    async fn batch_send(&self) -> Result<(), ProducerPublishError> {
        let messages = self.accumulator.get(self.batch_size).await?;

        if !messages.is_empty() {
            debug!("Sending batch of {} messages", messages.len());
            self.client
                .publish(self.producer_id, messages, self.publish_version)
                .await?;
        }

        Ok(())
    }
}

/// API for publising messages to RabbitMQ stream
#[derive(Clone)]
pub struct Producer<T>(Arc<ProducerInternal>, PhantomData<T>);

/// Builder for [`Producer`]
pub struct ProducerBuilder<T> {
    pub(crate) environment: Environment,
    pub(crate) name: Option<String>,
    pub batch_size: usize,
    pub batch_publishing_delay: Duration,
    pub(crate) data: PhantomData<T>,
    pub filter_value_extractor: Option<FilterValueExtractor>,
    pub(crate) client_provided_name: String,
}

#[derive(Clone)]
pub struct NoDedup {}

pub struct Dedup {}

impl<T> ProducerBuilder<T> {
    pub async fn build(self, stream: &str) -> Result<Producer<T>, ProducerCreateError> {
        // Connect to the user specified node first, then look for the stream leader.
        // The leader is the recommended node for writing, because writing to a replica will redundantly pass these messages
        // to the leader anyway - it is the only one capable of writing.

        let mut opt_with_client_provided_name = self.environment.options.client_options.clone();
        opt_with_client_provided_name.client_provided_name = self.client_provided_name.clone();

        let mut client = self
            .environment
            .create_client_with_options(opt_with_client_provided_name.clone())
            .await?;

        let mut publish_version = 1;

        if self.filter_value_extractor.is_some() {
            if client.filtering_supported() {
                publish_version = 2
            } else {
                return Err(ProducerCreateError::FilteringNotSupport);
            }
        }

        let metrics_collector = self.environment.options.client_options.collector.clone();
        if let Some(metadata) = client.metadata(vec![stream.to_string()]).await?.get(stream) {
            tracing::debug!(
                "Connecting to leader node {:?} of stream {}",
                metadata.leader,
                stream
            );
            let load_balancer_mode = self.environment.options.client_options.load_balancer_mode;
            if load_balancer_mode {
                // Producer must connect to leader node
                let options: ClientOptions = self.environment.options.client_options.clone();
                loop {
                    let temp_client = Client::connect(options.clone()).await?;
                    let mapping = temp_client.connection_properties().await;
                    if let Some(advertised_host) = mapping.get("advertised_host") {
                        if *advertised_host == metadata.leader.host.clone() {
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
                    host: metadata.leader.host.clone(),
                    port: metadata.leader.port as u16,
                    ..opt_with_client_provided_name.clone()
                })
                .await?
            };
        } else {
            return Err(ProducerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

        let waiting_confirmations: WaiterMap = Arc::new(DashMap::new());

        let confirm_handler = ProducerConfirmHandler {
            waiting_confirmations: waiting_confirmations.clone(),
            metrics_collector,
        };

        client.set_handler(confirm_handler).await;

        let producer_id = 1;
        let response = client
            .declare_publisher(producer_id, self.name.clone(), stream)
            .await?;

        let publish_sequence = if let Some(name) = self.name {
            let sequence = client.query_publisher_sequence(&name, stream).await?;

            let first_sequence = if sequence == 0 { 0 } else { sequence + 1 };

            Arc::new(AtomicU64::new(first_sequence))
        } else {
            Arc::new(AtomicU64::new(0))
        };

        if response.is_ok() {
            let producer = ProducerInternal {
                producer_id,
                batch_size: self.batch_size,
                stream: stream.to_string(),
                client,
                publish_sequence,
                waiting_confirmations,
                publish_version,
                closed: Arc::new(AtomicBool::new(false)),
                accumulator: MessageAccumulator::new(self.batch_size),
                filter_value_extractor: self.filter_value_extractor,
            };

            let internal_producer = Arc::new(producer);
            let producer = Producer(internal_producer.clone(), PhantomData);
            schedule_batch_send(internal_producer, self.batch_publishing_delay);

            Ok(producer)
        } else {
            Err(ProducerCreateError::Create {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn batch_delay(mut self, delay: Duration) -> Self {
        self.batch_publishing_delay = delay;
        self
    }

    pub fn client_provided_name(mut self, name: &str) -> Self {
        self.client_provided_name = String::from(name);
        self
    }

    pub fn name(mut self, name: &str) -> ProducerBuilder<Dedup> {
        self.name = Some(name.to_owned());
        ProducerBuilder {
            environment: self.environment,
            name: self.name,
            batch_size: self.batch_size,
            batch_publishing_delay: self.batch_publishing_delay,
            data: PhantomData,
            filter_value_extractor: None,
            client_provided_name: String::from("rust-stream-producer"),
        }
    }

    pub fn filter_value_extractor(
        mut self,
        filter_value_extractor: impl Fn(&Message) -> String + Send + Sync + 'static,
    ) -> Self {
        let f = Arc::new(filter_value_extractor);
        self.filter_value_extractor = Some(f);
        self
    }

    pub fn filter_value_extractor_arc(
        mut self,
        filter_value_extractor: Option<FilterValueExtractor>,
    ) -> Self {
        self.filter_value_extractor = filter_value_extractor;
        self
    }
}

pub struct MessageAccumulator {
    sender: mpsc::Sender<ClientMessage>,
    receiver: Mutex<mpsc::Receiver<ClientMessage>>,
    capacity: usize,
    message_count: AtomicUsize,
}

impl MessageAccumulator {
    pub fn new(batch_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(batch_size);
        Self {
            sender,
            receiver: Mutex::new(receiver),
            capacity: batch_size,
            message_count: AtomicUsize::new(0),
        }
    }

    pub async fn add(&self, message: ClientMessage) -> RabbitMQStreamResult<bool> {
        self.sender
            .send(message)
            .await
            .map_err(|err| ClientError::GenericError(Box::new(err)))?;

        let val = self.message_count.fetch_add(1, Ordering::Relaxed);

        Ok(val + 1 == self.capacity)
    }

    pub async fn get(&self, batch_size: usize) -> RabbitMQStreamResult<Vec<ClientMessage>> {
        let mut messages = Vec::with_capacity(batch_size);
        let mut count = 0;
        let mut receiver = self.receiver.lock().await;
        while count < batch_size {
            match receiver.try_recv().ok() {
                Some(message) => {
                    messages.push(message);
                    count += 1;
                }
                _ => break,
            }
        }
        self.message_count
            .fetch_sub(messages.len(), Ordering::Relaxed);
        Ok(messages)
    }
}

fn schedule_batch_send(producer: Arc<ProducerInternal>, delay: Duration) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(delay);

        debug!("Starting batch send interval every {:?}", delay);
        loop {
            interval.tick().await;

            match producer.batch_send().await {
                Ok(_) => {}
                Err(e) => error!("Error publishing batch {:?}", e),
            }
        }
    });
}

impl Producer<NoDedup> {
    pub async fn send_with_confirm(
        &self,
        message: Message,
    ) -> Result<ConfirmationStatus, ProducerPublishError> {
        self.do_send_with_confirm(message).await
    }
    pub async fn batch_send_with_confirm(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<ConfirmationStatus>, ProducerPublishError> {
        self.do_batch_send_with_confirm(messages).await
    }
    pub async fn batch_send<Fut>(
        &self,
        messages: Vec<Message>,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.do_batch_send(messages, cb).await
    }

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

impl Producer<Dedup> {
    pub async fn send_with_confirm(
        &mut self,
        message: Message,
    ) -> Result<ConfirmationStatus, ProducerPublishError> {
        self.do_send_with_confirm(message).await
    }
    pub async fn batch_send_with_confirm(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<Vec<ConfirmationStatus>, ProducerPublishError> {
        self.do_batch_send_with_confirm(messages).await
    }
    pub async fn batch_send<Fut>(
        &mut self,
        messages: Vec<Message>,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.do_batch_send(messages, cb).await
    }

    pub async fn send<Fut>(
        &mut self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.do_send(message, cb).await
    }
}

impl<T> Producer<T> {
    async fn do_send_with_confirm(
        &self,
        message: Message,
    ) -> Result<ConfirmationStatus, ProducerPublishError> {
        let (tx, mut rx) = channel(1);
        self.internal_send(message, move |status| {
            let cloned = tx.clone();
            async move {
                let _ = cloned.send(status).await;
            }
        })
        .await?;

        rx.recv()
            .await
            .ok_or_else(|| ProducerPublishError::Confirmation {
                stream: self.0.stream.clone(),
            })?
            .map_err(|err| ClientError::GenericError(Box::new(err)))
            .map(Ok)?
    }

    async fn do_batch_send_with_confirm(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<ConfirmationStatus>, ProducerPublishError> {
        let messages_len = messages.len();
        let (tx, mut rx) = channel(messages_len);

        self.internal_batch_send(messages, move |status| {
            let cloned = tx.clone();
            async move {
                let _ = cloned.send(status).await;
            }
        })
        .await?;

        let mut confirmations = Vec::with_capacity(messages_len);

        while let Some(confirmation) = rx.recv().await {
            confirmations.push(confirmation?);
        }

        Ok(confirmations)
    }
    async fn do_batch_send<Fut>(
        &self,
        messages: Vec<Message>,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.internal_batch_send(messages, cb).await?;

        Ok(())
    }

    async fn do_send<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        self.internal_send(message, cb).await?;
        Ok(())
    }

    async fn internal_send<Fut>(
        &self,
        message: Message,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        if self.is_closed() {
            return Err(ProducerPublishError::Closed);
        }
        let publishing_id = match message.publishing_id() {
            Some(publishing_id) => *publishing_id,
            None => self.0.publish_sequence.fetch_add(1, Ordering::Relaxed),
        };
        let mut msg = ClientMessage::new(publishing_id, message.clone(), None);

        if let Some(f) = self.0.filter_value_extractor.as_ref() {
            msg.filter_value_extract(f.as_ref())
        }

        let waiter = ProducerMessageWaiter::waiter_with_cb(cb, message);
        self.0.waiting_confirmations.insert(publishing_id, waiter);

        if self.0.accumulator.add(msg).await? {
            self.0.batch_send().await?;
        }

        Ok(())
    }
    async fn internal_batch_send<Fut>(
        &self,
        messages: Vec<Message>,
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
    ) -> Result<(), ProducerPublishError>
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        if self.is_closed() {
            return Err(ProducerPublishError::Closed);
        }

        let arc_cb = Arc::new(move |status| cb(status).boxed());

        let mut wrapped_msgs = Vec::with_capacity(messages.len());
        for message in messages {
            let waiter = ProducerMessageWaiter::waiter_with_arc_cb(arc_cb.clone(), message.clone());
            let publishing_id = match message.publishing_id() {
                Some(publishing_id) => *publishing_id,
                None => self.0.publish_sequence.fetch_add(1, Ordering::Relaxed),
            };

            let mut client_message = ClientMessage::new(publishing_id, message, None);
            if let Some(f) = self.0.filter_value_extractor.as_ref() {
                client_message.filter_value_extract(f.as_ref())
            }
            wrapped_msgs.push(client_message);

            self.0
                .waiting_confirmations
                .insert(publishing_id, waiter.clone());
        }

        self.0
            .client
            .publish(self.0.producer_id, wrapped_msgs, self.0.publish_version)
            .await?;

        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Relaxed)
    }
    // TODO handle producer state after close
    pub async fn close(self) -> Result<(), ProducerCloseError> {
        match self
            .0
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(false) => {
                let response = self.0.client.delete_publisher(self.0.producer_id).await?;
                if response.is_ok() {
                    self.0.client.close().await?;
                    Ok(())
                } else {
                    Err(ProducerCloseError::Close {
                        status: response.code().clone(),
                        stream: self.0.stream.clone(),
                    })
                }
            }
            _ => Err(ProducerCloseError::AlreadyClosed),
        }
    }
}

struct ProducerConfirmHandler {
    waiting_confirmations: WaiterMap,
    metrics_collector: Arc<dyn MetricsCollector>,
}

impl ProducerConfirmHandler {
    async fn with_waiter(
        &self,
        publishing_id: u64,
        cb: impl FnOnce(ProducerMessageWaiter) -> BoxFuture<'static, ()>,
    ) {
        match self.waiting_confirmations.remove(&publishing_id) {
            Some(confirm_sender) => cb(confirm_sender.1).await,
            None => todo!(),
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for ProducerConfirmHandler {
    async fn handle_message(&self, item: MessageResult) -> RabbitMQStreamResult<()> {
        match item {
            Some(Ok(response)) => {
                match response.kind() {
                    ResponseKind::PublishConfirm(confirm) => {
                        trace!("Got publish_confirm for {:?}", confirm.publishing_ids);
                        let confirm_len = confirm.publishing_ids.len();
                        for publishing_id in &confirm.publishing_ids {
                            let id = *publishing_id;
                            self.with_waiter(*publishing_id, move |waiter| {
                                async move {
                                    let _ = waiter.handle_confirm(id).await;
                                }
                                .boxed()
                            })
                            .await;
                        }
                        self.metrics_collector
                            .publish_confirm(confirm_len as u64)
                            .await;
                    }
                    ResponseKind::PublishError(error) => {
                        trace!("Got publish_error  {:?}", error);
                        for err in &error.publishing_errors {
                            let code = err.error_code.clone();
                            let id = err.publishing_id;
                            self.with_waiter(err.publishing_id, move |waiter| {
                                async move {
                                    let _ = waiter.handle_error(id, code).await;
                                }
                                .boxed()
                            })
                            .await;
                        }
                    }
                    _ => {}
                };
            }
            Some(Err(error)) => {
                trace!(?error);
                // TODO clean all waiting for confirm
            }
            None => {
                trace!("Connection closed");
                // TODO connection close clean all waiting
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ProducerMessageWaiter {
    cb: ConfirmCallback,
    msg: Message,
}

impl ProducerMessageWaiter {
    fn waiter_with_cb<Fut>(
        cb: impl Fn(Result<ConfirmationStatus, ProducerPublishError>) -> Fut + Send + Sync + 'static,
        msg: Message,
    ) -> Self
    where
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        Self {
            cb: Arc::new(move |confirm_status| cb(confirm_status).boxed()),
            msg,
        }
    }

    fn waiter_with_arc_cb(confirm_callback: ConfirmCallback, msg: Message) -> Self {
        Self {
            cb: confirm_callback,
            msg,
        }
    }
    async fn handle_confirm(self, publishing_id: u64) -> RabbitMQStreamResult<()> {
        (self.cb)(Ok(ConfirmationStatus {
            publishing_id,
            confirmed: true,
            status: ResponseCode::Ok,
            message: self.msg,
        }))
        .await;
        Ok(())
    }
    async fn handle_error(
        self,
        publishing_id: u64,
        status: ResponseCode,
    ) -> RabbitMQStreamResult<()> {
        (self.cb)(Ok(ConfirmationStatus {
            publishing_id,
            confirmed: false,
            status,
            message: self.msg,
        }))
        .await;
        Ok(())
    }
}
