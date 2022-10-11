mod channel;
mod codec;
mod dispatcher;
mod handler;
mod message;
mod metadata;
mod metrics;
mod options;
use crate::{error::ClientError, RabbitMQStreamResult};
use futures::{
    stream::{SplitSink, SplitStream},
    Stream, StreamExt, TryFutureExt,
};

pub use message::ClientMessage;
pub use metadata::{Broker, StreamMetadata};
pub use metrics::MetricsCollector;
pub use options::ClientOptions;
use rabbitmq_stream_protocol::{
    commands::{
        close::{CloseRequest, CloseResponse},
        create_stream::CreateStreamCommand,
        credit::CreditCommand,
        declare_publisher::DeclarePublisherCommand,
        delete::Delete,
        delete_publisher::DeletePublisherCommand,
        generic::GenericResponse,
        heart_beat::HeartBeatCommand,
        metadata::MetadataCommand,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        publish::PublishCommand,
        query_offset::{QueryOffsetRequest, QueryOffsetResponse},
        query_publisher_sequence::{QueryPublisherRequest, QueryPublisherResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        store_offset::StoreOffset,
        subscribe::{OffsetSpecification, SubscribeCommand},
        tune::TunesCommand,
        unsubscribe::UnSubscribeCommand,
    },
    types::PublishedMessage,
    FromResponse, Request, Response, ResponseCode, ResponseKind,
};
use tracing::trace;

pub use self::handler::{MessageHandler, MessageResult};
use self::{
    channel::{channel, ChannelReceiver, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    message::BaseMessage,
};

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use std::{future::Future, sync::atomic::Ordering};
use tokio::sync::RwLock;
use tokio::{net::TcpStream, sync::Notify, task::JoinHandle};
use tokio_util::codec::Framed;

type SinkConnection = SplitSink<Framed<TcpStream, RabbitMqStreamCodec>, Request>;
type StreamConnection = SplitStream<Framed<TcpStream, RabbitMqStreamCodec>>;

pub struct ClientState {
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
    handler: Option<Arc<dyn MessageHandler>>,
    heartbeat: u32,
    max_frame_size: u32,
    heartbeat_task: Option<Arc<JoinHandle<()>>>,
}

#[async_trait::async_trait]
impl MessageHandler for Client {
    async fn handle_message(&self, item: MessageResult) -> RabbitMQStreamResult<()> {
        match &item {
            Some(Ok(response)) => match response.kind_ref() {
                ResponseKind::Tunes(tune) => self.handle_tune_command(tune).await,
                ResponseKind::Heartbeat(_) => self.handle_heart_beat_command().await,
                _ => {
                    if let Some(handler) = self.state.read().await.handler.as_ref() {
                        let handler = handler.clone();

                        tokio::task::spawn(async move { handler.handle_message(item).await });
                    }
                }
            },
            Some(Err(err)) => {
                trace!(?err);
                if let Some(handler) = self.state.read().await.handler.as_ref() {
                    let handler = handler.clone();

                    tokio::task::spawn(async move { handler.handle_message(item).await });
                }
            }
            None => {
                trace!("Closing client");
                if let Some(handler) = self.state.read().await.handler.as_ref() {
                    let handler = handler.clone();
                    tokio::task::spawn(async move { handler.handle_message(None).await });
                }
            }
        }

        Ok(())
    }
}
/// Raw API for taking to RabbitMQ stream
///
/// For high level APIs check [`crate::Environment`]
#[derive(Clone)]
pub struct Client {
    dispatcher: Dispatcher<Client>,
    channel: Arc<ChannelSender<SinkConnection>>,
    state: Arc<RwLock<ClientState>>,
    opts: ClientOptions,
    tune_notifier: Arc<Notify>,
    publish_sequence: Arc<AtomicU64>,
}

impl Client {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client, ClientError> {
        let broker = opts.into();

        let (sender, receiver) = Client::create_connection(&broker).await?;

        let dispatcher = Dispatcher::new();

        let state = ClientState {
            server_properties: HashMap::new(),
            connection_properties: HashMap::new(),
            handler: None,
            heartbeat: broker.heartbeat,
            max_frame_size: broker.max_frame_size,
            heartbeat_task: None,
        };
        let mut client = Client {
            dispatcher,
            opts: broker,
            channel: Arc::new(sender),
            state: Arc::new(RwLock::new(state)),
            tune_notifier: Arc::new(Notify::new()),
            publish_sequence: Arc::new(AtomicU64::new(1)),
        };

        client.initialize(receiver).await?;

        Ok(client)
    }

    /// Get client's server properties.
    pub async fn server_properties(&self) -> HashMap<String, String> {
        self.state.read().await.server_properties.clone()
    }

    /// Get client's connection properties.
    pub async fn connection_properties(&self) -> HashMap<String, String> {
        self.state.read().await.connection_properties.clone()
    }

    pub async fn set_handler<H: MessageHandler>(&self, handler: H) {
        let mut state = self.state.write().await;

        state.handler = Some(Arc::new(handler));
    }

    pub async fn close(&self) -> RabbitMQStreamResult<()> {
        let mut state = self.state.write().await;
        if let Some(ref heartbeat_task) = state.heartbeat_task {
            heartbeat_task.abort();
            state.heartbeat_task = None
        }

        if self.channel.is_closed() {
            return Err(ClientError::AlreadyClosed);
        }
        let _: CloseResponse = self
            .send_and_receive(|correlation_id| {
                CloseRequest::new(correlation_id, ResponseCode::Ok, "Ok".to_owned())
            })
            .await?;
        self.channel.close().await
    }
    pub async fn subscribe(
        &self,
        subscription_id: u8,
        stream: &str,
        offset_specification: OffsetSpecification,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            SubscribeCommand::new(
                correlation_id,
                subscription_id,
                stream.to_owned(),
                offset_specification,
                credit,
                properties,
            )
        })
        .await
    }

    pub async fn unsubscribe(&self, subscription_id: u8) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            UnSubscribeCommand::new(correlation_id, subscription_id)
        })
        .await
    }

    pub async fn create_stream(
        &self,
        stream: &str,
        options: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            CreateStreamCommand::new(correlation_id, stream.to_owned(), options)
        })
        .await
    }

    pub async fn delete_stream(&self, stream: &str) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| Delete::new(correlation_id, stream.to_owned()))
            .await
    }

    pub async fn credit(&self, subscription_id: u8, credit: u16) -> RabbitMQStreamResult<()> {
        self.send(CreditCommand::new(subscription_id, credit)).await
    }

    pub async fn metadata(
        &self,
        streams: Vec<String>,
    ) -> RabbitMQStreamResult<HashMap<String, StreamMetadata>> {
        self.send_and_receive(|correlation_id| MetadataCommand::new(correlation_id, streams))
            .await
            .map(metadata::from_response)
    }

    pub async fn store_offset(
        &self,
        reference: &str,
        stream: &str,
        offset: u64,
    ) -> RabbitMQStreamResult<()> {
        self.send(StoreOffset::new(
            reference.to_owned(),
            stream.to_owned(),
            offset,
        ))
        .await
    }

    pub async fn query_offset(&self, reference: String, stream: &str) -> Result<u64, ClientError> {
        self.send_and_receive::<QueryOffsetResponse, _, _>(|correlation_id| {
            QueryOffsetRequest::new(correlation_id, reference, stream.to_owned())
        })
        .await
        .map(|query_offset| query_offset.from_response())
    }

    pub async fn declare_publisher(
        &self,
        publisher_id: u8,
        publisher_reference: Option<String>,
        stream: &str,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            DeclarePublisherCommand::new(
                correlation_id,
                publisher_id,
                publisher_reference,
                stream.to_owned(),
            )
        })
        .await
    }

    pub async fn delete_publisher(
        &self,
        publisher_id: u8,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            DeletePublisherCommand::new(correlation_id, publisher_id)
        })
        .await
    }

    pub async fn publish<T: BaseMessage>(
        &self,
        publisher_id: u8,
        messages: impl Into<Vec<T>>,
    ) -> RabbitMQStreamResult<Vec<u64>> {
        let messages: Vec<PublishedMessage> = messages
            .into()
            .into_iter()
            .map(|message| {
                let publishing_id = message
                    .publishing_id()
                    .unwrap_or_else(|| self.publish_sequence.fetch_add(1, Ordering::Relaxed));

                PublishedMessage::new(publishing_id, message.to_message())
            })
            .collect();
        let sequences = messages
            .iter()
            .map(rabbitmq_stream_protocol::types::PublishedMessage::publishing_id)
            .collect();
        let len = messages.len();

        // TODO batch publish with max frame size check
        self.send(PublishCommand::new(publisher_id, messages))
            .await?;

        self.opts.collector.publish(len as u64).await;

        Ok(sequences)
    }

    pub async fn query_publisher_sequence(
        &self,
        reference: &str,
        stream: &str,
    ) -> Result<u64, ClientError> {
        self.send_and_receive::<QueryPublisherResponse, _, _>(|correlation_id| {
            QueryPublisherRequest::new(correlation_id, reference.to_owned(), stream.to_owned())
        })
        .await
        .map(|sequence| sequence.from_response())
    }

    async fn create_connection(
        broker: &ClientOptions,
    ) -> Result<
        (
            ChannelSender<SinkConnection>,
            ChannelReceiver<StreamConnection>,
        ),
        ClientError,
    > {
        let stream = TcpStream::connect((broker.host.as_str(), broker.port)).await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        Ok((tx, rx))
    }
    async fn initialize<T>(&mut self, receiver: ChannelReceiver<T>) -> Result<(), ClientError>
    where
        T: Stream<Item = Result<Response, ClientError>> + Unpin + Send,
        T: 'static,
    {
        self.dispatcher.set_handler(self.clone()).await;
        self.dispatcher.start(receiver).await;

        self.with_state_lock(self.peer_properties(), move |state, server_properties| {
            state.server_properties = server_properties;
        })
        .await?;
        self.authenticate().await?;

        self.wait_for_tune_data().await?;

        self.with_state_lock(self.open(), |state, connection_properties| {
            state.connection_properties = connection_properties;
        })
        .await?;

        Ok(())
    }

    async fn with_state_lock<T>(
        &self,
        task: impl Future<Output = RabbitMQStreamResult<T>>,
        mut updater: impl FnMut(&mut ClientState, T),
    ) -> RabbitMQStreamResult<()> {
        let result = task.await?;

        let mut state = self.state.write().await;

        updater(&mut state, result);

        Ok(())
    }

    fn max_value(&self, client: u32, server: u32) -> u32 {
        match (client, server) {
            (client, server) if client == 0 || server == 0 => client.max(server),
            (client, server) => client.max(server),
        }
    }

    async fn wait_for_tune_data(&mut self) -> Result<(), ClientError> {
        self.tune_notifier.notified().await;
        Ok(())
    }

    async fn authenticate(&self) -> Result<(), ClientError> {
        self.sasl_mechanism()
            .and_then(|mechanisms| self.handle_authentication(mechanisms))
            .await
    }

    async fn handle_authentication(&self, _mechanism: Vec<String>) -> Result<(), ClientError> {
        let auth_data = format!("\u{0000}{}\u{0000}{}", self.opts.user, self.opts.password);

        self.send_and_receive::<GenericResponse, _, _>(|correlation_id| {
            SaslAuthenticateCommand::new(
                correlation_id,
                "PLAIN".to_owned(),
                auth_data.as_bytes().to_vec(),
            )
        })
        .await
        .map(|_| ())
    }

    async fn sasl_mechanism(&self) -> Result<Vec<String>, ClientError> {
        self.send_and_receive::<SaslHandshakeResponse, _, _>(|correlation_id| {
            SaslHandshakeCommand::new(correlation_id)
        })
        .await
        .map(|handshake| handshake.mechanisms)
    }

    async fn send_and_receive<T, R, M>(&self, msg_factory: M) -> Result<T, ClientError>
    where
        R: Into<Request>,
        T: FromResponse,
        M: FnOnce(u32) -> R,
    {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;

        self.channel
            .send(msg_factory(correlation_id).into())
            .await?;

        let response = receiver.recv().await.expect("It should contain a response");

        self.handle_response::<T>(response).await
    }

    async fn send<R>(&self, msg: R) -> Result<(), ClientError>
    where
        R: Into<Request>,
    {
        self.channel.send(msg.into()).await?;
        Ok(())
    }

    async fn handle_response<T: FromResponse>(&self, response: Response) -> Result<T, ClientError> {
        response.get::<T>().ok_or_else(|| {
            ClientError::CastError(format!(
                "Cannot cast response to {}",
                std::any::type_name::<T>()
            ))
        })
    }

    async fn open(&self) -> Result<HashMap<String, String>, ClientError> {
        self.send_and_receive::<OpenResponse, _, _>(|correlation_id| {
            OpenCommand::new(correlation_id, self.opts.v_host.clone())
        })
        .await
        .map(|open| open.connection_properties)
    }

    async fn peer_properties(&self) -> Result<HashMap<String, String>, ClientError> {
        self.send_and_receive::<PeerPropertiesResponse, _, _>(|correlation_id| {
            PeerPropertiesCommand::new(correlation_id, HashMap::new())
        })
        .await
        .map(|peer_properties| peer_properties.server_properties)
    }

    async fn handle_tune_command(&self, tunes: &TunesCommand) {
        let mut state = self.state.write().await;
        let old_heartbeat = state.heartbeat;
        state.heartbeat = self.max_value(self.opts.heartbeat, tunes.heartbeat);
        state.max_frame_size = self.max_value(self.opts.max_frame_size, tunes.max_frame_size);

        let heart_beat = state.heartbeat;
        let max_frame_size = state.max_frame_size;

        let _ = self
            .channel
            .send(TunesCommand::new(max_frame_size, heart_beat).into())
            .await;

        // if hearbeat interval changed, abort the old heartbeat task and start a new one
        if old_heartbeat != heart_beat {
            if let Some(ref heartbeat_task) = state.heartbeat_task {
                heartbeat_task.abort();
                state.heartbeat_task = None
            }
            if heart_beat != 0 {
                let c = self.channel.clone();
                let heartbeat_interval = (heart_beat / 2).max(1);
                let handle = tokio::task::spawn(async move {
                    loop {
                        trace!("sending heartbeat");
                        let _ = c.send(HeartBeatCommand::default().into()).await;
                        tokio::time::sleep(Duration::from_secs(heartbeat_interval.into())).await;
                    }
                });
                state.heartbeat_task = Some(Arc::new(handle));
            }
        }
        drop(state);

        self.tune_notifier.notify_one();
    }

    async fn handle_heart_beat_command(&self) {
        trace!("handling heartbeat");
        let _ = self.channel.send(HeartBeatCommand::default().into()).await;
    }
}
