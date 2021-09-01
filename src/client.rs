use crate::{
    channel::{channel, ChannelReceiver, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    error::RabbitMqStreamError,
    handler::MessageHandler,
    metadata::StreamMetadata,
    options::ClientOptions,
    RabbitMQStreamResult,
};
use futures::{
    stream::{SplitSink, SplitStream},
    Stream, StreamExt, TryFutureExt,
};
use rabbitmq_stream_protocol::{
    commands::{
        create_stream::CreateStreamCommand,
        credit::CreditCommand,
        delete::Delete,
        generic::GenericResponse,
        metadata::MetadataCommand,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        subscribe::{OffsetSpecification, SubscribeCommand},
        tune::TunesCommand,
        unsubscribe::UnSubscribeCommand,
    },
    FromResponse, Request, Response, ResponseKind,
};
use std::future::Future;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio::{net::TcpStream, sync::Notify};
use tokio_util::codec::Framed;

type SinkConnection = SplitSink<Framed<TcpStream, RabbitMqStreamCodec>, Request>;
type StreamConnection = SplitStream<Framed<TcpStream, RabbitMqStreamCodec>>;

pub struct ClientState {
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
    handler: Option<Arc<Box<dyn MessageHandler>>>,
    heartbeat: u32,
    max_frame_size: u32,
}

#[async_trait::async_trait]
impl MessageHandler for Client {
    async fn handle_message(&self, item: Response) -> RabbitMQStreamResult<()> {
        match item.kind() {
            ResponseKind::Tunes(tune) => self.handle_tune_command(tune).await,
            _ => {
                if let Some(handler) = self.state.read().await.handler.as_ref() {
                    let handler = handler.clone();
                    tokio::task::spawn(async move { handler.handle_message(item).await });
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    dispatcher: Dispatcher<Client>,
    channel: Arc<ChannelSender<SinkConnection>>,
    state: Arc<RwLock<ClientState>>,
    opts: ClientOptions,
    tune_notifier: Arc<Notify>,
}

impl Client {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client, RabbitMqStreamError> {
        let broker = opts.into();

        let (sender, receiver) = Client::create_connection(&broker).await?;

        let dispatcher = Dispatcher::new();

        let state = ClientState {
            server_properties: HashMap::new(),
            connection_properties: HashMap::new(),
            handler: None,
            heartbeat: broker.heartbeat,
            max_frame_size: broker.max_frame_size,
        };
        let mut client = Client {
            dispatcher,
            opts: broker,
            channel: Arc::new(sender),
            state: Arc::new(RwLock::new(state)),
            tune_notifier: Arc::new(Notify::new()),
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

        state.handler = Some(Arc::new(Box::new(handler)));
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
            .map(crate::metadata::from_response)
    }

    async fn create_connection(
        broker: &ClientOptions,
    ) -> Result<
        (
            ChannelSender<SinkConnection>,
            ChannelReceiver<StreamConnection>,
        ),
        RabbitMqStreamError,
    > {
        let stream = TcpStream::connect((broker.host.as_str(), broker.port)).await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        Ok((tx, rx))
    }
    async fn initialize<T>(
        &mut self,
        receiver: ChannelReceiver<T>,
    ) -> Result<(), RabbitMqStreamError>
    where
        T: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin + Send,
        T: 'static,
    {
        self.dispatcher.set_handler(self.clone()).await;
        self.dispatcher.start(receiver).await;

        self.with_state_lock(self.peer_properties(), |state, server_properties| {
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

    async fn wait_for_tune_data(&mut self) -> Result<(), RabbitMqStreamError> {
        self.tune_notifier.notified().await;
        Ok(())
    }

    async fn authenticate(&self) -> Result<(), RabbitMqStreamError> {
        self.sasl_mechanism()
            .and_then(|mechanisms| self.handle_authentication(mechanisms))
            .await
    }

    async fn handle_authentication(
        &self,
        _mechanism: Vec<String>,
    ) -> Result<(), RabbitMqStreamError> {
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

    async fn sasl_mechanism(&self) -> Result<Vec<String>, RabbitMqStreamError> {
        self.send_and_receive::<SaslHandshakeResponse, _, _>(|correlation_id| {
            SaslHandshakeCommand::new(correlation_id)
        })
        .await
        .map(|handshake| handshake.mechanisms)
    }

    async fn send_and_receive<T, R, M>(&self, msg_factory: M) -> Result<T, RabbitMqStreamError>
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

    async fn send<R>(&self, msg: R) -> Result<(), RabbitMqStreamError>
    where
        R: Into<Request>,
    {
        self.channel.send(msg.into()).await?;
        Ok(())
    }

    async fn handle_response<T: FromResponse>(
        &self,
        response: Response,
    ) -> Result<T, RabbitMqStreamError> {
        response.get::<T>().ok_or_else(|| {
            RabbitMqStreamError::CastError(format!(
                "Cannot cast response to {}",
                std::any::type_name::<T>()
            ))
        })
    }

    async fn open(&self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        self.send_and_receive::<OpenResponse, _, _>(|correlation_id| {
            OpenCommand::new(correlation_id, self.opts.v_host.clone())
        })
        .await
        .map(|open| open.connection_properties)
    }

    async fn peer_properties(&self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        self.send_and_receive::<PeerPropertiesResponse, _, _>(|correlation_id| {
            PeerPropertiesCommand::new(correlation_id, HashMap::new())
        })
        .await
        .map(|peer_properties| peer_properties.server_properties)
    }

    async fn handle_tune_command(&self, tunes: &TunesCommand) {
        let mut state = self.state.write().await;
        state.heartbeat = self.max_value(self.opts.heartbeat, tunes.heartbeat);
        state.max_frame_size = self.max_value(self.opts.max_frame_size, tunes.max_frame_size);

        let heart_beat = state.heartbeat;
        let max_frame_size = state.max_frame_size;
        drop(state);

        let _ = self
            .channel
            .send(TunesCommand::new(max_frame_size, heart_beat).into())
            .await;

        self.tune_notifier.notify_one();
    }
}
