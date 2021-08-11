use std::{collections::HashMap, sync::Arc};

use crate::{
    channel::{channel, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::{Dispatcher, MessageHandler},
    error::RabbitMqStreamError,
    options::ClientOptions,
    RabbitMQStreamResult,
};
use futures::{stream::SplitSink, StreamExt, TryFutureExt};
use rabbitmq_stream_protocol::{
    commands::{
        credit::CreditCommand,
        generic::GenericResponse,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        subscribe::{OffsetSpecification, SubscribeCommand},
        tune::TunesCommand,
    },
    FromResponse, FromResponseRef, Request, Response,
};
use tokio::sync::broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::{net::TcpStream, sync::broadcast::Receiver};
use tokio_util::codec::Framed;

type SinkConnection = SplitSink<Framed<TcpStream, RabbitMqStreamCodec>, Request>;

pub struct ClientInternal {
    dispatcher: Dispatcher<ClientInternalHandle>,
    handle: ClientInternalHandle,
    channel: ChannelSender<SinkConnection>,
    opts: ClientOptions,
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
}

#[derive(Clone)]
pub struct ClientInternalHandle {
    sender: BroadcastSender<Arc<Response>>,
}

#[async_trait::async_trait]
impl MessageHandler for ClientInternalHandle {
    async fn handle_message(&self, item: Response) -> RabbitMQStreamResult<()> {
        let _ = self.sender.send(Arc::new(item));

        Ok(())
    }
}

impl ClientInternalHandle {
    pub fn subscribe(&self) -> BroadcastReceiver<Arc<Response>> {
        self.sender.subscribe()
    }
}
pub struct Client(Arc<ClientInternal>);

impl Client {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client, RabbitMqStreamError> {
        let broker = opts.into();

        let (tx, _rx) = broadcast::channel(20);

        let handle = ClientInternalHandle { sender: tx };
        let (sender, dispatcher) =
            ClientInternal::create_connection(&broker, handle.clone()).await?;

        let mut internal = ClientInternal {
            dispatcher,
            handle,
            channel: sender,
            opts: broker,
            server_properties: HashMap::new(),
            connection_properties: HashMap::new(),
        };

        internal.initialize().await?;

        Ok(Client(Arc::new(internal)))
    }

    /// Get a reference to the client's server properties.
    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.0.server_properties
    }

    /// Get a reference to the client's connection properties.
    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.0.connection_properties
    }

    pub async fn subscribe(
        &self,
        subscription_id: u8,
        stream: &str,
        offset_specification: OffsetSpecification,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.0
            .send_and_receive(|correlation_id| {
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

    pub async fn credit(&self, subscription_id: u8, credit: u16) -> RabbitMQStreamResult<()> {
        self.0
            .send(CreditCommand::new(subscription_id, credit))
            .await
    }

    pub fn subscribe_messages(&self) -> Receiver<Arc<Response>> {
        self.0.handle.subscribe()
    }
}

impl ClientInternal {
    async fn initialize(&mut self) -> Result<(), RabbitMqStreamError> {
        let channel = self.handle.subscribe();
        self.server_properties = self.peer_properties().await?;
        self.authenticate().await?;

        self.wait_for_tune_data(channel).await?;
        self.connection_properties = self.open().await?;

        Ok(())
    }

    fn max_value(&self, client: u32, server: u32) -> u32 {
        match (client, server) {
            (client, server) if client == 0 || server == 0 => client.max(server),
            (client, server) => client.max(server),
        }
    }

    async fn wait_for_tune_data(
        &mut self,
        mut channel: Receiver<Arc<Response>>,
    ) -> Result<(), RabbitMqStreamError> {
        let response = channel.recv().await.expect("It should contain a message");

        let tunes = self.handle_response_ref::<TunesCommand>(&response).await?;

        self.opts.heartbeat = self.max_value(self.opts.heartbeat, tunes.heartbeat);
        self.opts.max_frame_size = self.max_value(self.opts.max_frame_size, tunes.max_frame_size);

        self.channel
            .send(TunesCommand::new(self.opts.max_frame_size, self.opts.heartbeat).into())
            .await
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

    async fn handle_response_ref<'a, T: FromResponseRef + 'a>(
        &self,
        response: &'a Response,
    ) -> Result<&'a T, RabbitMqStreamError> {
        response.get_ref::<T>().ok_or_else(|| {
            RabbitMqStreamError::CastError(format!(
                "Cannot cast response to {}",
                std::any::type_name::<T>()
            ))
        })
    }

    async fn open(&mut self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        self.send_and_receive::<OpenResponse, _, _>(|correlation_id| {
            OpenCommand::new(correlation_id, self.opts.v_host.clone())
        })
        .await
        .map(|open| open.connection_properties)
    }

    async fn peer_properties(&mut self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        self.send_and_receive::<PeerPropertiesResponse, _, _>(|correlation_id| {
            PeerPropertiesCommand::new(correlation_id, HashMap::new())
        })
        .await
        .map(|peer_properties| peer_properties.server_properties)
    }

    async fn create_connection(
        broker: &ClientOptions,
        handle: ClientInternalHandle,
    ) -> Result<
        (
            ChannelSender<SinkConnection>,
            Dispatcher<ClientInternalHandle>,
        ),
        RabbitMqStreamError,
    > {
        let stream = TcpStream::connect((broker.host.as_str(), broker.port)).await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        let dispatcher = Dispatcher::create(handle, rx).await;

        Ok((tx, dispatcher))
    }
}
