use std::{collections::HashMap, sync::Arc};

use futures::{stream::SplitSink, StreamExt, TryFutureExt};
use rabbitmq_stream_protocol::{
    commands::{
        generic::GenericResponse,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        tune::TunesCommand,
    },
    FromResponse, FromResponseRef, Request, Response,
};
use tokio::{net::TcpStream, sync::broadcast::Receiver};
use tokio_util::codec::Framed;

use crate::{
    channel::{channel, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    error::RabbitMqStreamError,
    options::ClientOptions,
};

type SinkConnection = SplitSink<Framed<TcpStream, RabbitMqStreamCodec>, Request>;

pub struct ClientInternal {
    dispatcher: Dispatcher,
    channel: ChannelSender<SinkConnection>,
    opts: ClientOptions,
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
}
pub struct Client(Arc<ClientInternal>);

impl Client {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client, RabbitMqStreamError> {
        let broker = opts.into();
        let (sender, dispatcher) = ClientInternal::create_connection(&broker).await?;

        let mut internal = ClientInternal {
            dispatcher,
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
}

impl ClientInternal {
    async fn initialize(&mut self) -> Result<(), RabbitMqStreamError> {
        let channel = self.dispatcher.subscribe();
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
        let response = channel.recv().await.unwrap();

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

        self.send::<GenericResponse, _, _>(|correlation_id| {
            SaslAuthenticateCommand::new(
                correlation_id.into(),
                "PLAIN".to_owned(),
                auth_data.as_bytes().to_vec(),
            )
        })
        .await
        .map(|_| ())
    }

    async fn sasl_mechanism(&self) -> Result<Vec<String>, RabbitMqStreamError> {
        self.send::<SaslHandshakeResponse, _, _>(|correlation_id| {
            SaslHandshakeCommand::new(correlation_id.into())
        })
        .await
        .map(|handshake| handshake.mechanisms)
    }

    async fn send<T, R, M>(&self, msg_factory: M) -> Result<T, RabbitMqStreamError>
    where
        R: Into<Request>,
        T: FromResponse,
        M: FnOnce(u32) -> R,
    {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;

        self.channel
            .send(msg_factory(correlation_id).into())
            .await?;

        let response = receiver.recv().await.unwrap();

        self.handle_response::<T>(response).await
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
        self.send::<OpenResponse, _, _>(|correlation_id| {
            OpenCommand::new(correlation_id.into(), self.opts.v_host.clone())
        })
        .await
        .map(|open| open.connection_properties)
    }

    async fn peer_properties(&mut self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        self.send::<PeerPropertiesResponse, _, _>(|correlation_id| {
            PeerPropertiesCommand::new(correlation_id.into(), HashMap::new())
        })
        .await
        .map(|peer_properties| peer_properties.server_properties)
    }

    async fn create_connection(
        broker: &ClientOptions,
    ) -> Result<(ChannelSender<SinkConnection>, Dispatcher), RabbitMqStreamError> {
        let stream = TcpStream::connect((broker.host.as_str(), broker.port)).await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        let dispatcher = Dispatcher::create(rx).await;

        Ok((tx, dispatcher))
    }
}
