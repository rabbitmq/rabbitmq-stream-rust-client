use std::{collections::HashMap, sync::Arc};

use futures::{stream::SplitSink, StreamExt};
use rabbitmq_stream_protocol::{
    commands::{
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        tune::TunesCommand,
    },
    Request, Response,
};
use tokio::{net::TcpStream, sync::broadcast::Receiver};
use tokio_util::codec::Framed;

use crate::{
    broker::Broker,
    channel::{channel, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    error::RabbitMqStreamError,
};

type SinkConnection = SplitSink<Framed<TcpStream, RabbitMqStreamCodec>, Request>;

pub struct Client {
    dispatcher: Dispatcher,
    channel: ChannelSender<SinkConnection>,
    broker: Broker,
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
}

impl Client {
    pub async fn connect(broker: impl Into<Broker>) -> Result<Client, RabbitMqStreamError> {
        let broker = broker.into();
        let (sender, dispatcher) = Self::create_connection(&broker).await?;

        let mut client = Client {
            dispatcher,
            channel: sender,
            broker,
            server_properties: HashMap::new(),
            connection_properties: HashMap::new(),
        };

        client.initialize().await?;

        Ok(client)
    }

    async fn initialize(&mut self) -> Result<(), RabbitMqStreamError> {
        let channel = self.dispatcher.subscribe();
        self.server_properties = self.peer_properties().await?;
        self.authenticate().await?;

        self.wait_for_tune_data(channel).await?;
        self.connection_properties = self.open().await?;

        Ok(())
    }

    async fn wait_for_tune_data(
        &self,
        mut channel: Receiver<Arc<Response>>,
    ) -> Result<(), RabbitMqStreamError> {
        let response = channel.recv().await.unwrap();

        let tunes = response.get_ref::<TunesCommand>().unwrap();

        self.channel
            .send(TunesCommand::new(tunes.max_frame_size, tunes.heartbeat).into())
            .await
    }

    async fn authenticate(&mut self) -> Result<(), RabbitMqStreamError> {
        let mechanism = self.sasl_mechanism().await?;

        self.handle_authentication(mechanism).await
    }

    async fn handle_authentication(
        &mut self,
        _mechanism: Vec<String>,
    ) -> Result<(), RabbitMqStreamError> {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;

        let data = format!(
            "\u{0000}{}\u{0000}{}",
            self.broker.user, self.broker.password
        );
        self.channel
            .send(
                SaslAuthenticateCommand::new(
                    correlation_id.into(),
                    "PLAIN".to_owned(),
                    data.as_bytes().to_vec(),
                )
                .into(),
            )
            .await?;

        let _response = receiver.recv().await;

        Ok(())
    }

    async fn sasl_mechanism(&mut self) -> Result<Vec<String>, RabbitMqStreamError> {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;

        self.channel
            .send(SaslHandshakeCommand::new(correlation_id.into()).into())
            .await?;

        let response = receiver.recv().await.unwrap();

        let handshake_response = response.get::<SaslHandshakeResponse>().expect("");

        Ok(handshake_response.mechanisms)
    }

    async fn open(&mut self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;
        self.channel
            .send(OpenCommand::new(correlation_id.into(), self.broker.v_host.clone()).into())
            .await?;

        let response = receiver.recv().await.unwrap();

        let open_response = response.get::<OpenResponse>().expect("");
        Ok(open_response.connection_properties)
    }

    async fn peer_properties(&mut self) -> Result<HashMap<String, String>, RabbitMqStreamError> {
        let (correlation_id, mut receiver) = self.dispatcher.response_channel().await;

        self.channel
            .send(PeerPropertiesCommand::new(correlation_id.into(), HashMap::new()).into())
            .await?;

        let response = receiver.recv().await.unwrap();
        let peer_properties_properties = response.get::<PeerPropertiesResponse>().unwrap();
        Ok(peer_properties_properties.server_properties)
    }

    async fn create_connection(
        broker: &Broker,
    ) -> Result<(ChannelSender<SinkConnection>, Dispatcher), RabbitMqStreamError> {
        let stream = TcpStream::connect((broker.host.as_str(), broker.port)).await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        let dispatcher = Dispatcher::create(rx).await;

        Ok((tx, dispatcher))
    }

    /// Get a reference to the client's server properties.
    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.server_properties
    }

    /// Get a reference to the client's connection properties.
    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.connection_properties
    }
}
