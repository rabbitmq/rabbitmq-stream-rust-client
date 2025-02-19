use std::ops::DerefMut;
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use std::{future::Future, sync::atomic::Ordering};

use futures::{
    stream::{SplitSink, SplitStream},
    Stream, StreamExt, TryFutureExt,
};
use pin_project::pin_project;
use rabbitmq_stream_protocol::commands::exchange_command_versions::{
    ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse,
};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::RwLock;
use tokio::{net::TcpStream, sync::Notify};
use tokio_rustls::client::TlsStream;

use tokio_util::codec::Framed;
use tracing::trace;

use crate::{error::ClientError, RabbitMQStreamResult};
pub use message::ClientMessage;
pub use metadata::{Broker, StreamMetadata};
pub use metrics::MetricsCollector;
pub use options::{ClientOptions, TlsConfiguration, TlsConfigurationBuilder};
use rabbitmq_stream_protocol::{
    commands::{
        close::{CloseRequest, CloseResponse},
        consumer_update_request::ConsumerUpdateRequestCommand,
        create_stream::CreateStreamCommand,
        create_super_stream::CreateSuperStreamCommand,
        credit::CreditCommand,
        declare_publisher::DeclarePublisherCommand,
        delete::Delete,
        delete_publisher::DeletePublisherCommand,
        delete_super_stream::DeleteSuperStreamCommand,
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
        superstream_partitions::SuperStreamPartitionsRequest,
        superstream_partitions::SuperStreamPartitionsResponse,
        superstream_route::SuperStreamRouteRequest,
        superstream_route::SuperStreamRouteResponse,
        tune::TunesCommand,
        unsubscribe::UnSubscribeCommand,
    },
    types::PublishedMessage,
    FromResponse, Request, Response, ResponseCode, ResponseKind,
};

pub use self::handler::{MessageHandler, MessageResult};
use self::{
    channel::{channel, ChannelReceiver, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    message::BaseMessage,
};

mod channel;
mod codec;
mod dispatcher;
mod handler;
mod message;
mod metadata;
mod metrics;
mod options;
mod task;

#[cfg_attr(docsrs, doc(cfg(feature = "tokio-stream")))]
#[pin_project(project = StreamProj)]
#[derive(Debug)]
pub enum GenericTcpStream {
    Tcp(#[pin] TcpStream),
    SecureTcp(#[pin] TlsStream<TcpStream>),
}

impl AsyncRead for GenericTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_read(cx, buf),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for GenericTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_write(cx, buf),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_flush(cx),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_shutdown(cx),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_shutdown(cx),
        }
    }
}

type SinkConnection = SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>;
type StreamConnection = SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>;

pub struct ClientState {
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
    handler: Option<Arc<dyn MessageHandler>>,
    heartbeat: u32,
    max_frame_size: u32,
    last_heatbeat: Instant,
    heartbeat_task: Option<task::TaskHandle>,
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
    filtering_supported: bool,
    client_properties: HashMap<String, String>,
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
            last_heatbeat: Instant::now(),
            heartbeat_task: None,
        };
        let mut client = Client {
            dispatcher,
            opts: broker,
            channel: Arc::new(sender),
            state: Arc::new(RwLock::new(state)),
            tune_notifier: Arc::new(Notify::new()),
            publish_sequence: Arc::new(AtomicU64::new(1)),
            filtering_supported: false,
            client_properties: HashMap::new(),
        };

        const VERSION: &str = env!("CARGO_PKG_VERSION");

        client
            .client_properties
            .insert(String::from("product"), String::from("RabbitMQ"));
        client
            .client_properties
            .insert(String::from("version"), String::from(VERSION));
        client
            .client_properties
            .insert(String::from("platform"), String::from("Rust"));
        client.client_properties.insert(
            String::from("copyright"),
            String::from("Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries."));
        client.client_properties.insert(
            String::from("information"),
            String::from(
                "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
            ),
        );
        client.client_properties.insert(
            String::from("connection_name"),
            client.opts.client_provided_name.clone(),
        );

        client.initialize(receiver).await?;

        let command_versions = client.exchange_command_versions().await?;
        let (_, max_version) = command_versions.key_version(2);
        if max_version >= 2 {
            client.filtering_supported = true
        }
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
        if self.channel.is_closed() {
            return Err(ClientError::AlreadyClosed);
        }
        let _: CloseResponse = self
            .send_and_receive(|correlation_id| {
                CloseRequest::new(correlation_id, ResponseCode::Ok, "Ok".to_owned())
            })
            .await?;

        let mut state = self.state.write().await;

        state.heartbeat_task.take();

        drop(state);
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

    pub async fn partitions(
        &self,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamPartitionsResponse> {
        self.send_and_receive(|correlation_id| {
            SuperStreamPartitionsRequest::new(correlation_id, super_stream)
        })
        .await
    }

    pub async fn route(
        &self,
        routing_key: String,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamRouteResponse> {
        self.send_and_receive(|correlation_id| {
            SuperStreamRouteRequest::new(correlation_id, routing_key, super_stream)
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

    pub async fn create_super_stream(
        &self,
        super_stream: &str,
        partitions: Vec<String>,
        binding_keys: Vec<String>,
        options: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            CreateSuperStreamCommand::new(
                correlation_id,
                super_stream.to_owned(),
                partitions,
                binding_keys,
                options,
            )
        })
        .await
    }

    pub async fn delete_stream(&self, stream: &str) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| Delete::new(correlation_id, stream.to_owned()))
            .await
    }

    pub async fn delete_super_stream(
        &self,
        super_stream: &str,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            DeleteSuperStreamCommand::new(correlation_id, super_stream.to_owned())
        })
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
        let response = self
            .send_and_receive::<QueryOffsetResponse, _, _>(|correlation_id| {
                QueryOffsetRequest::new(correlation_id, reference, stream.to_owned())
            })
            .await?;

        if !response.is_ok() {
            Err(ClientError::RequestError(response.code().clone()))
        } else {
            Ok(response.from_response())
        }
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
        version: u16,
    ) -> RabbitMQStreamResult<Vec<u64>> {
        let messages: Vec<PublishedMessage> = messages
            .into()
            .into_iter()
            .map(|message| {
                let publishing_id: u64 = message
                    .publishing_id()
                    .unwrap_or_else(|| self.publish_sequence.fetch_add(1, Ordering::Relaxed));
                let filter_value = message.filter_value();
                PublishedMessage::new(publishing_id, message.to_message(), filter_value)
            })
            .collect();
        let sequences = messages
            .iter()
            .map(rabbitmq_stream_protocol::types::PublishedMessage::publishing_id)
            .collect();
        let len = messages.len();

        // TODO batch publish with max frame size check
        self.send(PublishCommand::new(publisher_id, messages, version))
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

    pub async fn exchange_command_versions(
        &self,
    ) -> RabbitMQStreamResult<ExchangeCommandVersionsResponse> {
        self.send_and_receive::<ExchangeCommandVersionsResponse, _, _>(|correlation_id| {
            ExchangeCommandVersionsRequest::new(correlation_id, vec![])
        })
        .await
    }

    pub fn filtering_supported(&self) -> bool {
        self.filtering_supported
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
        let stream = broker.build_generic_tcp_stream().await?;
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

        // Start heartbeat task after connection is established
        self.start_hearbeat_task(self.state.write().await.deref_mut());

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

    fn negotiate_value(&self, client: u32, server: u32) -> u32 {
        match (client, server) {
            (client, server) if client == 0 || server == 0 => client.max(server),
            (client, server) => client.min(server),
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

        let response = self
            .send_and_receive::<GenericResponse, _, _>(|correlation_id| {
                SaslAuthenticateCommand::new(
                    correlation_id,
                    "PLAIN".to_owned(),
                    auth_data.as_bytes().to_vec(),
                )
            })
            .await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(ClientError::RequestError(response.code().clone()))
        }
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
        let Some((correlation_id, mut receiver)) = self.dispatcher.response_channel() else {
            trace!("Connection is closed here");
            return Err(ClientError::ConnectionClosed);
        };

        self.channel
            .send(msg_factory(correlation_id).into())
            .await?;

        let response = receiver.recv().await.ok_or(ClientError::ConnectionClosed)?;

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
        .and_then(|open| {
            if open.is_ok() {
                Ok(open.connection_properties)
            } else {
                Err(ClientError::RequestError(open.code().clone()))
            }
        })
    }

    async fn peer_properties(&self) -> Result<HashMap<String, String>, ClientError> {
        self.send_and_receive::<PeerPropertiesResponse, _, _>(|correlation_id| {
            PeerPropertiesCommand::new(correlation_id, self.client_properties.clone())
        })
        .await
        .map(|peer_properties| peer_properties.server_properties)
    }

    async fn handle_tune_command(&self, tunes: &TunesCommand) {
        let mut state = self.state.write().await;
        state.heartbeat = self.negotiate_value(self.opts.heartbeat, tunes.heartbeat);
        state.max_frame_size = self.negotiate_value(self.opts.max_frame_size, tunes.max_frame_size);

        let heart_beat = state.heartbeat;
        let max_frame_size = state.max_frame_size;

        trace!(
            "Handling tune with frame size {} and heartbeat {}",
            max_frame_size,
            heart_beat
        );

        if state.heartbeat_task.take().is_some() {
            self.start_hearbeat_task(&mut state);
        }

        drop(state);

        let _ = self
            .channel
            .send(TunesCommand::new(max_frame_size, heart_beat).into())
            .await;

        self.tune_notifier.notify_one();
    }

    fn start_hearbeat_task(&self, state: &mut ClientState) {
        if state.heartbeat == 0 {
            return;
        }
        let heartbeat_interval = (state.heartbeat / 2).max(1);
        let channel = self.channel.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                trace!("Sending heartbeat");
                let _ = channel.send(HeartBeatCommand::default().into()).await;
                tokio::time::sleep(Duration::from_secs(heartbeat_interval.into())).await;
            }
        })
        .into();
        state.heartbeat_task = Some(heartbeat_task);
    }

    async fn handle_heart_beat_command(&self) {
        trace!("Received heartbeat");
        let mut state = self.state.write().await;
        state.last_heatbeat = Instant::now();
    }

    pub async fn consumer_update(
        &self,
        correlation_id: u32,
        offset_specification: OffsetSpecification,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|_| {
            ConsumerUpdateRequestCommand::new(correlation_id, 1, offset_specification)
        })
        .await
    }
}
