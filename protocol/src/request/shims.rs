use crate::{
    commands::{
        close::CloseRequest, create_stream::CreateStreamCommand, credit::CreditCommand,
        declare_publisher::DeclarePublisherCommand, delete::Delete,
        delete_publisher::DeletePublisherCommand,
        exchange_command_versions::ExchangeCommandVersionsRequest, heart_beat::HeartBeatCommand,
        metadata::MetadataCommand, open::OpenCommand, peer_properties::PeerPropertiesCommand,
        publish::PublishCommand, query_offset::QueryOffsetRequest,
        query_publisher_sequence::QueryPublisherRequest,
        sasl_authenticate::SaslAuthenticateCommand, sasl_handshake::SaslHandshakeCommand,
        store_offset::StoreOffset, subscribe::SubscribeCommand, tune::TunesCommand,
        unsubscribe::UnSubscribeCommand, Command,
    },
    types::Header,
    Request, RequestKind,
};
impl<T> From<T> for Request
where
    T: Into<RequestKind> + Command,
{
    fn from(cmd: T) -> Self {
        Request {
            header: Header::new(cmd.key(), cmd.version()),
            kind: cmd.into(),
        }
    }
}

impl From<PeerPropertiesCommand> for RequestKind {
    fn from(cmd: PeerPropertiesCommand) -> Self {
        RequestKind::PeerProperties(cmd)
    }
}

impl From<OpenCommand> for RequestKind {
    fn from(cmd: OpenCommand) -> Self {
        RequestKind::Open(cmd)
    }
}
impl From<SaslHandshakeCommand> for RequestKind {
    fn from(cmd: SaslHandshakeCommand) -> Self {
        RequestKind::SaslHandshake(cmd)
    }
}
impl From<SaslAuthenticateCommand> for RequestKind {
    fn from(cmd: SaslAuthenticateCommand) -> Self {
        RequestKind::SaslAuthenticate(cmd)
    }
}

impl From<TunesCommand> for RequestKind {
    fn from(cmd: TunesCommand) -> Self {
        RequestKind::Tunes(cmd)
    }
}

impl From<CreateStreamCommand> for RequestKind {
    fn from(cmd: CreateStreamCommand) -> Self {
        RequestKind::CreateStream(cmd)
    }
}
impl From<Delete> for RequestKind {
    fn from(cmd: Delete) -> Self {
        RequestKind::Delete(cmd)
    }
}

impl From<SubscribeCommand> for RequestKind {
    fn from(cmd: SubscribeCommand) -> Self {
        RequestKind::Subscribe(cmd)
    }
}

impl From<CreditCommand> for RequestKind {
    fn from(cmd: CreditCommand) -> Self {
        RequestKind::Credit(cmd)
    }
}
impl From<MetadataCommand> for RequestKind {
    fn from(cmd: MetadataCommand) -> Self {
        RequestKind::Metadata(cmd)
    }
}

impl From<CloseRequest> for RequestKind {
    fn from(cmd: CloseRequest) -> Self {
        RequestKind::Close(cmd)
    }
}
impl From<DeclarePublisherCommand> for RequestKind {
    fn from(cmd: DeclarePublisherCommand) -> Self {
        RequestKind::DeclarePublisher(cmd)
    }
}
impl From<DeletePublisherCommand> for RequestKind {
    fn from(cmd: DeletePublisherCommand) -> Self {
        RequestKind::DeletePublisher(cmd)
    }
}

impl From<HeartBeatCommand> for RequestKind {
    fn from(cmd: HeartBeatCommand) -> Self {
        RequestKind::Heartbeat(cmd)
    }
}

impl From<PublishCommand> for RequestKind {
    fn from(cmd: PublishCommand) -> Self {
        RequestKind::Publish(cmd)
    }
}
impl From<QueryOffsetRequest> for RequestKind {
    fn from(cmd: QueryOffsetRequest) -> Self {
        RequestKind::QueryOffset(cmd)
    }
}

impl From<QueryPublisherRequest> for RequestKind {
    fn from(cmd: QueryPublisherRequest) -> Self {
        RequestKind::QueryPublisherSequence(cmd)
    }
}
impl From<StoreOffset> for RequestKind {
    fn from(cmd: StoreOffset) -> Self {
        RequestKind::StoreOffset(cmd)
    }
}
impl From<UnSubscribeCommand> for RequestKind {
    fn from(cmd: UnSubscribeCommand) -> Self {
        RequestKind::Unsubscribe(cmd)
    }
}
impl From<ExchangeCommandVersionsRequest> for RequestKind {
    fn from(cmd: ExchangeCommandVersionsRequest) -> Self {
        RequestKind::ExchangeCommandVersions(cmd)
    }
}
