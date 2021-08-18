use std::io::Write;

use crate::{
    codec::{decoder::read_u32, Decoder, Encoder},
    commands::{
        close::CloseRequest, create_stream::CreateStreamCommand, credit::CreditCommand,
        declare_publisher::DeclarePublisherCommand, delete::Delete,
        delete_publisher::DeletePublisherCommand, heart_beat::HeartBeatCommand,
        metadata::MetadataCommand, open::OpenCommand, peer_properties::PeerPropertiesCommand,
        publish::PublishCommand, query_offset::QueryOffsetRequest,
        query_publisher_sequence::QueryPublisherRequest,
        sasl_authenticate::SaslAuthenticateCommand, sasl_handshake::SaslHandshakeCommand,
        store_offset::StoreOffset, subscribe::SubscribeCommand, tune::TunesCommand,
        unsubscribe::UnSubscribeCommand,
    },
    error::{DecodeError, EncodeError},
    protocol::commands::*,
    types::Header,
};

use byteorder::{BigEndian, WriteBytesExt};
mod shims;
#[derive(Debug, PartialEq)]
pub struct Request {
    header: Header,
    kind: RequestKind,
}

impl Request {
    /// Get a reference to the request's kind.
    pub fn kind(&self) -> &RequestKind {
        &self.kind
    }

    /// Get a reference to the request's header.
    pub fn header(&self) -> &Header {
        &self.header
    }
}
#[derive(Debug, PartialEq)]
pub enum RequestKind {
    PeerProperties(PeerPropertiesCommand),
    SaslHandshake(SaslHandshakeCommand),
    SaslAuthenticate(SaslAuthenticateCommand),
    Tunes(TunesCommand),
    Open(OpenCommand),
    Close(CloseRequest),
    Delete(Delete),
    CreateStream(CreateStreamCommand),
    Subscribe(SubscribeCommand),
    Credit(CreditCommand),
    Metadata(MetadataCommand),
    DeclarePublisher(DeclarePublisherCommand),
    DeletePublisher(DeletePublisherCommand),
    Heartbeat(HeartBeatCommand),
    Publish(PublishCommand),
    QueryOffset(QueryOffsetRequest),
    QueryPublisherSequence(QueryPublisherRequest),
    StoreOffset(StoreOffset),
    Unsubscribe(UnSubscribeCommand),
}

impl Encoder for RequestKind {
    fn encoded_size(&self) -> u32 {
        match self {
            RequestKind::PeerProperties(peer) => peer.encoded_size(),
            RequestKind::SaslHandshake(handshake) => handshake.encoded_size(),
            RequestKind::SaslAuthenticate(authenticate) => authenticate.encoded_size(),
            RequestKind::Tunes(tunes) => tunes.encoded_size(),
            RequestKind::Open(open) => open.encoded_size(),
            RequestKind::Delete(delete) => delete.encoded_size(),
            RequestKind::CreateStream(create_stream) => create_stream.encoded_size(),
            RequestKind::Subscribe(subscribe) => subscribe.encoded_size(),
            RequestKind::Credit(credit) => credit.encoded_size(),
            RequestKind::Metadata(metadata) => metadata.encoded_size(),
            RequestKind::Close(close) => close.encoded_size(),
            RequestKind::DeclarePublisher(declare_publisher) => declare_publisher.encoded_size(),
            RequestKind::DeletePublisher(delete_publisher) => delete_publisher.encoded_size(),
            RequestKind::Heartbeat(heartbeat) => heartbeat.encoded_size(),
            RequestKind::Publish(publish) => publish.encoded_size(),
            RequestKind::QueryOffset(query_offset) => query_offset.encoded_size(),
            RequestKind::QueryPublisherSequence(query_publisher) => query_publisher.encoded_size(),
            RequestKind::StoreOffset(store_offset) => store_offset.encoded_size(),
            RequestKind::Unsubscribe(unsubscribe) => unsubscribe.encoded_size(),
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        match self {
            RequestKind::PeerProperties(peer) => peer.encode(writer),
            RequestKind::SaslHandshake(handshake) => handshake.encode(writer),
            RequestKind::SaslAuthenticate(authenticate) => authenticate.encode(writer),
            RequestKind::Tunes(tunes) => tunes.encode(writer),
            RequestKind::Open(open) => open.encode(writer),
            RequestKind::Delete(delete) => delete.encode(writer),
            RequestKind::CreateStream(create_stream) => create_stream.encode(writer),
            RequestKind::Subscribe(subscribe) => subscribe.encode(writer),
            RequestKind::Credit(credit) => credit.encode(writer),
            RequestKind::Metadata(metadata) => metadata.encode(writer),
            RequestKind::Close(close) => close.encode(writer),
            RequestKind::DeclarePublisher(declare_publisher) => declare_publisher.encode(writer),
            RequestKind::DeletePublisher(delete_publisher) => delete_publisher.encode(writer),
            RequestKind::Heartbeat(heartbeat) => heartbeat.encode(writer),
            RequestKind::Publish(publish) => publish.encode(writer),
            RequestKind::QueryOffset(query_offset) => query_offset.encode(writer),
            RequestKind::QueryPublisherSequence(query_publisher) => query_publisher.encode(writer),
            RequestKind::StoreOffset(store_offset) => store_offset.encode(writer),
            RequestKind::Unsubscribe(unsubcribe) => unsubcribe.encode(writer),
        }
    }
}
impl Encoder for Request {
    fn encoded_size(&self) -> u32 {
        self.header.encoded_size() + self.kind.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.encoded_size())?;
        self.header.encode(writer)?;
        self.kind.encode(writer)?;
        Ok(())
    }
}

impl Decoder for Request {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, _) = read_u32(input)?;
        let (input, header) = Header::decode(input)?;

        let (input, cmd) = match header.key() {
            COMMAND_OPEN => OpenCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_PEER_PROPERTIES => {
                PeerPropertiesCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_SASL_HANDSHAKE => {
                SaslHandshakeCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_SASL_AUTHENTICATE => {
                SaslAuthenticateCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_TUNE => TunesCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_DELETE_STREAM => Delete::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_CREATE_STREAM => {
                CreateStreamCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_METADATA => MetadataCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_CLOSE => CloseRequest::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_CREDIT => CreditCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_DECLARE_PUBLISHER => {
                DeclarePublisherCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_DELETE_PUBLISHER => {
                DeletePublisherCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }

            COMMAND_HEARTBEAT => {
                HeartBeatCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_PUBLISH => PublishCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_QUERY_OFFSET => {
                QueryOffsetRequest::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_QUERY_PUBLISHER_SEQUENCE => {
                QueryPublisherRequest::decode(input).map(|(i, kind)| (i, kind.into()))?
            }

            COMMAND_STORE_OFFSET => StoreOffset::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_SUBSCRIBE => {
                SubscribeCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_UNSUBSCRIBE => {
                UnSubscribeCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            n => return Err(DecodeError::UsupportedResponseType(n)),
        };
        Ok((input, Request { header, kind: cmd }))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        codec::{Decoder, Encoder},
        commands::{
            close::CloseRequest, create_stream::CreateStreamCommand, credit::CreditCommand,
            declare_publisher::DeclarePublisherCommand, delete::Delete,
            delete_publisher::DeletePublisherCommand, heart_beat::HeartBeatCommand,
            metadata::MetadataCommand, open::OpenCommand, peer_properties::PeerPropertiesCommand,
            publish::PublishCommand, query_offset::QueryOffsetRequest,
            query_publisher_sequence::QueryPublisherRequest,
            sasl_authenticate::SaslAuthenticateCommand, sasl_handshake::SaslHandshakeCommand,
            store_offset::StoreOffset, subscribe::SubscribeCommand, tune::TunesCommand,
            unsubscribe::UnSubscribeCommand, Command,
        },
    };

    use std::fmt::Debug;

    use fake::{Dummy, Fake, Faker};

    use super::Request;

    #[test]
    fn request_open_test() {
        request_encode_decode_test::<OpenCommand>()
    }

    #[test]
    fn request_peer_properties_test() {
        request_encode_decode_test::<PeerPropertiesCommand>()
    }

    #[test]
    fn request_create_stream_test() {
        request_encode_decode_test::<CreateStreamCommand>()
    }

    #[test]
    fn request_delete_stream_test() {
        request_encode_decode_test::<Delete>()
    }

    #[test]
    fn request_sasl_authenticate_test() {
        request_encode_decode_test::<SaslAuthenticateCommand>()
    }

    #[test]
    fn request_sasl_handshake_test() {
        request_encode_decode_test::<SaslHandshakeCommand>()
    }

    #[test]
    fn request_tune_test() {
        request_encode_decode_test::<TunesCommand>()
    }

    #[test]
    fn request_metadata_test() {
        request_encode_decode_test::<MetadataCommand>()
    }
    #[test]
    fn request_close_test() {
        request_encode_decode_test::<CloseRequest>()
    }

    #[test]
    fn request_credit_test() {
        request_encode_decode_test::<CreditCommand>()
    }

    #[test]
    fn request_declare_publisher_test() {
        request_encode_decode_test::<DeclarePublisherCommand>()
    }
    #[test]
    fn request_delete_publisher_test() {
        request_encode_decode_test::<DeletePublisherCommand>()
    }
    #[test]
    fn request_heartbeat_test() {
        request_encode_decode_test::<HeartBeatCommand>()
    }

    #[test]
    fn request_publish_test() {
        request_encode_decode_test::<PublishCommand>()
    }
    #[test]
    fn request_query_offset_test() {
        request_encode_decode_test::<QueryOffsetRequest>()
    }
    #[test]
    fn request_query_publisher_test() {
        request_encode_decode_test::<QueryPublisherRequest>()
    }

    #[test]
    fn request_store_offset_test() {
        request_encode_decode_test::<StoreOffset>()
    }
    #[test]
    fn request_subscribe_test() {
        request_encode_decode_test::<SubscribeCommand>()
    }
    #[test]
    fn request_unsubscribe_test() {
        request_encode_decode_test::<UnSubscribeCommand>()
    }
    fn request_encode_decode_test<T>()
    where
        T: Dummy<Faker> + Encoder + Decoder + Debug + PartialEq + Command + Into<Request>,
    {
        let command: T = Faker.fake();

        let request: Request = command.into();

        let mut buffer = vec![];
        let _ = request.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Request::decode(&buffer).unwrap();

        assert_eq!(buffer[4..].len(), decoded.encoded_size() as usize);

        assert_eq!(request, decoded);

        assert!(remaining.is_empty());
    }
}
