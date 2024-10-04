use std::convert::TryInto;

use crate::{
    codec::{
        decoder::{read_u16, read_u32},
        Decoder,
    },
    commands::{
        close::CloseResponse, credit::CreditResponse, deliver::DeliverCommand,
        exchange_command_versions::ExchangeCommandVersionsResponse, generic::GenericResponse,
        heart_beat::HeartbeatResponse, metadata::MetadataResponse,
        metadata_update::MetadataUpdateCommand, open::OpenResponse,
        peer_properties::PeerPropertiesResponse, publish_confirm::PublishConfirm,
        publish_error::PublishErrorResponse, query_offset::QueryOffsetResponse,
        query_publisher_sequence::QueryPublisherResponse, sasl_handshake::SaslHandshakeResponse,
        tune::TunesCommand,
    },
    error::DecodeError,
    protocol::commands::*,
    types::Header,
};
mod shims;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ResponseCode {
    Ok,
    StreamDoesNotExist,
    SubscriptionIdAlreadyExists,
    SubscriptionIdDoesNotExist,
    StreamAlreadyExists,
    StreamNotAvailable,
    SaslMechanismNotSupported,
    AuthenticationFailure,
    SaslError,
    SaslChallange,
    AuthenticationFailureLoopback,
    VirtualHostAccessFailure,
    UnknownFrame,
    FrameTooLarge,
    InternalError,
    AccessRefused,
    PrecoditionFailed,
    PublisherDoesNotExist,
    OffsetNotFound,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Response {
    header: Header,
    pub(crate) kind: ResponseKind,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResponseKind {
    Open(OpenResponse),
    Close(CloseResponse),
    PeerProperties(PeerPropertiesResponse),
    SaslHandshake(SaslHandshakeResponse),
    Generic(GenericResponse),
    Tunes(TunesCommand),
    Deliver(DeliverCommand),
    Heartbeat(HeartbeatResponse),
    Metadata(MetadataResponse),
    MetadataUpdate(MetadataUpdateCommand),
    PublishConfirm(PublishConfirm),
    PublishError(PublishErrorResponse),
    QueryOffset(QueryOffsetResponse),
    QueryPublisherSequence(QueryPublisherResponse),
    Credit(CreditResponse),
    ExchangeCommandVersions(ExchangeCommandVersionsResponse),
}

impl Response {
    pub fn new(header: Header, kind: ResponseKind) -> Self {
        Self { header, kind }
    }

    pub fn correlation_id(&self) -> Option<u32> {
        match &self.kind {
            ResponseKind::Open(open) => Some(open.correlation_id),
            ResponseKind::Close(close) => Some(close.correlation_id),
            ResponseKind::PeerProperties(peer_properties) => Some(peer_properties.correlation_id),
            ResponseKind::SaslHandshake(handshake) => Some(handshake.correlation_id),
            ResponseKind::Generic(generic) => Some(generic.correlation_id),
            ResponseKind::Metadata(metadata) => Some(metadata.correlation_id),
            ResponseKind::QueryOffset(query_offset) => Some(query_offset.correlation_id),
            ResponseKind::QueryPublisherSequence(query_publisher) => {
                Some(query_publisher.correlation_id)
            }
            ResponseKind::MetadataUpdate(_) => None,
            ResponseKind::PublishConfirm(_) => None,
            ResponseKind::PublishError(_) => None,
            ResponseKind::Tunes(_) => None,
            ResponseKind::Heartbeat(_) => None,
            ResponseKind::Deliver(_) => None,
            ResponseKind::Credit(_) => None,
            ResponseKind::ExchangeCommandVersions(exchange_command_versions) => {
                Some(exchange_command_versions.correlation_id)
            }
        }
    }

    pub fn get<T>(self) -> Option<T>
    where
        T: FromResponse,
    {
        T::from_response(self)
    }

    pub fn kind_ref(&self) -> &ResponseKind {
        &self.kind
    }
    pub fn kind(self) -> ResponseKind {
        self.kind
    }
}

impl Decoder for Response {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, _) = read_u32(input)?;

        let (input, header) = Header::decode(input)?;
        let (input, kind) = match header.key() {
            COMMAND_OPEN => {
                OpenResponse::decode(input).map(|(i, kind)| (i, ResponseKind::Open(kind)))?
            }

            COMMAND_CLOSE => {
                CloseResponse::decode(input).map(|(i, kind)| (i, ResponseKind::Close(kind)))?
            }
            COMMAND_PEER_PROPERTIES => PeerPropertiesResponse::decode(input)
                .map(|(i, kind)| (i, ResponseKind::PeerProperties(kind)))?,
            COMMAND_SASL_HANDSHAKE => SaslHandshakeResponse::decode(input)
                .map(|(i, kind)| (i, ResponseKind::SaslHandshake(kind)))?,

            COMMAND_DECLARE_PUBLISHER
            | COMMAND_DELETE_PUBLISHER
            | COMMAND_SASL_AUTHENTICATE
            | COMMAND_SUBSCRIBE
            | COMMAND_UNSUBSCRIBE
            | COMMAND_CREATE_STREAM
            | COMMAND_CREATE_SUPER_STREAM
            | COMMAND_DELETE_SUPER_STREAM
            | COMMAND_DELETE_STREAM => {
                GenericResponse::decode(input).map(|(i, kind)| (i, ResponseKind::Generic(kind)))?
            }
            COMMAND_TUNE => {
                TunesCommand::decode(input).map(|(i, kind)| (i, ResponseKind::Tunes(kind)))?
            }
            COMMAND_DELIVER => DeliverCommand::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::Deliver(kind)))?,

            COMMAND_HEARTBEAT => HeartbeatResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::Heartbeat(kind)))?,
            COMMAND_METADATA => MetadataResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::Metadata(kind)))?,
            COMMAND_METADATA_UPDATE => MetadataUpdateCommand::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::MetadataUpdate(kind)))?,
            COMMAND_PUBLISH_CONFIRM => PublishConfirm::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::PublishConfirm(kind)))?,

            COMMAND_PUBLISH_ERROR => PublishErrorResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::PublishError(kind)))?,

            COMMAND_QUERY_OFFSET => QueryOffsetResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::QueryOffset(kind)))?,

            COMMAND_CREDIT => CreditResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::Credit(kind)))?,

            COMMAND_QUERY_PUBLISHER_SEQUENCE => QueryPublisherResponse::decode(input)
                .map(|(remaining, kind)| (remaining, ResponseKind::QueryPublisherSequence(kind)))?,
            COMMAND_EXCHANGE_COMMAND_VERSIONS => ExchangeCommandVersionsResponse::decode(input)
                .map(|(remaining, kind)| {
                    (remaining, ResponseKind::ExchangeCommandVersions(kind))
                })?,
            n => return Err(DecodeError::UnsupportedResponseType(n)),
        };
        Ok((input, Response { header, kind }))
    }
}

impl Decoder for ResponseCode {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, code) = read_u16(input)?;
        Ok((input, code.try_into()?))
    }
}

pub trait FromResponse
where
    Self: Sized,
{
    fn from_response(response: Response) -> Option<Self>;
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use byteorder::{BigEndian, WriteBytesExt};

    use crate::{
        codec::{Decoder, Encoder},
        commands::{
            close::CloseResponse, deliver::DeliverCommand,
            exchange_command_versions::ExchangeCommandVersionsResponse, generic::GenericResponse,
            heart_beat::HeartbeatResponse, metadata::MetadataResponse,
            metadata_update::MetadataUpdateCommand, open::OpenResponse,
            peer_properties::PeerPropertiesResponse, publish_confirm::PublishConfirm,
            publish_error::PublishErrorResponse, query_offset::QueryOffsetResponse,
            query_publisher_sequence::QueryPublisherResponse,
            sasl_handshake::SaslHandshakeResponse, tune::TunesCommand,
        },
        protocol::{
            commands::{
                COMMAND_CLOSE, COMMAND_DELIVER, COMMAND_HEARTBEAT, COMMAND_METADATA,
                COMMAND_METADATA_UPDATE, COMMAND_OPEN, COMMAND_PEER_PROPERTIES,
                COMMAND_PUBLISH_CONFIRM, COMMAND_PUBLISH_ERROR, COMMAND_QUERY_OFFSET,
                COMMAND_QUERY_PUBLISHER_SEQUENCE, COMMAND_SASL_AUTHENTICATE,
                COMMAND_SASL_HANDSHAKE, COMMAND_TUNE,
            },
            version::PROTOCOL_VERSION,
        },
        response::COMMAND_EXCHANGE_COMMAND_VERSIONS,
        types::Header,
        ResponseCode,
    };

    use super::{Response, ResponseKind};
    impl Encoder for ResponseKind {
        fn encoded_size(&self) -> u32 {
            match self {
                ResponseKind::Open(open) => open.encoded_size(),
                ResponseKind::Close(close) => close.encoded_size(),
                ResponseKind::PeerProperties(peer_properties) => peer_properties.encoded_size(),
                ResponseKind::SaslHandshake(handshake) => handshake.encoded_size(),
                ResponseKind::Generic(generic) => generic.encoded_size(),
                ResponseKind::Tunes(tune) => tune.encoded_size(),
                ResponseKind::Heartbeat(heartbeat) => heartbeat.encoded_size(),
                ResponseKind::Deliver(deliver) => deliver.encoded_size(),
                ResponseKind::Metadata(metadata) => metadata.encoded_size(),
                ResponseKind::MetadataUpdate(metadata) => metadata.encoded_size(),
                ResponseKind::PublishConfirm(publish_confirm) => publish_confirm.encoded_size(),
                ResponseKind::PublishError(publish_error) => publish_error.encoded_size(),
                ResponseKind::QueryOffset(query_offset) => query_offset.encoded_size(),
                ResponseKind::QueryPublisherSequence(query_publisher) => {
                    query_publisher.encoded_size()
                }
                ResponseKind::Credit(credit) => credit.encoded_size(),
                ResponseKind::ExchangeCommandVersions(exchange_command_versions) => {
                    exchange_command_versions.encoded_size()
                }
            }
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            match self {
                ResponseKind::Open(open) => open.encode(writer),
                ResponseKind::Close(close) => close.encode(writer),
                ResponseKind::PeerProperties(peer_properties) => peer_properties.encode(writer),
                ResponseKind::SaslHandshake(handshake) => handshake.encode(writer),
                ResponseKind::Generic(generic) => generic.encode(writer),
                ResponseKind::Tunes(tune) => tune.encode(writer),
                ResponseKind::Heartbeat(heartbeat) => heartbeat.encode(writer),
                ResponseKind::Deliver(deliver) => deliver.encode(writer),
                ResponseKind::Metadata(metadata) => metadata.encode(writer),
                ResponseKind::MetadataUpdate(metadata) => metadata.encode(writer),
                ResponseKind::PublishConfirm(publish_confirm) => publish_confirm.encode(writer),
                ResponseKind::PublishError(publish_error) => publish_error.encode(writer),
                ResponseKind::QueryOffset(query_offset) => query_offset.encode(writer),
                ResponseKind::QueryPublisherSequence(query_publisher) => {
                    query_publisher.encode(writer)
                }
                ResponseKind::Credit(credit) => credit.encode(writer),
                ResponseKind::ExchangeCommandVersions(exchange_command_versions) => {
                    exchange_command_versions.encode(writer)
                }
            }
        }
    }

    impl Encoder for Response {
        fn encoded_size(&self) -> u32 {
            self.header.encoded_size() + 2 + self.kind.encoded_size()
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            writer.write_u32::<BigEndian>(self.encoded_size())?;
            self.header.encode(writer)?;
            self.kind.encode(writer)?;
            Ok(())
        }
    }

    macro_rules! response_test {
        ($ty:ty, $variant:path, $cmd:expr) => {
            use fake::{Fake, Faker};
            let payload: $ty = Faker.fake();

            let response = Response {
                header: Header::new($cmd, PROTOCOL_VERSION),
                kind: $variant(payload),
            };

            let mut buffer = vec![];

            response.encode(&mut buffer).unwrap();

            let (remaining, decoded) = Response::decode(&buffer).unwrap();

            assert_eq!(response, decoded);

            assert!(remaining.is_empty());
        };
    }

    macro_rules! response_payload_test {
        ($payload:expr, $variant:path, $cmd:expr) => {
            let response = Response {
                header: Header::new($cmd, PROTOCOL_VERSION),
                kind: $variant($payload),
            };

            let mut buffer = vec![];

            response.encode(&mut buffer).unwrap();

            let (remaining, decoded) = Response::decode(&buffer).unwrap();

            assert_eq!(response, decoded);

            assert!(remaining.is_empty());
        };
    }

    #[test]
    fn open_response_ok_test() {
        use {fake::Fake, fake::Faker};
        let mut response: OpenResponse = Faker.fake();
        response.code = ResponseCode::Ok;
        response_payload_test!(response, ResponseKind::Open, COMMAND_OPEN);
    }

    #[test]
    fn peer_properties_response_test() {
        response_test!(
            PeerPropertiesResponse,
            ResponseKind::PeerProperties,
            COMMAND_PEER_PROPERTIES
        );
    }

    #[test]
    fn sasl_handshake_response_test() {
        response_test!(
            SaslHandshakeResponse,
            ResponseKind::SaslHandshake,
            COMMAND_SASL_HANDSHAKE
        );
    }

    #[test]
    fn generic_response_test() {
        response_test!(
            GenericResponse,
            ResponseKind::Generic,
            COMMAND_SASL_AUTHENTICATE
        );
    }

    #[test]
    fn tune_response_test() {
        response_test!(TunesCommand, ResponseKind::Tunes, COMMAND_TUNE);
    }
    #[test]
    fn metadata_response_test() {
        response_test!(MetadataResponse, ResponseKind::Metadata, COMMAND_METADATA);
    }
    #[test]
    fn close_response_test() {
        response_test!(CloseResponse, ResponseKind::Close, COMMAND_CLOSE);
    }
    #[test]
    fn deliver_response_test() {
        response_test!(DeliverCommand, ResponseKind::Deliver, COMMAND_DELIVER);
    }
    #[test]
    fn metadata_update_response_test() {
        response_test!(
            MetadataUpdateCommand,
            ResponseKind::MetadataUpdate,
            COMMAND_METADATA_UPDATE
        );
    }
    #[test]
    fn publish_confirm_response_test() {
        response_test!(
            PublishConfirm,
            ResponseKind::PublishConfirm,
            COMMAND_PUBLISH_CONFIRM
        );
    }
    #[test]
    fn publish_error_response_test() {
        response_test!(
            PublishErrorResponse,
            ResponseKind::PublishError,
            COMMAND_PUBLISH_ERROR
        );
    }
    #[test]
    fn query_offset_response_test() {
        response_test!(
            QueryOffsetResponse,
            ResponseKind::QueryOffset,
            COMMAND_QUERY_OFFSET
        );
    }
    #[test]
    fn query_publisher_response_test() {
        response_test!(
            QueryPublisherResponse,
            ResponseKind::QueryPublisherSequence,
            COMMAND_QUERY_PUBLISHER_SEQUENCE
        );
    }
    #[test]
    fn heartbeat_response_test() {
        response_test!(
            HeartbeatResponse,
            ResponseKind::Heartbeat,
            COMMAND_HEARTBEAT
        );
    }

    #[test]
    fn exchange_command_versions_response_test() {
        response_test!(
            ExchangeCommandVersionsResponse,
            ResponseKind::ExchangeCommandVersions,
            COMMAND_EXCHANGE_COMMAND_VERSIONS
        );
    }
}
