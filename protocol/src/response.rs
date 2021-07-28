use std::convert::{TryFrom, TryInto};

use crate::{
    codec::{
        decoder::{read_u16, read_u32},
        Decoder,
    },
    commands::{
        generic::GenericResponse, open::OpenResponse, peer_properties::PeerPropertiesResponse,
        sasl_handshake::SaslHandshakeResponse, tune::TunesCommand,
    },
    error::DecodeError,
    protocol::{
        commands::{
            COMMAND_OPEN, COMMAND_PEER_PROPERTIES, COMMAND_SASL_AUTHENTICATE,
            COMMAND_SASL_HANDSHAKE, COMMAND_TUNE,
        },
        responses::*,
    },
    types::Header,
};

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
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
}
#[derive(Debug, PartialEq)]
pub struct Response {
    header: Header,
    kind: ResponseKind,
}

#[derive(Debug, PartialEq)]
pub enum ResponseKind {
    Open(OpenResponse),
    PeerProperties(PeerPropertiesResponse),
    SaslHandshake(SaslHandshakeResponse),
    Generic(GenericResponse),
    Tunes(TunesCommand),
}

impl Decoder for Response {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, _) = read_u32(input)?;

        let (input, header) = Header::decode(input)?;

        let (input, kind) = match header.key() {
            COMMAND_OPEN => {
                OpenResponse::decode(input).map(|(i, kind)| (i, ResponseKind::Open(kind)))?
            }

            COMMAND_PEER_PROPERTIES => PeerPropertiesResponse::decode(input)
                .map(|(i, kind)| (i, ResponseKind::PeerProperties(kind)))?,
            COMMAND_SASL_HANDSHAKE => SaslHandshakeResponse::decode(input)
                .map(|(i, kind)| (i, ResponseKind::SaslHandshake(kind)))?,

            COMMAND_SASL_AUTHENTICATE => {
                GenericResponse::decode(input).map(|(i, kind)| (i, ResponseKind::Generic(kind)))?
            }
            COMMAND_TUNE => {
                TunesCommand::decode(input).map(|(i, kind)| (i, ResponseKind::Tunes(kind)))?
            }

            n => return Err(DecodeError::UsupportedResponseType(n)),
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

impl TryFrom<u16> for ResponseCode {
    type Error = DecodeError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            RESPONSE_CODE_OK => Ok(ResponseCode::Ok),
            RESPONSE_CODE_STREAM_DOES_NOT_EXIST => Ok(ResponseCode::StreamDoesNotExist),
            RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS => {
                Ok(ResponseCode::SubscriptionIdAlreadyExists)
            }
            RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST => {
                Ok(ResponseCode::SubscriptionIdDoesNotExist)
            }
            RESPONSE_CODE_STREAM_ALREADY_EXISTS => Ok(ResponseCode::StreamAlreadyExists),
            RESPONSE_CODE_STREAM_NOT_AVAILABLE => Ok(ResponseCode::StreamNotAvailable),
            RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED => {
                Ok(ResponseCode::SaslMechanismNotSupported)
            }
            RESPONSE_CODE_AUTHENTICATION_FAILURE => Ok(ResponseCode::AuthenticationFailure),
            RESPONSE_CODE_SASL_ERROR => Ok(ResponseCode::SaslError),
            RESPONSE_CODE_SASL_CHALLENGE => Ok(ResponseCode::SaslChallange),
            RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK => {
                Ok(ResponseCode::AuthenticationFailureLoopback)
            }
            RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE => Ok(ResponseCode::VirtualHostAccessFailure),
            RESPONSE_CODE_UNKNOWN_FRAME => Ok(ResponseCode::UnknownFrame),
            RESPONSE_CODE_FRAME_TOO_LARGE => Ok(ResponseCode::FrameTooLarge),
            RESPONSE_CODE_INTERNAL_ERROR => Ok(ResponseCode::InternalError),
            RESPONSE_CODE_ACCESS_REFUSED => Ok(ResponseCode::AccessRefused),
            RESPONSE_CODE_PRECONDITION_FAILED => Ok(ResponseCode::PrecoditionFailed),
            RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST => Ok(ResponseCode::PublisherDoesNotExist),
            _ => Err(DecodeError::UnknownResponseCode(value)),
        }
    }
}

impl From<&ResponseCode> for u16 {
    fn from(code: &ResponseCode) -> Self {
        match code {
            ResponseCode::Ok => RESPONSE_CODE_OK,
            ResponseCode::StreamDoesNotExist => RESPONSE_CODE_STREAM_DOES_NOT_EXIST,
            ResponseCode::SubscriptionIdAlreadyExists => {
                RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS
            }
            ResponseCode::SubscriptionIdDoesNotExist => {
                RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST
            }
            ResponseCode::StreamAlreadyExists => RESPONSE_CODE_STREAM_ALREADY_EXISTS,
            ResponseCode::StreamNotAvailable => RESPONSE_CODE_STREAM_NOT_AVAILABLE,
            ResponseCode::SaslMechanismNotSupported => RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED,
            ResponseCode::AuthenticationFailure => RESPONSE_CODE_AUTHENTICATION_FAILURE,
            ResponseCode::SaslError => RESPONSE_CODE_SASL_ERROR,
            ResponseCode::SaslChallange => RESPONSE_CODE_SASL_CHALLENGE,
            ResponseCode::AuthenticationFailureLoopback => {
                RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK
            }
            ResponseCode::VirtualHostAccessFailure => RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE,
            ResponseCode::UnknownFrame => RESPONSE_CODE_UNKNOWN_FRAME,
            ResponseCode::FrameTooLarge => RESPONSE_CODE_FRAME_TOO_LARGE,
            ResponseCode::InternalError => RESPONSE_CODE_INTERNAL_ERROR,
            ResponseCode::AccessRefused => RESPONSE_CODE_ACCESS_REFUSED,
            ResponseCode::PrecoditionFailed => RESPONSE_CODE_PRECONDITION_FAILED,
            ResponseCode::PublisherDoesNotExist => RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST,
        }
    }
}

#[cfg(test)]
mod tests {

    use byteorder::{BigEndian, WriteBytesExt};

    use crate::{
        codec::{Decoder, Encoder},
        commands::{
            generic::GenericResponse, open::OpenResponse, peer_properties::PeerPropertiesResponse,
            sasl_handshake::SaslHandshakeResponse, tune::TunesCommand,
        },
        protocol::{
            commands::{
                COMMAND_OPEN, COMMAND_PEER_PROPERTIES, COMMAND_SASL_AUTHENTICATE,
                COMMAND_SASL_HANDSHAKE, COMMAND_TUNE,
            },
            version::PROTOCOL_VERSION,
        },
        types::Header,
    };

    use super::{Response, ResponseKind};
    impl Encoder for ResponseKind {
        fn encoded_size(&self) -> u32 {
            match self {
                ResponseKind::Open(open) => open.encoded_size(),
                ResponseKind::PeerProperties(peer_properties) => peer_properties.encoded_size(),
                ResponseKind::SaslHandshake(handshake) => handshake.encoded_size(),
                ResponseKind::Generic(generic) => generic.encoded_size(),
                ResponseKind::Tunes(tune) => tune.encoded_size(),
            }
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            match self {
                ResponseKind::Open(open) => open.encode(writer),
                ResponseKind::PeerProperties(peer_properties) => peer_properties.encode(writer),
                ResponseKind::SaslHandshake(handshake) => handshake.encode(writer),
                ResponseKind::Generic(generic) => generic.encode(writer),
                ResponseKind::Tunes(tune) => tune.encode(writer),
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

    #[test]
    fn open_response_test() {
        response_test!(OpenResponse, ResponseKind::Open, COMMAND_OPEN);
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
}
