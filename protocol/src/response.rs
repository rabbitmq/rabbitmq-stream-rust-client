use std::convert::{TryFrom, TryInto};

use crate::{
    codec::{read_u16, read_u32, Decoder},
    commands::open::OpenResponse,
    error::DecodeError,
    protocol::{commands::COMMAND_OPEN, responses::RESPONSE_CODE_OK},
    types::Header,
};

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
    kind: Option<ResponseKind>,
}

#[derive(Debug, PartialEq)]
pub enum ResponseKind {
    Open(OpenResponse),
}

impl Decoder for Response {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, _) = read_u32(input)?;
        let (input, header) = Header::decode(input)?;

        let (input, kind) = match header.key() {
            COMMAND_OPEN => {
                OpenResponse::decode(input).map(|(i, kind)| (i, Some(ResponseKind::Open(kind))))?
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
            // TODO map all response code
            RESPONSE_CODE_OK => Ok(ResponseCode::Ok),
            _ => Err(DecodeError::UnknownResponseCode(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use byteorder::{BigEndian, WriteBytesExt};

    use crate::{
        codec::{Decoder, Encoder},
        commands::open::OpenResponse,
        protocol::{
            commands::COMMAND_OPEN, responses::RESPONSE_CODE_OK, version::PROTOCOL_VERSION,
        },
        response::ResponseCode,
        types::Header,
    };

    use super::{Response, ResponseKind};
    impl Encoder for ResponseCode {
        fn encoded_size(&self) -> u32 {
            2
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            // TODO convert ResponseCode to u16
            writer.write_u16::<BigEndian>(RESPONSE_CODE_OK)?;

            Ok(())
        }
    }
    impl Encoder for ResponseKind {
        fn encoded_size(&self) -> u32 {
            match self {
                ResponseKind::Open(open) => open.encoded_size(),
            }
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            match self {
                ResponseKind::Open(open) => open.encode(writer),
            }
        }
    }

    impl Encoder for Response {
        fn encoded_size(&self) -> u32 {
            self.header.encoded_size()
                + 2
                + self
                    .kind
                    .as_ref()
                    .map(|kind| kind.encoded_size())
                    .unwrap_or(0)
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            writer.write_u32::<BigEndian>(self.encoded_size())?;
            self.header.encode(writer)?;

            if let Some(kind) = &self.kind {
                kind.encode(writer)?;
            }
            Ok(())
        }
    }

    #[test]
    fn response_test() {
        let mut properties = HashMap::new();

        properties.insert("test".to_owned(), "test".to_owned());

        let open_response = OpenResponse {
            connection_properties: properties,
            correlation_id: 1.into(),
            code: ResponseCode::Ok,
        };

        let response = Response {
            header: Header::new(COMMAND_OPEN, PROTOCOL_VERSION),
            kind: Some(ResponseKind::Open(open_response)),
        };

        let mut buffer = vec![];

        response.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Response::decode(&buffer).unwrap();

        assert_eq!(response, decoded);

        assert!(remaining.is_empty());
    }
}
