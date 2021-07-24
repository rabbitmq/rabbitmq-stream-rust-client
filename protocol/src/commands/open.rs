use std::{collections::HashMap, io::Write};

use crate::{
    codec::{Decoder, Encoder},
    types::CorrelationId,
};
use byteorder::{BigEndian, WriteBytesExt};

#[derive(PartialEq, Debug)]
pub struct OpenRequest {
    virtual_host: String,
}

impl Encoder for OpenRequest {
    fn encode(&self, writer: &mut impl Write) -> Result<(), ()> {
        self.encode_str(writer, &self.virtual_host)?;
        Ok(())
    }
}

pub enum ResponseCode {
    Ok,
    STREAM_DOES_NOT_EXIST,
    SUBSCRIPTION_ID_ALREADY_EXISTS,
    SUBSCRIPTION_ID_DOES_NOT_EXIST,
    STREAM_ALREADY_EXISTS,
    STREAM_NOT_AVAILABLE,
    SASL_MECHANISM_NOT_SUPPORTED,
    AUTHENTICATION_FAILURE,
    SASL_ERROR,
    SASL_CHALLENGE,
    AUTHENTICATION_FAILURE_LOOPBACK,
    VIRTUAL_HOST_ACCESS_FAILURE,
    UNKNOWN_FRAME,
    FRAME_TOO_LARGE,
    INTERNAL_ERROR,
    ACCESS_REFUSED,
    PRECONDITION_FAILED,
    PUBLISHER_DOES_NOT_EXIST,
}
pub struct OpenResponse {
    correlation_id: CorrelationId,
    response_code: ResponseCode,
    connection_properties: HashMap<String, String>,
}

impl OpenResponse {
    fn deserialize(buffer: &[u8]) -> OpenResponse {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    impl Decoder for OpenRequest {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), ()> {
            let (remaining, virtual_host) = Self::decode_str(input)?;

            Ok((
                remaining,
                OpenRequest {
                    virtual_host: virtual_host.unwrap(),
                },
            ))
        }
    }
    use crate::{
        codec::{Decoder, Encoder},
        request::Request,
    };

    use super::OpenRequest;

    #[test]
    fn open_request_ser_der() {
        let mut buffer = vec![];

        let open = OpenRequest {
            virtual_host: "test".to_owned(),
        };

        let _ = open.encode(&mut buffer);

        let (remaining, decoded) = OpenRequest::decode(&buffer).unwrap();

        assert_eq!(open, decoded);

        dbg!(remaining);

        assert!(remaining.is_empty());
    }
}
