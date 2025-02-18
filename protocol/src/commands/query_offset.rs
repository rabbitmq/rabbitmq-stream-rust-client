use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_QUERY_OFFSET,
    FromResponse, ResponseCode,
};
use std::io::Write;

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct QueryOffsetRequest {
    correlation_id: u32,
    reference: String,
    stream: String,
}

impl QueryOffsetRequest {
    pub fn new(correlation_id: u32, reference: String, stream: String) -> Self {
        Self {
            correlation_id,
            reference,
            stream,
        }
    }
}

impl Encoder for QueryOffsetRequest {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.stream.as_str().encoded_size()
            + self.reference.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        let size = self.reference.len();
        if size >= 256 {
            return Err(EncodeError::MaxSizeError(size));
        }

        self.correlation_id.encode(writer)?;
        self.reference.as_str().encode(writer)?;
        self.stream.as_str().encode(writer)?;
        Ok(())
    }
}

impl Command for QueryOffsetRequest {
    fn key(&self) -> u16 {
        COMMAND_QUERY_OFFSET
    }
}

impl Decoder for QueryOffsetRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, opt_reference) = Option::decode(input)?;

        if let Some(reference) = opt_reference {
            match reference.len() {
                0..=255 => {
                    let (input, opt_stream) = Option::decode(input)?;

                    return Ok((
                        input,
                        QueryOffsetRequest {
                            correlation_id,
                            reference,
                            stream: opt_stream.unwrap(),
                        },
                    ));
                }
                size => return Err(DecodeError::MismatchSize(size)),
            }
        }

        Err(DecodeError::Empty)
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct QueryOffsetResponse {
    pub(crate) correlation_id: u32,
    response_code: ResponseCode,
    offset: u64,
}

impl QueryOffsetResponse {
    pub fn new(correlation_id: u32, response_code: ResponseCode, offset: u64) -> Self {
        Self {
            correlation_id,
            response_code,
            offset,
        }
    }

    pub fn from_response(&self) -> u64 {
        self.offset
    }

    pub fn code(&self) -> &ResponseCode {
        &self.response_code
    }

    pub fn is_ok(&self) -> bool {
        self.response_code == ResponseCode::Ok
    }
}

impl Encoder for QueryOffsetResponse {
    fn encoded_size(&self) -> u32 {
        self.offset.encoded_size()
            + self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.offset.encode(writer)?;
        Ok(())
    }
}

impl Decoder for QueryOffsetResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, offset) = u64::decode(input)?;
        Ok((
            input,
            QueryOffsetResponse {
                correlation_id,
                response_code,
                offset,
            },
        ))
    }
}

impl FromResponse for QueryOffsetResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::QueryOffset(query_offset) => Some(query_offset),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryOffsetRequest;
    use super::QueryOffsetResponse;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn query_offset_request_response_test() {
        command_encode_decode_test::<QueryOffsetRequest>();
    }

    #[test]
    fn query_offset_response_response_test() {
        command_encode_decode_test::<QueryOffsetResponse>();
    }
}
