use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_STORE_OFFSET,
};
use std::io::Write;

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
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

    fn encoded_size(&self) -> u32 {
        let stream_len = self.stream.len() as u32;
        let reference_len = self.reference.len() as u32;
        self.correlation_id.encoded_size() + stream_len + reference_len
    }
}

impl Command for QueryOffsetRequest {
    fn key(&self) -> u16 {
        COMMAND_STORE_OFFSET
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
#[derive(PartialEq, Debug)]
pub struct QueryOffsetResponse {
    correlation_id: u32,
    response_code: u16,
    offset: u64,
}

impl QueryOffsetResponse {
    pub fn new(correlation_id: u32, response_code: u16, offset: u64) -> Self {
        Self {
            correlation_id,
            response_code,
            offset,
        }
    }
}

impl Encoder for QueryOffsetResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.offset.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.offset.encoded_size()
            + self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
    }
}

impl Decoder for QueryOffsetResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = u16::decode(input)?;
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
