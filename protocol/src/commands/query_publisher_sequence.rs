use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_QUERY_PUBLISHER_SEQUENCE,
    ResponseCode,
};
use std::io::Write;

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct QueryPublisherRequest {
    correlation_id: u32,
    publisher_reference: String,
    stream: String,
}

impl QueryPublisherRequest {
    pub fn new(correlation_id: u32, publisher_reference: String, stream: String) -> Self {
        Self {
            correlation_id,
            publisher_reference,
            stream,
        }
    }
}

impl Encoder for QueryPublisherRequest {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        let size = self.publisher_reference.len();
        if size >= 256 {
            return Err(EncodeError::MaxSizeError(size));
        }

        self.correlation_id.encode(writer)?;
        self.publisher_reference.as_str().encode(writer)?;
        self.stream.as_str().encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        let stream_len = self.stream.len() as u32;
        let publisher_reference_len = self.publisher_reference.len() as u32;
        self.correlation_id.encoded_size() + stream_len + publisher_reference_len
    }
}

impl Command for QueryPublisherRequest {
    fn key(&self) -> u16 {
        COMMAND_QUERY_PUBLISHER_SEQUENCE
    }
}

impl Decoder for QueryPublisherRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, opt_publisher_reference) = Option::decode(input)?;

        if let Some(publisher_reference) = opt_publisher_reference {
            match publisher_reference.len() {
                0..=255 => {
                    let (input, opt_stream) = Option::decode(input)?;

                    return Ok((
                        input,
                        QueryPublisherRequest {
                            correlation_id,
                            publisher_reference,
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
pub struct QueryPublisherResponse {
    correlation_id: u32,
    response_code: ResponseCode,
    sequence: u64,
}

impl QueryPublisherResponse {
    pub fn new(correlation_id: u32, response_code: ResponseCode, sequence: u64) -> Self {
        Self {
            correlation_id,
            response_code,
            sequence,
        }
    }
}

impl Encoder for QueryPublisherResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.sequence.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.sequence.encoded_size()
            + self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
    }
}

impl Decoder for QueryPublisherResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, sequence) = u64::decode(input)?;
        Ok((
            input,
            QueryPublisherResponse {
                correlation_id,
                response_code,
                sequence,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::QueryPublisherRequest;
    use super::QueryPublisherResponse;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn query_publisher_request_test() {
        command_encode_decode_test::<QueryPublisherRequest>();
    }

    #[test]
    fn query_publisher_response_test() {
        command_encode_decode_test::<QueryPublisherResponse>();
    }
}
