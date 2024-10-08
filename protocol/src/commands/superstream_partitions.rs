use std::io::Write;

#[cfg(test)]
use fake::Fake;
use super::Command;
use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PARTITIONS,
    FromResponse, ResponseCode,
};

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct SuperStreamPartitionsRequest {
    correlation_id: u32,
    super_stream: String,
}

impl SuperStreamPartitionsRequest {
    pub fn new(correlation_id: u32, super_stream: String) -> Self {
        Self {
            correlation_id,
            super_stream,
        }
    }
}

impl Encoder for SuperStreamPartitionsRequest {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.super_stream.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.super_stream.as_str().encode(writer)?;
        Ok(())
    }
}

impl Decoder for SuperStreamPartitionsRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, super_stream) = Option::decode(input)?;

        Ok((
            input,
            SuperStreamPartitionsRequest {
                correlation_id,
                super_stream: super_stream.unwrap(),
            },
        ))
    }
}

impl Command for SuperStreamPartitionsRequest {
    fn key(&self) -> u16 {
        COMMAND_PARTITIONS
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct SuperStreamPartitionsResponse {
    pub(crate) correlation_id: u32,
    response_code: ResponseCode,
    pub streams: Vec<String>,
}

impl SuperStreamPartitionsResponse {
    pub fn new(correlation_id: u32, streams: Vec<String>, response_code: ResponseCode) -> Self {
        Self {
            correlation_id,
            response_code,
            streams,
        }
    }
    pub fn is_ok(&self) -> bool {
        self.response_code == ResponseCode::Ok
    }
}

impl Encoder for SuperStreamPartitionsResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.streams.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
            + self.streams.encoded_size()
    }
}

impl Decoder for SuperStreamPartitionsResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, streams) = <Vec<String>>::decode(input)?;

        Ok((
            input,
            SuperStreamPartitionsResponse {
                correlation_id,
                response_code,
                streams,
            },
        ))
    }
}

impl FromResponse for SuperStreamPartitionsResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::SuperStreamPartitions(partitions_response) => {
                Some(partitions_response)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::SuperStreamPartitionsRequest;
    use super::SuperStreamPartitionsResponse;

    #[test]
    fn super_stream_partition_request_test() {
        command_encode_decode_test::<SuperStreamPartitionsRequest>();
    }

    #[test]
    fn super_stream_partition_response_test() {
        command_encode_decode_test::<SuperStreamPartitionsResponse>();
    }
}
