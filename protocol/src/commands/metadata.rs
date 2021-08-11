use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_METADATA,
    ResponseCode,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct MetadataCommand {
    correlation_id: u32,
    stream: String,
}

impl MetadataCommand {
    pub fn new(correlation_id: u32, stream: String) -> Self {
        Self {
            correlation_id,
            stream,
        }
    }
}

impl Encoder for MetadataCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.stream.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.stream.as_str().encode(writer)?;
        Ok(())
    }
}

impl Decoder for MetadataCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, stream) = Option::decode(input)?;

        Ok((
            input,
            MetadataCommand {
                correlation_id,
                stream: stream.unwrap(),
            },
        ))
    }
}

impl Command for MetadataCommand {
    fn key(&self) -> u16 {
        COMMAND_METADATA
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
pub struct MetadataResponse {
    pub(crate) correlation_id: u32,
    pub reference: u16,
    pub host: String,
    pub port: u32,
    pub stream_name: String,
    pub code: ResponseCode,
    pub leader_reference: u16,
    pub replicas_references: Vec<u32>,
}

impl Decoder for MetadataResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, reference) = u16::decode(input)?;
        let (input, host) = Option::decode(input)?;
        let (input, port) = u32::decode(input)?;
        let (input, stream_name) = Option::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, leader_reference) = u16::decode(input)?;
        let (input, replicas_references) = Vec::decode(input)?;

        Ok((
            input,
            MetadataResponse {
                correlation_id,
                reference,
                host: host.unwrap(),
                port,
                stream_name: stream_name.unwrap(),
                code,
                leader_reference,
                replicas_references,
            },
        ))
    }
}

impl Encoder for MetadataResponse {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        self.correlation_id.encode(writer)?;
        self.reference.encode(writer)?;
        self.host.as_str().encode(writer)?;
        self.port.encode(writer)?;
        self.stream_name.as_str().encode(writer)?;
        self.code.encode(writer)?;
        self.leader_reference.encode(writer)?;
        self.replicas_references.encode(writer)?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {

    use crate::commands::metadata::{MetadataCommand, MetadataResponse};
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn metadata_request_test() {
        command_encode_decode_test::<MetadataCommand>()
    }

    #[test]
    fn metadata_response_test() {
        command_encode_decode_test::<MetadataResponse>();
    }
}
