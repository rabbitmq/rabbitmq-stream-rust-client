use std::io::Write;

use crate::{
    codec::{decoder::read_vec, Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_METADATA,
    FromResponse, ResponseCode,
};

use super::Command;

use byteorder::{BigEndian, WriteBytesExt};
#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct MetadataCommand {
    correlation_id: u32,
    streams: Vec<String>,
}

impl MetadataCommand {
    pub fn new(correlation_id: u32, streams: Vec<String>) -> Self {
        Self {
            correlation_id,
            streams,
        }
    }
}

impl Encoder for MetadataCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.streams.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.streams.encode(writer)?;
        Ok(())
    }
}

impl Decoder for MetadataCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, streams) = Vec::<String>::decode(input)?;

        Ok((
            input,
            MetadataCommand {
                correlation_id,
                streams,
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
#[derive(Debug, Eq, PartialEq)]
pub struct MetadataResponse {
    pub(crate) correlation_id: u32,
    pub brokers: Vec<Broker>,
    pub stream_metadata: Vec<StreamMetadata>,
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq)]
pub struct Broker {
    pub reference: u16,
    pub host: String,
    pub port: u32,
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq)]
pub struct StreamMetadata {
    pub stream_name: String,
    pub code: ResponseCode,
    pub leader_reference: u16,
    pub replicas_references: Vec<u16>,
}

impl Decoder for Broker {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, reference) = u16::decode(input)?;
        let (input, host) = Option::decode(input)?;
        let (input, port) = u32::decode(input)?;
        Ok((
            input,
            Broker {
                reference,
                host: host.unwrap(),
                port,
            },
        ))
    }
}
impl Decoder for StreamMetadata {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, stream_name) = Option::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, leader_reference) = u16::decode(input)?;
        let (input, replicas_references) = Vec::decode(input)?;

        Ok((
            input,
            StreamMetadata {
                stream_name: stream_name.unwrap(),
                code,
                leader_reference,
                replicas_references,
            },
        ))
    }
}

impl Decoder for MetadataResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, brokers) = read_vec(input)?;
        let (input, stream_metadata) = read_vec(input)?;
        Ok((
            input,
            MetadataResponse {
                correlation_id,
                brokers,
                stream_metadata,
            },
        ))
    }
}

impl Encoder for Broker {
    fn encoded_size(&self) -> u32 {
        self.reference.encoded_size() + self.port.encoded_size() + self.host.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.reference.encode(writer)?;
        self.host.as_str().encode(writer)?;
        self.port.encode(writer)?;
        Ok(())
    }
}

impl Encoder for StreamMetadata {
    fn encoded_size(&self) -> u32 {
        self.stream_name.as_str().encoded_size()
            + self.code.encoded_size()
            + self.leader_reference.encoded_size()
            + self.replicas_references.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.stream_name.as_str().encode(writer)?;
        self.code.encode(writer)?;
        self.leader_reference.encode(writer)?;
        self.replicas_references.encode(writer)?;
        Ok(())
    }
}

impl Encoder for Vec<Broker> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for Vec<StreamMetadata> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for MetadataResponse {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        self.correlation_id.encode(writer)?;
        self.brokers.encode(writer)?;
        self.stream_metadata.encode(writer)?;
        Ok(())
    }
}

impl FromResponse for MetadataResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::Metadata(metadata) => Some(metadata),
            _ => None,
        }
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
