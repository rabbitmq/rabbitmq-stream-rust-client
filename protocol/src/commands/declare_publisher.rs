use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DECLARE_PUBLISHER,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct DeclarePublisherCommand {
    correlation_id: u32,
    publisher_id: u8,
    publisher_reference: Option<String>,
    stream_name: String,
}

impl DeclarePublisherCommand {
    pub fn new(
        correlation_id: u32,
        publisher_id: u8,
        publisher_reference: Option<String>,
        stream_name: String,
    ) -> Self {
        Self {
            correlation_id,
            publisher_id,
            publisher_reference,
            stream_name,
        }
    }
}

impl Encoder for DeclarePublisherCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.publisher_id.encoded_size()
            + self.publisher_reference.encoded_size()
            + self.stream_name.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.publisher_id.encode(writer)?;
        self.publisher_reference.encode(writer)?;
        self.stream_name.as_str().encode(writer)?;
        Ok(())
    }
}

impl Command for DeclarePublisherCommand {
    fn key(&self) -> u16 {
        COMMAND_DECLARE_PUBLISHER
    }
}

impl Decoder for DeclarePublisherCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, publisher_id) = u8::decode(input)?;
        let (input, publisher_reference) = Option::decode(input)?;
        let (input, stream_name) = Option::decode(input)?;

        Ok((
            input,
            DeclarePublisherCommand {
                correlation_id,
                publisher_id,
                publisher_reference,
                stream_name: stream_name.unwrap(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::DeclarePublisherCommand;

    #[test]
    fn create_stream_request_test() {
        command_encode_decode_test::<DeclarePublisherCommand>();
    }
}
