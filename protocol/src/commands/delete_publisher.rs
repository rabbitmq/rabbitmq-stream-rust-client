use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELETE_PUBLISHER,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct DeletePublisherCommand {
    correlation_id: u32,
    publisher_id: u8,
}

impl DeletePublisherCommand {
    pub fn new(correlation_id: u32, publisher_id: u8) -> Self {
        Self {
            correlation_id,
            publisher_id,
        }
    }
}

impl Encoder for DeletePublisherCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.publisher_id.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.publisher_id.encode(writer)?;
        Ok(())
    }
}

impl Command for DeletePublisherCommand {
    fn key(&self) -> u16 {
        COMMAND_DELETE_PUBLISHER
    }
}

impl Decoder for DeletePublisherCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, publisher_id) = u8::decode(input)?;

        Ok((
            input,
            DeletePublisherCommand {
                correlation_id,
                publisher_id,
            },
        ))
    }
}
#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::DeletePublisherCommand;

    #[test]
    fn create_stream_request_test() {
        command_encode_decode_test::<DeletePublisherCommand>();
    }
}
