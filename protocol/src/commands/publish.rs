use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PUBLISH,
};

use super::Command;

use crate::types::PublishedMessage;
#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct PublishCommand {
    publisher_id: u8,
    published_messages: Vec<PublishedMessage>,
}

impl PublishCommand {
    pub fn new(publisher_id: u8, published_messages: Vec<PublishedMessage>) -> Self {
        Self {
            publisher_id,
            published_messages,
        }
    }
}

impl Encoder for PublishCommand {
    fn encoded_size(&self) -> u32 {
        self.publisher_id.encoded_size() + self.published_messages.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publisher_id.encode(writer)?;
        self.published_messages.encode(writer)?;
        Ok(())
    }
}

impl Command for PublishCommand {
    fn key(&self) -> u16 {
        COMMAND_PUBLISH
    }
}

impl Decoder for PublishCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, publisher_id) = u8::decode(input)?;
        let (input, published_messages) = Vec::decode(input)?;

        Ok((
            input,
            PublishCommand {
                publisher_id,
                published_messages,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::PublishCommand;

    #[test]
    fn publish_request_test() {
        command_encode_decode_test::<PublishCommand>();
    }
}
