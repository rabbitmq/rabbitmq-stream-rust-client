use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PUBLISH_ERROR,
};
use std::io::Write;

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct PublishError {
    publisher_id: u8,
    publishing_error: Vec<(u64, u16)>,
    publishing_id: u64,
    code: u16,
}

impl PublishError {
    pub fn new(
        publisher_id: u8,
        publishing_error: Vec<(u64, u16)>,
        publishing_id: u64,
        code: u16,
    ) -> Self {
        Self {
            publisher_id,
            publishing_error,
            publishing_id,
            code,
        }
    }
}

impl Encoder for PublishError {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publisher_id.encode(writer)?;
        self.publishing_error.encode(writer)?;
        self.publishing_id.encode(writer)?;
        self.code.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.publisher_id.encoded_size()
            + self.publishing_error.encoded_size()
            + self.publisher_id.encoded_size()
            + self.code.encoded_size()
    }
}

impl Command for PublishError {
    fn key(&self) -> u16 {
        COMMAND_PUBLISH_ERROR
    }
}

impl Decoder for PublishError {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, publisher_id) = u8::decode(input)?;
        let (input, publishing_error) = Vec::<(u64, u16)>::decode(input)?;
        let (input, publishing_id) = u64::decode(input)?;
        let (input, code) = u16::decode(input)?;

        Ok((
            input,
            PublishError {
                publisher_id,
                publishing_error,
                publishing_id,
                code,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::PublishError;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn publish_error_test() {
        command_encode_decode_test::<PublishError>();
    }
}
