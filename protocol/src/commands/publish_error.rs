use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PUBLISH_ERROR,
};

use crate::types::PublishingError;

use std::io::Write;

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct PublishErrorResponse {
    publisher_id: u8,
    publishing_error: Vec<PublishingError>,
    publishing_id: u64,
    code: u16,
}

impl PublishErrorResponse {
    pub fn new(
        publisher_id: u8,
        publishing_error: Vec<PublishingError>,
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

impl Encoder for PublishErrorResponse {
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

impl Command for PublishErrorResponse {
    fn key(&self) -> u16 {
        COMMAND_PUBLISH_ERROR
    }
}

impl Decoder for PublishErrorResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, publisher_id) = u8::decode(input)?;
        let (input, publishing_error) = Vec::<PublishingError>::decode(input)?;
        let (input, publishing_id) = u64::decode(input)?;
        let (input, code) = u16::decode(input)?;

        Ok((
            input,
            PublishErrorResponse {
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
    use super::PublishErrorResponse;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn publish_error_test() {
        command_encode_decode_test::<PublishErrorResponse>();
    }
}
