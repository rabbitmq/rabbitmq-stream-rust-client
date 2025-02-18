use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PUBLISH_ERROR,
};

use crate::types::PublishingError;

use std::io::Write;

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct PublishErrorResponse {
    pub publisher_id: u8,
    pub publishing_errors: Vec<PublishingError>,
}

impl PublishErrorResponse {
    pub fn new(publisher_id: u8, publishing_errors: Vec<PublishingError>) -> Self {
        Self {
            publisher_id,
            publishing_errors,
        }
    }
}

impl Encoder for PublishErrorResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publisher_id.encode(writer)?;
        self.publishing_errors.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.publisher_id.encoded_size()
            + self.publishing_errors.encoded_size()
            + self.publisher_id.encoded_size()
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
        let (input, publishing_errors) = Vec::<PublishingError>::decode(input)?;

        Ok((
            input,
            PublishErrorResponse {
                publisher_id,
                publishing_errors,
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
