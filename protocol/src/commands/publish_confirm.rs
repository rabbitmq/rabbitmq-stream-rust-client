use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PUBLISH_CONFIRM,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct PublishConfirm {
    pub publisher_id: u8,
    pub publishing_ids: Vec<u64>,
}

impl PublishConfirm {
    pub fn new(publisher_id: u8, publishing_ids: Vec<u64>) -> Self {
        Self {
            publisher_id,
            publishing_ids,
        }
    }
}

impl Encoder for PublishConfirm {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publisher_id.encode(writer)?;
        self.publishing_ids.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.publisher_id.encoded_size() + self.publishing_ids.encoded_size()
    }
}

impl Command for PublishConfirm {
    fn key(&self) -> u16 {
        COMMAND_PUBLISH_CONFIRM
    }
}
impl Decoder for PublishConfirm {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, publisher_id) = u8::decode(input)?;
        let (input, publishing_ids) = Vec::decode(input)?;

        Ok((
            input,
            PublishConfirm {
                publisher_id,
                publishing_ids,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::PublishConfirm;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn publish_confirm_test() {
        command_encode_decode_test::<PublishConfirm>()
    }
}
