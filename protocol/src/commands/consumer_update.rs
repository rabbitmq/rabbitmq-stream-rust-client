use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CONSUMER_UPDATE,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct ConsumerUpdateCommand {
    pub(crate) correlation_id: u32,
    subscription_id: u8,
    active: u8,
}

impl ConsumerUpdateCommand {
    pub fn new(correlation_id: u32, subscription_id: u8, active: u8) -> Self {
        Self {
            correlation_id,
            subscription_id,
            active,
        }
    }
}

impl Encoder for ConsumerUpdateCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.subscription_id.encoded_size()
            + self.active.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        self.active.encode(writer)?;
        Ok(())
    }
}

impl Decoder for ConsumerUpdateCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;
        let (input, active) = u8::decode(input)?;

        Ok((
            input,
            ConsumerUpdateCommand {
                correlation_id,
                subscription_id,
                active,
            },
        ))
    }
}

impl Command for ConsumerUpdateCommand {
    fn key(&self) -> u16 {
        COMMAND_CONSUMER_UPDATE
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::ConsumerUpdateCommand;

    #[test]
    fn subscribe_request_test() {
        command_encode_decode_test::<ConsumerUpdateCommand>();
    }
}
