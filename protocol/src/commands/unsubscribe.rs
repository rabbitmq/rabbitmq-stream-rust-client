use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_UNSUBSCRIBE,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct UnSubscribeCommand {
    correlation_id: u32,
    subscription_id: u8,
}

impl UnSubscribeCommand {
    pub fn new(correlation_id: u32, subscription_id: u8) -> Self {
        Self {
            correlation_id,
            subscription_id,
        }
    }
}

impl Encoder for UnSubscribeCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.subscription_id.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        Ok(())
    }
}

impl Command for UnSubscribeCommand {
    fn key(&self) -> u16 {
        COMMAND_UNSUBSCRIBE
    }
}

impl Decoder for UnSubscribeCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;

        Ok((
            input,
            UnSubscribeCommand {
                correlation_id,
                subscription_id,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::UnSubscribeCommand;

    #[test]
    fn unsubscribe_request_test() {
        command_encode_decode_test::<UnSubscribeCommand>();
    }
}
