use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CONSUMER_UPDATE_REQUEST,
};

use crate::commands::subscribe::OffsetSpecification;

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct ConsumerUpdateRequestCommand {
    pub(crate) correlation_id: u32,
    response_code: u16,
    offset_specification: OffsetSpecification,
}

impl ConsumerUpdateRequestCommand {
    pub fn new(
        correlation_id: u32,
        response_code: u16,
        offset_specification: OffsetSpecification,
    ) -> Self {
        Self {
            correlation_id,
            response_code,
            offset_specification,
        }
    }
}

impl Encoder for ConsumerUpdateRequestCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
            + self.offset_specification.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.offset_specification.encode(writer)?;
        Ok(())
    }
}

impl Decoder for ConsumerUpdateRequestCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = u16::decode(input)?;
        let (input, offset_specification) = OffsetSpecification::decode(input)?;

        Ok((
            input,
            ConsumerUpdateRequestCommand {
                correlation_id,
                response_code,
                offset_specification,
            },
        ))
    }
}

impl Command for ConsumerUpdateRequestCommand {
    fn key(&self) -> u16 {
        COMMAND_CONSUMER_UPDATE_REQUEST
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::ConsumerUpdateRequestCommand;

    #[test]
    fn subscribe_request_test() {
        command_encode_decode_test::<ConsumerUpdateRequestCommand>();
    }
}
