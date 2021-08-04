use std::collections::HashMap;
use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_SUBSCRIBE,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct SubscribeCommand {
    correlation_id: u32,
    subscription_id: u32,
    stream_name: String,
    offset_specification: u16,
    offset: u64,
    credit: u16,
    properties: HashMap<String, String>,
}

impl SubscribeCommand {
    pub fn new(
        correlation_id: u32,
        subscription_id: u32,
        stream_name: String,
        offset_specification: u16,
        offset: u64,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            correlation_id,
            subscription_id,
            stream_name,
            offset_specification,
            offset,
            credit,
            properties,
        }
    }
}

impl Encoder for SubscribeCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.subscription_id.encoded_size()
            + self.stream_name.as_str().encoded_size()
            + self.offset_specification.encoded_size()
            + self.offset.encoded_size()
            + self.credit.encoded_size()
            + self.properties.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        self.stream_name.as_str().encode(writer)?;
        self.offset_specification.encode(writer)?;
        self.offset.encode(writer)?;
        self.credit.encode(writer)?;
        self.properties.encode(writer)?;
        Ok(())
    }
}

impl Decoder for SubscribeCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u32::decode(input)?;
        let (input, stream_name) = Option::decode(input)?;
        let (input, offset_specification) = u16::decode(input)?;
        let (input, offset) = u64::decode(input)?;
        let (input, credit) = u16::decode(input)?;
        let (input, properties) = HashMap::decode(input)?;

        Ok((
            input,
            SubscribeCommand {
                correlation_id,
                subscription_id,
                stream_name: stream_name.unwrap(),
                offset_specification,
                offset,
                credit,
                properties,
            },
        ))
    }
}

impl Command for SubscribeCommand {
    fn key(&self) -> u16 {
        COMMAND_SUBSCRIBE
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::SubscribeCommand;

    #[test]
    fn subscribe_request_test() {
        command_encode_decode_test::<SubscribeCommand>();
    }
}
