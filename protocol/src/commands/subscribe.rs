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
#[derive(PartialEq, Eq, Debug)]
pub struct SubscribeCommand {
    correlation_id: u32,
    subscription_id: u8,
    stream_name: String,
    offset_specification: OffsetSpecification,
    credit: u16,
    properties: HashMap<String, String>,
}

impl SubscribeCommand {
    pub fn new(
        correlation_id: u32,
        subscription_id: u8,
        stream_name: String,
        offset_specification: OffsetSpecification,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            correlation_id,
            subscription_id,
            stream_name,
            offset_specification,
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
            + self.credit.encoded_size()
            + self.properties.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        self.stream_name.as_str().encode(writer)?;
        self.offset_specification.encode(writer)?;
        self.credit.encode(writer)?;
        self.properties.encode(writer)?;
        Ok(())
    }
}

impl Decoder for SubscribeCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;
        let (input, stream_name) = Option::decode(input)?;
        let (input, offset_specification) = OffsetSpecification::decode(input)?;
        let (input, credit) = u16::decode(input)?;
        let (input, properties) = HashMap::decode(input)?;

        Ok((
            input,
            SubscribeCommand {
                correlation_id,
                subscription_id,
                stream_name: stream_name.unwrap(),
                offset_specification,
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

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub enum OffsetSpecification {
    First,
    Last,
    Next,
    Offset(u64),
    Timestamp(i64),
}

impl OffsetSpecification {
    fn get_type(&self) -> u16 {
        match self {
            OffsetSpecification::First => 1,
            OffsetSpecification::Last => 2,
            OffsetSpecification::Next => 3,
            OffsetSpecification::Offset(_) => 4,
            OffsetSpecification::Timestamp(_) => 5,
        }
    }
}

impl Encoder for OffsetSpecification {
    fn encoded_size(&self) -> u32 {
        self.get_type().encoded_size()
            + match self {
                OffsetSpecification::Offset(offset) => offset.encoded_size(),
                OffsetSpecification::Timestamp(timestamp) => timestamp.encoded_size(),
                _ => 0,
            }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.get_type().encode(writer)?;
        match self {
            OffsetSpecification::Offset(offset) => offset.encode(writer),
            OffsetSpecification::Timestamp(timestamp) => timestamp.encode(writer),
            _ => Ok(()),
        }
    }
}

impl Decoder for OffsetSpecification {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, offset_type) = u16::decode(input)?;

        match offset_type {
            1 => Ok((input, OffsetSpecification::First)),
            2 => Ok((input, OffsetSpecification::Last)),
            3 => Ok((input, OffsetSpecification::Next)),
            4 => {
                let (input, offset) = u64::decode(input)?;
                Ok((input, OffsetSpecification::Offset(offset)))
            }
            5 => {
                let (input, timestamp) = i64::decode(input)?;
                Ok((input, OffsetSpecification::Timestamp(timestamp)))
            }
            _ => panic!("Offset type not supported"),
        }
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
