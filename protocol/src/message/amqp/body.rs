use crate::message::amqp::codec::AmqpEncoder;

use super::{
    codec::constants::{
        MESSAGE_BODY_DATA, MESSAGE_BODY_SEQUENCE, MESSAGE_BODY_VALUE, SECTION_PREFIX_LENGTH,
    },
    types::{List, Value},
    AmqpEncodeError,
};

#[cfg(test)]
use fake::Fake;

#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct MessageBody {
    pub data: Vec<Vec<u8>>,
    pub sequence: Vec<List>,
    pub value: Option<Value>,
}

impl MessageBody {
    pub fn data(&self) -> Option<&Vec<u8>> {
        self.data.get(0)
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    pub fn set_value(&mut self, value: impl Into<Value>) -> &mut Self {
        self.value = Some(value.into());
        self
    }

    pub fn set_data(&mut self, data: impl Into<Vec<u8>>) -> &mut Self {
        self.data.clear();
        self.data.push(data.into());
        self
    }
}

impl AmqpEncoder for MessageBody {
    fn encoded_size(&self) -> u32 {
        let mut size = self
            .data
            .iter()
            .fold(0, |a, d| a + d.encoded_size() + SECTION_PREFIX_LENGTH);
        size += self
            .sequence
            .iter()
            .fold(0, |a, seq| a + seq.encoded_size() + SECTION_PREFIX_LENGTH);

        if let Some(ref val) = self.value {
            size + val.encoded_size() + SECTION_PREFIX_LENGTH
        } else {
            size
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        for data in &self.data {
            MESSAGE_BODY_DATA.encode(writer)?;
            data.encode(writer)?;
        }

        for sequence in &self.sequence {
            MESSAGE_BODY_SEQUENCE.encode(writer)?;
            sequence.encode(writer)?;
        }
        if let Some(ref val) = self.value {
            MESSAGE_BODY_VALUE.encode(writer)?;
            val.encode(writer)?;
        }
        Ok(())
    }
}
