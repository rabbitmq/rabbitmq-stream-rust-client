use crate::message::amqp::codec::AmqpEncoder;

use super::{
    codec::constants::{MESSAGE_BODY_DATA, MESSAGE_BODY_SEQUENCE, MESSAGE_BODY_VALUE},
    types::{List, Value},
    AmqpEncodeError,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageBody {
    pub data: Vec<Vec<u8>>,
    pub sequence: Vec<List>,
    pub value: Option<Value>,
}

impl MessageBody {
    pub fn data(&self) -> Option<&Vec<u8>> {
        self.data.first()
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    #[cfg(test)]
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
        let mut size = self.data.iter().fold(0, |a, d| {
            a + d.encoded_size() + MESSAGE_BODY_DATA.encoded_size()
        });
        size += self.sequence.iter().fold(0, |a, seq| {
            a + seq.encoded_size() + MESSAGE_BODY_SEQUENCE.encoded_size()
        });

        if let Some(ref val) = self.value {
            size + val.encoded_size() + MESSAGE_BODY_VALUE.encoded_size()
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

#[cfg(test)]
mod tests {

    use fake::{Dummy, Fake, Faker};

    use crate::message::amqp::Value;

    use super::MessageBody;
    impl Dummy<Faker> for MessageBody {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            MessageBody {
                data: config.fake_with_rng(rng),
                sequence: config.fake_with_rng(rng),
                value: Some(Value::Simple(config.fake_with_rng(rng))),
            }
        }
    }
}
