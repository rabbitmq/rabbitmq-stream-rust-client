use crate::{
    message::amqp::{codec::constants::TypeCode, AmqpDecoder, AmqpEncoder},
    utils::TupleMapperSecond,
};

use super::{Long, Map, Symbol, ULong, Value};
#[cfg(test)]
use fake::Fake;

/// Key for annotations [`Map`]
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum AnnonationKey {
    Symbol(Symbol),
    ULong(ULong),
    Long(Long),
}

pub type Annotations = Map<AnnonationKey, Value>;

impl Annotations {
    pub fn put<K, V>(&mut self, key: K, value: V) -> Option<Value>
    where
        K: Into<AnnonationKey>,
        V: Into<Value>,
    {
        self.0.insert(key.into(), value.into())
    }

    pub fn get<K>(&self, key: K) -> Option<&Value>
    where
        K: Into<AnnonationKey>,
    {
        self.0.get(&key.into())
    }
}

impl From<&str> for AnnonationKey {
    fn from(string: &str) -> Self {
        AnnonationKey::Symbol(Symbol::from(string))
    }
}

impl From<String> for AnnonationKey {
    fn from(string: String) -> Self {
        AnnonationKey::Symbol(Symbol::from(string))
    }
}
impl From<u64> for AnnonationKey {
    fn from(number: u64) -> Self {
        AnnonationKey::ULong(number)
    }
}

impl AmqpDecoder for AnnonationKey {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::message::amqp::AmqpDecodeError> {
        let (_, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Symbol8 | TypeCode::Symbol32 => {
                Symbol::decode(input).map_second(AnnonationKey::Symbol)
            }
            TypeCode::ULong | TypeCode::ULong0 | TypeCode::ULongSmall => {
                ULong::decode(input).map_second(AnnonationKey::ULong)
            }
            TypeCode::Long | TypeCode::LongSmall => {
                Long::decode(input).map_second(AnnonationKey::Long)
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for AnnonationKey {
    fn encoded_size(&self) -> u32 {
        match self {
            AnnonationKey::Symbol(symbol) => symbol.encoded_size(),
            AnnonationKey::ULong(long) => long.encoded_size(),
            AnnonationKey::Long(long) => long.encoded_size(),
        }
    }

    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::message::amqp::AmqpEncodeError> {
        match self {
            AnnonationKey::Symbol(symbol) => symbol.encode(writer),
            AnnonationKey::ULong(long) => long.encode(writer),
            AnnonationKey::Long(long) => long.encode(writer),
        }
    }
}
