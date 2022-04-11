use std::{convert::TryFrom, hash::Hash};

use super::{
    Binary, Boolean, Byte, Char, Double, Float, Int, List, Long, Map, Short, SimpleValue, Str,
    Timestamp, UByte, UInt, ULong, UShort, Uuid,
};
use crate::{
    error::DecodeError,
    message::amqp::{
        codec::constants::TypeCode, types::Descriptor, AmqpDecodeError, AmqpDecoder, AmqpEncoder,
        Symbol,
    },
    utils::TupleMapperSecond,
};
use derive_more::From;

#[cfg(test)]
use fake::Fake;

/// AMQP 1.0 data types
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum Value {
    Simple(SimpleValue),
    Collection(CollectionValue),
    Described(DescribedValue),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct DescribedValue {
    descriptor: Descriptor,
    value: Box<Value>,
}

impl AmqpEncoder for DescribedValue {
    fn encoded_size(&self) -> u32 {
        1 + self.descriptor.encoded_size() + self.value.encoded_size()
    }

    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::message::amqp::AmqpEncodeError> {
        TypeCode::Described.encode(writer)?;
        self.descriptor.encode(writer)?;
        self.value.encode(writer)?;
        Ok(())
    }
}

impl AmqpDecoder for DescribedValue {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Described => {
                let (input, descriptor) = Descriptor::decode(input)?;
                let (input, value) = Value::decode(input)?;
                Ok((
                    input,
                    DescribedValue {
                        descriptor,
                        value: Box::new(value),
                    },
                ))
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, From)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum CollectionValue {
    List(List),
    Map(Map<Value, Value>),
}

impl AmqpDecoder for CollectionValue {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::message::amqp::AmqpDecodeError> {
        let (_, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::List0 | TypeCode::List8 | TypeCode::List32 => {
                List::decode(input).map_second(CollectionValue::List)
            }
            TypeCode::Map8 | TypeCode::Map32 => Map::decode(input).map_second(CollectionValue::Map),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

impl AmqpEncoder for CollectionValue {
    fn encoded_size(&self) -> u32 {
        match self {
            CollectionValue::List(list) => list.encoded_size(),
            CollectionValue::Map(map) => map.encoded_size(),
        }
    }

    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::message::amqp::AmqpEncodeError> {
        match self {
            CollectionValue::List(list) => list.encode(writer),
            CollectionValue::Map(map) => map.encode(writer),
        }
    }
}

impl Hash for Map<Value, Value> {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        unimplemented!()
    }
}

impl AmqpDecoder for Value {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::message::amqp::AmqpDecodeError> {
        let (_, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Described => DescribedValue::decode(input).map_second(Value::Described),
            TypeCode::List0
            | TypeCode::List8
            | TypeCode::List32
            | TypeCode::Map8
            | TypeCode::Map32
            | TypeCode::Array8
            | TypeCode::Array32 => CollectionValue::decode(input).map_second(Value::Collection),

            _ => SimpleValue::decode(input).map_second(Value::Simple),
        }
    }
}

impl AmqpEncoder for Value {
    fn encoded_size(&self) -> u32 {
        match self {
            Value::Simple(simple) => simple.encoded_size(),
            Value::Collection(collection) => collection.encoded_size(),
            Value::Described(described) => described.encoded_size(),
        }
    }

    fn encode(
        &self,
        writer: &mut impl std::io::Write,
    ) -> Result<(), crate::message::amqp::AmqpEncodeError> {
        match self {
            Value::Simple(simple) => simple.encode(writer),
            Value::Collection(collection) => collection.encode(writer),
            Value::Described(described) => described.encode(writer),
        }
    }
}

macro_rules! impl_simple_value {
    ($type:ty) => {
        impl From<$type> for Value {
            fn from(value: $type) -> Value {
                Value::Simple(value.into())
            }
        }
    };
}
macro_rules! impl_collection_value {
    ($type:ty) => {
        impl From<$type> for Value {
            fn from(value: $type) -> Value {
                Value::Collection(value.into())
            }
        }
    };
}

macro_rules! impl_try_from_simple_value_ref {
    ($type:ty) => {
        impl<'a> TryFrom<&'a Value> for $type {
            type Error = DecodeError;

            fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                match value {
                    Value::Simple(simple) => simple
                        .try_into()
                        .map_err(|err: &str| DecodeError::MessageParse(err.to_string())),
                    _ => Err(DecodeError::MessageParse(
                        "Failed to cast Value to simple type".to_string(),
                    )),
                }
            }
        }
    };
}

impl_try_from_simple_value_ref!(&'a Boolean);
impl_try_from_simple_value_ref!(&'a UByte);
impl_try_from_simple_value_ref!(&'a UShort);
impl_try_from_simple_value_ref!(&'a UInt);
impl_try_from_simple_value_ref!(&'a ULong);
impl_try_from_simple_value_ref!(&'a Byte);
impl_try_from_simple_value_ref!(&'a Short);
impl_try_from_simple_value_ref!(&'a Int);
impl_try_from_simple_value_ref!(&'a Long);
impl_try_from_simple_value_ref!(&'a Float);
impl_try_from_simple_value_ref!(&'a Double);
impl_try_from_simple_value_ref!(&'a Char);
impl_try_from_simple_value_ref!(&'a Timestamp);
impl_try_from_simple_value_ref!(&'a Uuid);
impl_try_from_simple_value_ref!(&'a Binary);
impl_try_from_simple_value_ref!(&'a Str);
impl_try_from_simple_value_ref!(&'a Symbol);

impl_simple_value!(Boolean);
impl_simple_value!(UByte);
impl_simple_value!(UShort);
impl_simple_value!(UInt);
impl_simple_value!(ULong);
impl_simple_value!(Byte);
impl_simple_value!(Short);
impl_simple_value!(Int);
impl_simple_value!(Long);
impl_simple_value!(Float);
impl_simple_value!(Double);
impl_simple_value!(Char);
impl_simple_value!(Timestamp);
impl_simple_value!(Uuid);
impl_simple_value!(Binary);
impl_simple_value!(Str);
impl_simple_value!(Symbol);
impl_simple_value!(&str);

impl_collection_value!(List);
