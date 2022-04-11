use super::primitives::ULong;
use super::symbol::Symbol;
use crate::message::amqp::codec::constants::TypeCode;
use crate::message::amqp::codec::{AmqpDecoder, AmqpEncoder};
use crate::message::amqp::error::{AmqpDecodeError, AmqpEncodeError};
use crate::utils::TupleMapperSecond;

#[cfg(test)]
use fake::Fake;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum Descriptor {
    Ulong(u64),
    Symbol(Symbol),
}

impl AmqpEncoder for Descriptor {
    fn encoded_size(&self) -> u32 {
        match *self {
            Descriptor::Ulong(v) => 1 + v.encoded_size(),
            Descriptor::Symbol(ref v) => 1 + v.encoded_size(),
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Described.encode(writer)?;
        match *self {
            Descriptor::Ulong(v) => v.encode(writer),
            Descriptor::Symbol(ref v) => v.encode(writer),
        }
    }
}

impl AmqpDecoder for Descriptor {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::Described) => {
                let (remaining, code) = TypeCode::decode(input)?;
                match code {
                    TypeCode::ULong => ULong::decode(input).map_second(Descriptor::Ulong),
                    TypeCode::ULong0 => Ok((remaining, Descriptor::Ulong(0))),
                    TypeCode::ULongSmall => ULong::decode(input).map_second(Descriptor::Ulong),
                    TypeCode::Symbol8 | TypeCode::Symbol32 => {
                        Symbol::decode(input).map_second(Descriptor::Symbol)
                    }
                    _ => Err(Self::invalid_type_code(code)),
                }
            }
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}
