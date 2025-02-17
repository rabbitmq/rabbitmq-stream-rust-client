use crate::utils::TupleMapperSecond;
pub use definitions::*;
use derive_more::{From, TryInto};
pub use descriptor::Descriptor;
pub use primitives::*;

use super::{codec::constants::TypeCode, AmqpDecoder, AmqpEncodeError, AmqpEncoder};

mod annotations;
mod definitions;
mod descriptor;
mod primitives;
mod symbol;

pub use annotations::{AnnonationKey, Annotations};
pub use primitives::{SimpleValue, Value};
pub use symbol::Symbol;

pub use primitives::Str;

pub type Milliseconds = u32;

#[derive(Debug, Eq, PartialEq, Clone, From, TryInto)]
#[try_into(owned, ref, ref_mut)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum MessageId {
    ULong(ULong),
    Long(Long),
    Uuid(Uuid),
    Binary(Binary),
    String(Str),
}

impl AmqpEncoder for MessageId {
    fn encoded_size(&self) -> u32 {
        match self {
            MessageId::ULong(id) => id.encoded_size(),
            MessageId::Uuid(id) => id.encoded_size(),
            MessageId::Binary(id) => id.encoded_size(),
            MessageId::String(id) => id.encoded_size(),
            MessageId::Long(id) => id.encoded_size(),
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        match self {
            MessageId::ULong(id) => id.encode(writer),
            MessageId::Uuid(id) => id.encode(writer),
            MessageId::Binary(id) => id.encode(writer),
            MessageId::String(id) => id.encode(writer),
            MessageId::Long(id) => id.encode(writer),
        }
    }
}

impl AmqpDecoder for MessageId {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), super::AmqpDecodeError> {
        let (_, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::ULong0 | TypeCode::ULong | TypeCode::ULongSmall => {
                ULong::decode(input).map_second(MessageId::ULong)
            }
            TypeCode::Long | TypeCode::LongSmall => Long::decode(input).map_second(MessageId::Long),
            TypeCode::Uuid => Uuid::decode(input).map_second(MessageId::Uuid),
            TypeCode::Binary8 | TypeCode::Binary32 => {
                Binary::decode(input).map_second(MessageId::Binary)
            }
            TypeCode::String8 | TypeCode::String32 => {
                Str::decode(input).map_second(MessageId::String)
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
