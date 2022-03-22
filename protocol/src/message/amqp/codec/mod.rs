use std::{any::type_name, io::Write};

use self::constants::TypeCode;

use super::error::{AmqpDecodeError, AmqpEncodeError};

pub mod constants;
mod decoder;

pub trait AmqpEncoder {
    fn encoded_size(&self) -> u32;
    fn encode(&self, writer: &mut impl Write) -> Result<(), AmqpEncodeError>;
}

pub trait AmqpDecoder
where
    Self: Sized,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError>;

    fn invalid_type_code(code: TypeCode) -> AmqpDecodeError {
        AmqpDecodeError::InvalidTypeCodeFor {
            target: type_name::<Self>().to_string(),
            code,
        }
    }
}
