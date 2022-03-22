use std::ops::Deref;

use crate::{
    codec::decoder::{read_exact, read_u32, read_u8},
    message::amqp::{
        codec::{constants::TypeCode, AmqpDecoder, AmqpEncoder},
        error::{AmqpDecodeError, AmqpEncodeError},
    },
    utils::TupleMapperSecond,
};

use byteorder::{BigEndian, WriteBytesExt};
#[cfg(test)]
use fake::Fake;

use super::Str;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct Symbol(Str);

impl Deref for Symbol {
    type Target = Str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AmqpEncoder for Symbol {
    fn encoded_size(&self) -> u32 {
        let length = self.0.len() as u32;
        let size = if length > u8::MAX as u32 { 5 } else { 2 };
        size + length
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let length = self.0.len();
        if length > u8::MAX as usize {
            TypeCode::Symbol32.encode(writer)?;
            writer.write_u32::<BigEndian>(length as u32)?;
        } else {
            TypeCode::Symbol8.encode(writer)?;
            writer.write_u8(length as u8)?;
        }

        writer.write_all(self.0.as_bytes())?;
        Ok(())
    }
}
impl AmqpDecoder for Symbol {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Symbol8 => {
                let (input, len) = read_u8(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
                    .and_then(|(input, bytes)| Ok((input, String::from_utf8(bytes)?.into())))
            }
            TypeCode::Symbol32 => {
                let (input, len) = read_u32(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
                    .and_then(|(input, bytes)| Ok((input, String::from_utf8(bytes)?.into())))
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

impl From<Str> for Symbol {
    fn from(string: Str) -> Self {
        Symbol(string)
    }
}

impl From<&str> for Symbol {
    fn from(string: &str) -> Self {
        Symbol(string.to_string())
    }
}
