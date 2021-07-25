use std::{collections::HashMap, io::Write};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use crate::{
    error::{DecodeError, EncodeError},
    types::{CorrelationId, Header},
};

pub trait Encoder {
    fn encoded_size(&self) -> u32;
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError>;
}

pub(crate) trait Decoder
where
    Self: Sized,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError>;

    fn decode_str(input: &[u8]) -> Result<(&[u8], Option<String>), DecodeError> {
        let (input, len) = read_i16(input)?;

        if len == -1 {
            return Ok((input, None));
        }
        let (bytes, input) = input.split_at(len as usize);
        let string = String::from_utf8(bytes.to_vec())?;
        Ok((input, Some(string)))
    }

    fn decode_map(input: &[u8]) -> Result<(&[u8], HashMap<String, String>), DecodeError> {
        let (mut input, num_properties) = read_u32(input)?;

        let mut map = HashMap::with_capacity(num_properties as usize);
        for _ in 0..num_properties {
            let (input1, key) = Self::decode_str(input)?;
            let (input2, value) = Self::decode_str(input1)?;

            if let (Some(k), Some(v)) = (key, value) {
                map.insert(k, v);
            }
            input = input2;
        }

        Ok((input, map))
    }
}

fn check_len(input: &[u8], size: usize) -> Result<(), DecodeError> {
    if input.len() < size {
        return Err(DecodeError::Incomplete(size));
    }
    Ok(())
}

macro_rules! reader {
    ( $fn:ident, $size:expr, $ret:ty) => {
        #[allow(unused)]
        pub fn $fn(input: &[u8]) -> Result<(&[u8], $ret), crate::error::DecodeError> {
            check_len(input, $size)?;
            let x = byteorder::BigEndian::$fn(input);
            Ok((&input[$size..], x))
        }
    };
}

reader!(read_u32, 4, u32);
reader!(read_i32, 4, i32);
reader!(read_i16, 2, i16);
reader!(read_u16, 2, u16);

impl Encoder for CorrelationId {
    fn encoded_size(&self) -> u32 {
        4
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(**self)?;
        Ok(())
    }
}

impl Decoder for CorrelationId {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        read_u32(input).map(|(input, correlation_id)| (input, correlation_id.into()))
    }
}

impl Encoder for &str {
    fn encoded_size(&self) -> u32 {
        2 + self.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i16::<BigEndian>(self.len() as i16)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl Encoder for HashMap<String, String> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, (k, v)| {
            acc + k.as_str().encoded_size() + v.as_str().encoded_size()
        })
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.len() as u32)?;

        for (k, v) in self {
            k.as_str().encode(writer)?;
            v.as_str().encode(writer)?;
        }
        Ok(())
    }
}

impl Decoder for Header {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, key) = read_u16(input)?;
        let (input, version) = read_u16(input)?;

        Ok((input, Header::new(key, version)))
    }
}

impl Encoder for Header {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u16::<BigEndian>(self.key())?;
        writer.write_u16::<BigEndian>(self.version())?;

        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        2 + 2
    }
}
