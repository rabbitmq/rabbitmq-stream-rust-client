use std::io::Write;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use crate::error::{DecodeError, EncodeError};

pub trait Encoder {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError>;

    fn encode_str(&self, writer: &mut impl Write, input: &str) -> Result<(), EncodeError> {
        writer.write_i16::<BigEndian>(input.len() as i16)?;
        writer.write_all(input.as_bytes())?;
        Ok(())
    }
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
        let string = String::from_utf8(bytes.to_vec()).unwrap();
        Ok((input, Some(string)))
    }
}

macro_rules! check_len {
    ($buf:ident, $size:expr) => {
        if $buf.len() < $size {
            return Err(crate::error::DecodeError::Incomplete($size));
        }
    };
}

macro_rules! reader {
    ( $fn:ident, $size:expr, $ret:ty) => {
        pub fn $fn(input: &[u8]) -> Result<(&[u8], $ret), crate::error::DecodeError> {
            check_len!(input, $size);
            let x = byteorder::BigEndian::$fn(input);
            Ok((&input[$size..], x))
        }
    };
}

reader!(read_u32, 4, u32);
reader!(read_i32, 4, i32);
reader!(read_i16, 2, i16);
