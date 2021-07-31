use std::collections::HashMap;

use super::Decoder;
use crate::{error::DecodeError, types::Header};
use byteorder::ByteOrder;

impl Decoder for u8 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_u8(input)
    }
}

impl Decoder for u16 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_u16(input)
    }
}

impl Decoder for u32 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_u32(input)
    }
}

impl Decoder for u64 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_u64(input)
    }
}

impl Decoder for Vec<u8> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, len) = read_i32(input)?;
        let len = len as usize;
        Ok((&input[len..], input[..len].to_vec()))
    }
}

impl Decoder for Header {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, key) = read_u16(input)?;
        let (input, version) = read_u16(input)?;

        Ok((input, Header::new(key, version)))
    }
}

impl Decoder for Option<String> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, len) = read_i16(input)?;

        if len == -1 {
            return Ok((input, None));
        }
        let (bytes, input) = input.split_at(len as usize);
        let string = String::from_utf8(bytes.to_vec())?;
        Ok((input, Some(string)))
    }
}

impl Decoder for HashMap<String, String> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (mut input, num_properties) = read_u32(input)?;

        let mut map = HashMap::with_capacity(num_properties as usize);
        for _ in 0..num_properties {
            let (input1, key) = Option::<String>::decode(input)?;
            let (input2, value) = Option::<String>::decode(input1)?;

            if let (Some(k), Some(v)) = (key, value) {
                map.insert(k, v);
            }
            input = input2;
        }

        Ok((input, map))
    }
}

impl Decoder for Vec<String> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (mut input, num_properties) = read_u32(input)?;
        let mut vec: Vec<String> = Vec::new();
        for _ in 0..num_properties {
            let (input1, value) = Option::<String>::decode(input)?;

            if let Some(v) = value {
                vec.push(v)
            }
            input = input1;
        }

        Ok((input, vec))
    }
}

pub fn check_len(input: &[u8], size: usize) -> Result<(), DecodeError> {
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

pub fn read_u8(input: &[u8]) -> Result<(&[u8], u8), DecodeError> {
    check_len(input, 1)?;
    Ok((&input[1..], input[0]))
}

reader!(read_i16, 2, i16);
reader!(read_u16, 2, u16);
reader!(read_u32, 4, u32);
reader!(read_i32, 4, i32);
reader!(read_u64, 8, u64);
