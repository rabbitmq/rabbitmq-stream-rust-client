use std::collections::HashMap;

use byteorder::ByteOrder;

use crate::message::Message;
use crate::types::PublishedMessage;
use crate::types::PublishingError;
use crate::ResponseCode;
use crate::{error::DecodeError, types::Header};

use super::Decoder;

impl Decoder for i8 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_i8(input)
    }
}

impl Decoder for i32 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_i32(input)
    }
}

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
impl Decoder for i64 {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        read_i64(input)
    }
}

impl Decoder for Vec<u8> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, len) = read_i32(input)?;
        let len = len as usize;
        Ok((&input[len..], input[..len].to_vec()))
    }
}

pub fn read_vec<T: Decoder>(input: &[u8]) -> Result<(&[u8], Vec<T>), DecodeError> {
    let (mut input, len) = read_i32(input)?;
    let len = len as usize;
    let mut result: Vec<T> = Vec::with_capacity(len);
    for _ in 0..len {
        let (input1, value) = T::decode(input)?;
        result.push(value);
        input = input1
    }
    Ok((input, result))
}

impl Decoder for Vec<u32> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, result) = read_vec(input)?;
        Ok((input, result))
    }
}
impl Decoder for Vec<u16> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, result) = read_vec(input)?;
        Ok((input, result))
    }
}

impl Decoder for Vec<u64> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, result) = read_vec(input)?;
        Ok((input, result))
    }
}

impl Decoder for Header {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, key) = read_u16(input)?;
        let (input, version) = read_u16(input)?;

        Ok((input, Header::new(extract_response_code(key), version)))
    }
}

impl Decoder for PublishedMessage {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, publishing_id) = u64::decode(input)?;
        let (input, body) = read_vec::<u8>(input)?;
        let (_, message) = Message::decode(&body)?;
        Ok((input, PublishedMessage::new(publishing_id, message)))
    }
}

impl Decoder for Vec<PublishedMessage> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (mut input, len) = u32::decode(input)?;
        let mut result = Vec::new();
        for _ in 0..len {
            let (input1, published_message) = PublishedMessage::decode(input)?;
            result.push(published_message);
            input = input1
        }
        Ok((input, result))
    }
}

fn extract_response_code(code: u16) -> u16 {
    code & 0b0111_1111_1111_1111
}

impl Decoder for Option<String> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, len) = read_i16(input)?;

        if len == 0 {
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

impl Decoder for PublishingError {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, publishing_id) = read_u64(input)?;
        let (input, code) = ResponseCode::decode(input)?;

        Ok((input, PublishingError::new(publishing_id, code)))
    }
}

impl Decoder for Vec<PublishingError> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (mut input, num_properties) = read_u32(input)?;
        let mut vec: Vec<PublishingError> = Vec::new();
        for _ in 0..num_properties {
            let (input1, value) = PublishingError::decode(input)?;

            vec.push(value);
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

pub fn read_i8(input: &[u8]) -> Result<(&[u8], i8), DecodeError> {
    check_len(input, 1)?;
    Ok((&input[1..], input[0] as i8))
}

reader!(read_i16, 2, i16);
reader!(read_u16, 2, u16);
reader!(read_u32, 4, u32);
reader!(read_i32, 4, i32);
reader!(read_u64, 8, u64);
reader!(read_i64, 8, i64);
