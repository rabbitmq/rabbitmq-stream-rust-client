use std::io::Write;

use crate::error::{DecodeError, EncodeError};

pub mod decoder;
pub mod encoder;

pub trait Encoder {
    fn encoded_size(&self) -> u32;
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError>;
    fn encoded_size_version_2(&self) -> u32 {
        self.encoded_size()
    }
    fn encode_version_2(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.encode(writer)
    }
}

pub trait Decoder
where
    Self: Sized,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError>;
    fn decode_version_2(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        Decoder::decode(input)
    }
}
