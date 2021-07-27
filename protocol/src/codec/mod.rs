use std::io::Write;

use crate::error::{DecodeError, EncodeError};

pub mod decoder;
pub mod encoder;

pub trait Encoder {
    fn encoded_size(&self) -> u32;
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError>;
}

pub(crate) trait Decoder
where
    Self: Sized,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError>;
}
