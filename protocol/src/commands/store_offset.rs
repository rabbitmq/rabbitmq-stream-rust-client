use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_STORE_OFFSET,
};
use std::io::Write;

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct StoreOffset {
    reference: String,
    stream: String,
    offset: u64,
}

impl StoreOffset {
    pub fn new(reference: String, stream: String, offset: u64) -> Self {
        Self {
            reference,
            stream,
            offset,
        }
    }
}

impl Encoder for StoreOffset {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        let size = self.reference.len();
        if size >= 256 {
            return Err(EncodeError::MaxSizeError(size));
        }

        self.reference.as_str().encode(writer)?;
        self.stream.as_str().encode(writer)?;
        self.offset.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.reference.as_str().encoded_size()
            + self.stream.as_str().encoded_size()
            + self.offset.encoded_size()
    }
}

impl Command for StoreOffset {
    fn key(&self) -> u16 {
        COMMAND_STORE_OFFSET
    }
}
impl Decoder for StoreOffset {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, opt_reference) = Option::decode(input)?;
        let (input, opt_stream) = Option::decode(input)?;
        if let (Some(reference), Some(stream)) = (opt_reference, opt_stream) {
            match reference.len() {
                0..=255 => {
                    let (input, offset) = u64::decode(input)?;

                    return Ok((
                        input,
                        StoreOffset {
                            reference,
                            stream,
                            offset,
                        },
                    ));
                }
                size => return Err(DecodeError::MismatchSize(size)),
            }
        }

        Err(DecodeError::Empty)
    }
}

#[cfg(test)]
mod tests {
    use super::StoreOffset;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn open_response_test() {
        command_encode_decode_test::<StoreOffset>();
    }
}
