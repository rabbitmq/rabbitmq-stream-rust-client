use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELETE_STREAM,
    types::CorrelationId,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct Delete {
    correlation_id: CorrelationId,
    stream: String,
}

impl Delete {
    pub fn new(correlation_id: CorrelationId, stream: String) -> Self {
        Self {
            correlation_id,
            stream,
        }
    }
}

impl Encoder for Delete {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.stream.as_str().encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.stream.as_str().encoded_size()
    }
}

impl Command for Delete {
    fn key(&self) -> u16 {
        COMMAND_DELETE_STREAM
    }
}
impl Decoder for Delete {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, stream) = Option::decode(input)?;

        Ok((
            input,
            Delete {
                correlation_id,
                stream: stream.unwrap(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Delete;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn delete_stream_request_test() {
        command_encode_decode_test::<Delete>()
    }
}
