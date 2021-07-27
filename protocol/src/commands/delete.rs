use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELETE_STREAM,
    types::CorrelationId,
};

use super::Command;

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
        self.correlation_id.encoded_size() + self.stream.len() as u32
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
        let (input, stream) = Self::decode_str(input)?;

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
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn delete_stream_request_test() {
        let mut buffer = vec![];

        let delete_cmd = Delete {
            correlation_id: 1.into(),
            stream: "my_stream".to_owned(),
        };

        let _ = delete_cmd.encode(&mut buffer);

        let (remaining, decoded) = Delete::decode(&buffer).unwrap();

        assert_eq!(delete_cmd, decoded);

        assert!(remaining.is_empty());
    }
}
