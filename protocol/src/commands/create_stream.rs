use std::io::Write;

use crate::{codec::Encoder, error::EncodeError, protocol::commands::COMMAND_CREATE_STREAM};

use super::{Command, Correlated};

#[derive(PartialEq, Debug)]
pub struct CreateStreamCommand {
    stream_name: String,
}

impl CreateStreamCommand {
    pub fn new(stream_name: String) -> Self {
        Self { stream_name }
    }
}

impl Encoder for CreateStreamCommand {
    fn encoded_size(&self) -> u32 {
        4 + self.stream_name.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.encode_str(writer, &self.stream_name)?;
        Ok(())
    }
}

impl Correlated for CreateStreamCommand {}

impl Command for CreateStreamCommand {
    fn key(&self) -> u16 {
        COMMAND_CREATE_STREAM
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        codec::{Decoder, Encoder},
        error::DecodeError,
    };

    use super::CreateStreamCommand;

    impl Decoder for CreateStreamCommand {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
            let (remaining, stream_name) = Self::decode_str(input)?;

            Ok((
                remaining,
                CreateStreamCommand {
                    stream_name: stream_name.unwrap(),
                },
            ))
        }
    }

    #[test]
    fn create_stream_request_test() {
        let mut buffer = vec![];

        let create_stream = CreateStreamCommand {
            stream_name: "my_stream".to_owned(),
        };

        let _ = create_stream.encode(&mut buffer);

        let (remaining, decoded) = CreateStreamCommand::decode(&buffer).unwrap();

        assert_eq!(create_stream, decoded);

        assert!(remaining.is_empty());
    }
}
