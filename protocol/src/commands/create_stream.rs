use std::collections::HashMap;
use std::io::Write;

use crate::{codec::Encoder, error::EncodeError, protocol::commands::COMMAND_CREATE_STREAM};

use super::{Command, Correlated};

#[derive(PartialEq, Debug)]
pub struct CreateStreamCommand {
    stream_name: String,
    args: HashMap<String, String>,
}

impl CreateStreamCommand {
    pub fn new(stream_name: String, args: HashMap<String, String>) -> Self {
        Self { stream_name, args }
    }
}

impl Encoder for CreateStreamCommand {
    fn encoded_size(&self) -> u32 {
        4 + self.stream_name.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.encode_str(writer, &self.stream_name)?;
        self.encode_map(writer, &self.args)?;
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
    use std::collections::HashMap;

    use crate::{
        codec::{Decoder, Encoder},
        error::DecodeError,
    };

    use super::CreateStreamCommand;

    impl Decoder for CreateStreamCommand {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
            let (remaining_str, stream_name) = Self::decode_str(input)?;
            let (remaining, args) = Self::decode_map(remaining_str)?;

            Ok((
                remaining,
                CreateStreamCommand {
                    stream_name: stream_name.unwrap(),
                    args,
                },
            ))
        }
    }

    #[test]
    fn create_stream_request_test() {
        let mut buffer = vec![];

        let mut stream_args = HashMap::new();
        stream_args.insert("max-len".to_string(), "1GB".to_string());
        stream_args.insert("max-age".to_string(), "1000".to_string());

        let create_stream = CreateStreamCommand {
            stream_name: "my_stream".to_owned(),
            args: stream_args.to_owned(),
        };

        let _ = create_stream.encode(&mut buffer);

        let (remaining, decoded) = CreateStreamCommand::decode(&buffer).unwrap();

        assert_eq!(create_stream, decoded);

        assert!(remaining.is_empty());
    }
}
