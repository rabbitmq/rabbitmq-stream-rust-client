use std::collections::HashMap;
use std::io::Write;

use crate::{
    codec::Encoder, error::EncodeError, protocol::commands::COMMAND_CREATE_STREAM,
    types::CorrelationId,
};

use super::Command;

#[derive(PartialEq, Debug)]
pub struct CreateStreamCommand {
    correlation_id: CorrelationId,
    stream_name: String,
    args: HashMap<String, String>,
}

impl CreateStreamCommand {
    pub fn new(
        correlation_id: CorrelationId,
        stream_name: String,
        args: HashMap<String, String>,
    ) -> Self {
        Self {
            correlation_id,
            stream_name,
            args,
        }
    }
}

impl Encoder for CreateStreamCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.stream_name.as_str().encoded_size()
            + self.args.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.stream_name.as_str().encode(writer)?;
        self.args.encode(writer)?;
        Ok(())
    }
}

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
        types::CorrelationId,
    };

    use super::CreateStreamCommand;

    impl Decoder for CreateStreamCommand {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
            let (input, correlation_id) = CorrelationId::decode(input)?;
            let (input, stream_name) = Self::decode_str(input)?;
            let (input, args) = Self::decode_map(input)?;

            Ok((
                input,
                CreateStreamCommand {
                    correlation_id,
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
            correlation_id: 1.into(),
            stream_name: "my_stream".to_owned(),
            args: stream_args.to_owned(),
        };

        let _ = create_stream.encode(&mut buffer);

        let (remaining, decoded) = CreateStreamCommand::decode(&buffer).unwrap();

        assert_eq!(create_stream, decoded);

        assert!(remaining.is_empty());
    }
}
