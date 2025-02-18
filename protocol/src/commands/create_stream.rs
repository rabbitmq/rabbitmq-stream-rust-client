use std::collections::HashMap;
use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CREATE_STREAM,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct CreateStreamCommand {
    correlation_id: u32,
    stream_name: String,
    args: HashMap<String, String>,
}

impl CreateStreamCommand {
    pub fn new(correlation_id: u32, stream_name: String, args: HashMap<String, String>) -> Self {
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

impl Decoder for CreateStreamCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, stream_name) = Option::decode(input)?;
        let (input, args) = HashMap::decode(input)?;

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

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::CreateStreamCommand;

    #[test]
    fn create_stream_request_test() {
        command_encode_decode_test::<CreateStreamCommand>();
    }
}
