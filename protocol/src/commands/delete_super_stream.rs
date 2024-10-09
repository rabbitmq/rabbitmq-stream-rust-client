use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELETE_SUPER_STREAM,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct DeleteSuperStreamCommand {
    correlation_id: u32,
    super_stream_name: String,
}

impl DeleteSuperStreamCommand {
    pub fn new(correlation_id: u32, super_stream_name: String) -> Self {
        Self {
            correlation_id,
            super_stream_name,
        }
    }
}

impl Encoder for DeleteSuperStreamCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.super_stream_name.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.super_stream_name.as_str().encode(writer)?;
        Ok(())
    }
}

impl Decoder for DeleteSuperStreamCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, super_stream_name) = Option::decode(input)?;

        Ok((
            input,
            DeleteSuperStreamCommand {
                correlation_id,
                super_stream_name: super_stream_name.unwrap(),
            },
        ))
    }
}

impl Command for DeleteSuperStreamCommand {
    fn key(&self) -> u16 {
        COMMAND_DELETE_SUPER_STREAM
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::create_super_stream::CreateSuperStreamCommand;
    use crate::commands::tests::command_encode_decode_test;

    use super::DeleteSuperStreamCommand;

    #[test]
    fn delete_super_stream_request_test() {
        command_encode_decode_test::<DeleteSuperStreamCommand>();
    }
}
