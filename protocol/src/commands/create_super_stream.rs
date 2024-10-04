use std::collections::HashMap;
use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CREATE_SUPER_STREAM,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct CreateSuperStreamCommand {
    correlation_id: u32,
    super_stream_name: String,
    partitions: Vec<String>,
    binding_keys: Vec<String>,
    args: HashMap<String, String>,
}

impl CreateSuperStreamCommand {
    pub fn new(
        correlation_id: u32,
        super_stream_name: String,
        partitions: Vec<String>,
        binding_keys: Vec<String>,
        args: HashMap<String, String>,
    ) -> Self {
        Self {
            correlation_id,
            super_stream_name,
            partitions,
            binding_keys,
            args,
        }
    }
}

impl Encoder for CreateSuperStreamCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.super_stream_name.as_str().encoded_size()
            + self.partitions.encoded_size()
            + self.binding_keys.encoded_size()
            + self.args.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.super_stream_name.as_str().encode(writer)?;
        self.partitions.encode(writer)?;
        self.binding_keys.encode(writer)?;
        self.args.encode(writer)?;
        Ok(())
    }
}

impl Decoder for CreateSuperStreamCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, super_stream_name) = Option::decode(input)?;
        let (input, partitions) = <Vec<String>>::decode(input)?;
        let (input, binding_keys) = <Vec<String>>::decode(input)?;
        let (input, args) = HashMap::decode(input)?;

        Ok((
            input,
            CreateSuperStreamCommand {
                correlation_id,
                super_stream_name: super_stream_name.unwrap(),
                partitions,
                binding_keys,
                args,
            },
        ))
    }
}

impl Command for CreateSuperStreamCommand {
    fn key(&self) -> u16 {
        COMMAND_CREATE_SUPER_STREAM
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::create_stream::CreateStreamCommand;
    use crate::commands::tests::command_encode_decode_test;

    use super::CreateSuperStreamCommand;

    #[test]
    fn create_super_stream_request_test() {
        command_encode_decode_test::<CreateSuperStreamCommand>();
    }
}
