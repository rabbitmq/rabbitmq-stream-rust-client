use crate::{
    codec::Decoder, error::DecodeError, protocol::commands::COMMAND_METADATA_UPDATE, ResponseCode,
};

use super::Command;

use crate::codec::Encoder;
#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct MetadataUpdateCommand {
    correlation_id: u32,
    code: ResponseCode,
    stream: String,
}

impl Decoder for MetadataUpdateCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, stream) = Option::decode(input)?;

        Ok((
            input,
            MetadataUpdateCommand {
                correlation_id,
                code,
                stream: stream.unwrap(),
            },
        ))
    }
}

impl Encoder for MetadataUpdateCommand {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        self.correlation_id.encode(writer)?;
        self.code.encode(writer)?;
        self.stream.as_str().encode(writer)?;
        Ok(())
    }
}

impl Command for MetadataUpdateCommand {
    fn key(&self) -> u16 {
        COMMAND_METADATA_UPDATE
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::MetadataUpdateCommand;

    #[test]
    fn metadata_update_test() {
        command_encode_decode_test::<MetadataUpdateCommand>()
    }
}
