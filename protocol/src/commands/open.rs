use std::{collections::HashMap, io::Write};

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_OPEN,
    response::ResponseCode,
    types::CorrelationId,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct OpenCommand {
    correlation_id: CorrelationId,
    virtual_host: String,
}

impl OpenCommand {
    pub fn new(correlation_id: CorrelationId, virtual_host: String) -> Self {
        Self {
            correlation_id,
            virtual_host,
        }
    }
}

impl Encoder for OpenCommand {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.virtual_host.as_str().encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.virtual_host.as_str().encoded_size()
    }
}

impl Command for OpenCommand {
    fn key(&self) -> u16 {
        COMMAND_OPEN
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
pub struct OpenResponse {
    pub(crate) correlation_id: CorrelationId,
    pub(crate) code: ResponseCode,
    pub(crate) connection_properties: HashMap<String, String>,
}

impl OpenResponse {
    /// Get a reference to the open response's connection properties.
    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.connection_properties
    }
}

impl Decoder for OpenResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, connection_properties) = HashMap::decode(input)?;

        Ok((
            input,
            OpenResponse {
                correlation_id,
                code: response_code,
                connection_properties,
            },
        ))
    }
}

impl Decoder for OpenCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, virtual_host) = Option::decode(input)?;

        Ok((
            input,
            OpenCommand {
                correlation_id: correlation_id.into(),
                virtual_host: virtual_host.unwrap(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::OpenCommand;
    use crate::{
        codec::Encoder,
        commands::{open::OpenResponse, tests::command_encode_decode_test},
    };

    #[test]
    fn open_command_test() {
        command_encode_decode_test::<OpenCommand>()
    }

    impl Encoder for OpenResponse {
        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.correlation_id.encode(writer)?;
            self.code.encode(writer)?;
            self.connection_properties.encode(writer)?;
            Ok(())
        }

        fn encoded_size(&self) -> u32 {
            0
        }
    }

    #[test]
    fn open_response_test() {
        command_encode_decode_test::<OpenResponse>();
    }
}
