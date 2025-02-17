use std::{collections::HashMap, io::Write};

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_OPEN,
    response::ResponseCode,
    FromResponse,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct OpenCommand {
    correlation_id: u32,
    virtual_host: String,
}

impl OpenCommand {
    pub fn new(correlation_id: u32, virtual_host: String) -> Self {
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
#[derive(Debug, PartialEq, Eq)]
pub struct OpenResponse {
    pub(crate) correlation_id: u32,
    pub(crate) code: ResponseCode,
    pub connection_properties: HashMap<String, String>,
}

impl OpenResponse {
    /// Get a reference to the generic response's code.
    pub fn code(&self) -> &ResponseCode {
        &self.code
    }

    pub fn is_ok(&self) -> bool {
        self.code == ResponseCode::Ok
    }
}

impl OpenResponse {
    /// Get a reference to the open response's connection properties.
    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.connection_properties
    }
}

impl FromResponse for OpenResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::Open(open) => Some(open),
            _ => None,
        }
    }
}

impl Decoder for OpenResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;

        let (input, connection_properties) = if response_code == ResponseCode::Ok {
            HashMap::decode(input)?
        } else {
            (input, HashMap::new())
        };

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
        let (input, correlation_id) = u32::decode(input)?;
        let (input, virtual_host) = Option::decode(input)?;

        Ok((
            input,
            OpenCommand {
                correlation_id,
                virtual_host: virtual_host.unwrap(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use fake::{Fake, Faker};

    use super::OpenCommand;
    use crate::{
        codec::Encoder,
        commands::{
            open::OpenResponse,
            tests::{command_encode_decode_test, specific_command_encode_decode_test},
        },
        ResponseCode,
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
        let mut response: OpenResponse = Faker.fake();
        response.code = ResponseCode::Ok;
        specific_command_encode_decode_test(response);
    }
}
