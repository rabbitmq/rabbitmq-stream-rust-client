use std::collections::HashMap;
use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PEER_PROPERTIES,
    FromResponse, ResponseCode,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct PeerPropertiesCommand {
    correlation_id: u32,
    client_properties: HashMap<String, String>,
}

impl PeerPropertiesCommand {
    pub fn new(correlation_id: u32, client_properties: HashMap<String, String>) -> Self {
        Self {
            correlation_id,
            client_properties,
        }
    }

    /// Get a reference to the peer properties command's correlation id.
    pub fn correlation_id(&self) -> u32 {
        self.correlation_id
    }
}

impl Encoder for PeerPropertiesCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.client_properties.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.client_properties.encode(writer)?;
        Ok(())
    }
}

impl Decoder for PeerPropertiesCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, client_properties) = HashMap::decode(input)?;

        Ok((
            input,
            PeerPropertiesCommand {
                correlation_id,
                client_properties,
            },
        ))
    }
}

impl Command for PeerPropertiesCommand {
    fn key(&self) -> u16 {
        COMMAND_PEER_PROPERTIES
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq)]
pub struct PeerPropertiesResponse {
    pub(crate) correlation_id: u32,
    pub(crate) code: ResponseCode,
    pub server_properties: HashMap<String, String>,
}

impl PeerPropertiesResponse {
    pub fn new(
        correlation_id: u32,
        code: ResponseCode,
        server_properties: HashMap<String, String>,
    ) -> Self {
        Self {
            correlation_id,
            code,
            server_properties,
        }
    }

    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.server_properties
    }
}

impl FromResponse for PeerPropertiesResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::PeerProperties(peer_properties) => Some(peer_properties),
            _ => None,
        }
    }
}

impl Decoder for PeerPropertiesResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, server_properties) = HashMap::decode(input)?;

        Ok((
            input,
            PeerPropertiesResponse {
                correlation_id,
                code,
                server_properties,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::codec::Encoder;
    use crate::commands::peer_properties::PeerPropertiesResponse;
    use crate::commands::tests::command_encode_decode_test;

    use super::PeerPropertiesCommand;

    #[test]
    fn peer_properties_request_test() {
        command_encode_decode_test::<PeerPropertiesCommand>()
    }

    impl Encoder for PeerPropertiesResponse {
        fn encoded_size(&self) -> u32 {
            0
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.correlation_id.encode(writer)?;
            self.code.encode(writer)?;
            self.server_properties.encode(writer)?;
            Ok(())
        }
    }

    #[test]
    fn peer_properties_response_test() {
        command_encode_decode_test::<PeerPropertiesResponse>();
    }
}
