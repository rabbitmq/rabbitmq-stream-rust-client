use std::collections::HashMap;
use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PEER_PROPERTIES,
    types::CorrelationId,
    ResponseCode,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct PeerPropertiesCommand {
    correlation_id: CorrelationId,
    client_properties: HashMap<String, String>,
}

impl PeerPropertiesCommand {
    pub fn new(correlation_id: CorrelationId, client_properties: HashMap<String, String>) -> Self {
        Self {
            correlation_id,
            client_properties,
        }
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
        let (input, correlation_id) = CorrelationId::decode(input)?;
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
#[derive(Debug, PartialEq)]
pub struct PeerPropertiesResponse {
    pub(crate) correlation_id: CorrelationId,
    pub(crate) code: ResponseCode,
    pub(crate) server_properties: HashMap<String, String>,
}

impl PeerPropertiesResponse {
    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.server_properties
    }
}

impl Decoder for PeerPropertiesResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
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
