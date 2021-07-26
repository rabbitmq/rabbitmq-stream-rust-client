use std::collections::HashMap;
use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_PEER_PROPERTIES,
    types::CorrelationId,
};

use super::Command;

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
        let (input, client_properties) = Self::decode_map(input)?;

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

#[derive(Debug, PartialEq)]
pub struct PeerPropertiesResponse {
    server_properties: HashMap<String, String>,
}

impl PeerPropertiesResponse {
    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.server_properties
    }
}

impl Decoder for PeerPropertiesResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, server_properties) = Self::decode_map(input)?;

        Ok((input, PeerPropertiesResponse { server_properties }))
    }
}

#[cfg(test)]
mod tests {
    use super::PeerPropertiesCommand;
    use std::collections::HashMap;

    use crate::codec::{Decoder, Encoder};

    #[test]
    fn peer_properties_request_test() {
        let mut buffer = vec![];

        let mut peer_properties = HashMap::new();
        peer_properties.insert("product".to_string(), "RabbitMQ Stream".to_string());
        peer_properties.insert(
            "information".to_string(),
            "Licensed under the MPL 2.0.".to_string(),
        );

        let peer_properties = PeerPropertiesCommand {
            correlation_id: 99.into(),
            client_properties: peer_properties.to_owned(),
        };

        let _ = peer_properties.encode(&mut buffer);

        let (remaining, decoded) = PeerPropertiesCommand::decode(&buffer).unwrap();

        assert_eq!(peer_properties, decoded);

        assert!(remaining.is_empty());
    }
}
