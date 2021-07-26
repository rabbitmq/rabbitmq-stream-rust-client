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
        let (input, server_properties) = Self::decode_map(input)?;

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
    use std::collections::HashMap;

    use crate::codec::{Decoder, Encoder};
    use crate::commands::peer_properties::PeerPropertiesResponse;

    use super::PeerPropertiesCommand;
    use crate::ResponseCode;

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
    fn open_response_test() {
        let mut buffer = vec![];

        let mut properties = HashMap::new();
        properties.insert("server".to_owned(), "RabbitMQ server".to_owned());
        properties.insert("Version ".to_owned(), "5.5.5.5".to_owned());

        let peer_properties_response = PeerPropertiesResponse {
            correlation_id: 1.into(),
            code: ResponseCode::Ok,
            server_properties: properties,
        };

        let _ = peer_properties_response.encode(&mut buffer);

        let (remaining, decoded) = PeerPropertiesResponse::decode(&buffer).unwrap();

        assert_eq!(peer_properties_response, decoded);

        assert!(remaining.is_empty());
    }
}
