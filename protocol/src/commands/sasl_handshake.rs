use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_SASL_HANDSHAKE,
    response::ResponseCode,
    types::CorrelationId,
};

use super::Command;

#[derive(PartialEq, Debug)]
pub struct SaslHandshakeCommand {
    correlation_id: CorrelationId,
}

impl SaslHandshakeCommand {
    pub fn new(correlation_id: CorrelationId) -> Self {
        Self { correlation_id }
    }
}

impl Encoder for SaslHandshakeCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        Ok(())
    }
}

impl Decoder for SaslHandshakeCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;

        Ok((input, SaslHandshakeCommand { correlation_id }))
    }
}

impl Command for SaslHandshakeCommand {
    fn key(&self) -> u16 {
        COMMAND_SASL_HANDSHAKE
    }
}

#[derive(Debug, PartialEq)]
pub struct SaslHandshakeResponse {
    pub(crate) correlation_id: CorrelationId,
    pub(crate) code: ResponseCode,
    pub(crate) mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    pub fn mechanisms(&self) -> &Vec<String> {
        &self.mechanisms
    }
}

impl Decoder for SaslHandshakeResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, mechanisms) = Self::decode_string_vec(input)?;

        Ok((
            input,
            SaslHandshakeResponse {
                correlation_id,
                code: response_code,
                mechanisms,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        codec::{Decoder, Encoder},
        ResponseCode,
    };

    use super::SaslHandshakeCommand;
    use super::SaslHandshakeResponse;

    #[test]
    fn sasl_handshake_request_test() {
        let mut buffer = vec![];

        let sasl_handshake = SaslHandshakeCommand {
            correlation_id: 66.into(),
        };

        let _ = sasl_handshake.encode(&mut buffer);

        let (remaining, decoded) = SaslHandshakeCommand::decode(&buffer).unwrap();

        assert_eq!(sasl_handshake, decoded);

        assert!(remaining.is_empty());
    }

    impl Encoder for SaslHandshakeResponse {
        fn encoded_size(&self) -> u32 {
            0
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.correlation_id.encode(writer)?;
            self.code.encode(writer)?;
            self.mechanisms.encode(writer)?;
            Ok(())
        }
    }
    #[test]
    fn sasl_handshake_response_test() {
        let mut buffer = vec![];

        let mut mechanisms: Vec<String> = Vec::new();
        mechanisms.push(String::from("PLAIN"));
        mechanisms.push(String::from("TEST"));

        let sasl_handshake_response = SaslHandshakeResponse {
            correlation_id: 77.into(),
            code: ResponseCode::Ok,
            mechanisms,
        };

        let _ = sasl_handshake_response.encode(&mut buffer);

        let (remaining, decoded) = SaslHandshakeResponse::decode(&buffer).unwrap();

        assert_eq!(sasl_handshake_response, decoded);

        assert!(remaining.is_empty());
    }
}
