use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_SASL_HANDSHAKE,
    response::ResponseCode,
    types::CorrelationId,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
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

#[cfg_attr(test, derive(fake::Dummy))]
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
        let (input, mechanisms) = Vec::decode(input)?;

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

    use crate::{codec::Encoder, commands::tests::command_encode_decode_test};

    use super::SaslHandshakeCommand;
    use super::SaslHandshakeResponse;

    #[test]
    fn sasl_handshake_request_test() {
        command_encode_decode_test::<SaslHandshakeCommand>();
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
        command_encode_decode_test::<SaslHandshakeResponse>()
    }
}
