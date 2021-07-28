use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_SASL_AUTHENTICATE,
    types::CorrelationId,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct SaslAuthenticateCommand {
    correlation_id: CorrelationId,
    mechanism: String,
    sasl_data: Vec<u8>,
}

impl SaslAuthenticateCommand {
    pub fn new(correlation_id: CorrelationId, mechanism: String, sasl_data: Vec<u8>) -> Self {
        Self {
            correlation_id,
            mechanism,
            sasl_data,
        }
    }
}

impl Encoder for SaslAuthenticateCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.mechanism.as_str().encoded_size()
            + self.sasl_data.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.mechanism.as_str().encode(writer)?;
        self.sasl_data.encode(writer)?;
        Ok(())
    }
}

impl Decoder for SaslAuthenticateCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, mechanism) = Option::decode(input)?;
        let (input, sasl_data) = Vec::decode(input)?;

        Ok((
            input,
            SaslAuthenticateCommand {
                correlation_id,
                mechanism: mechanism.unwrap(),
                sasl_data,
            },
        ))
    }
}

impl Command for SaslAuthenticateCommand {
    fn key(&self) -> u16 {
        COMMAND_SASL_AUTHENTICATE
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::SaslAuthenticateCommand;

    #[test]
    fn sasl_authenticate_request_test() {
        command_encode_decode_test::<SaslAuthenticateCommand>()
    }
}
