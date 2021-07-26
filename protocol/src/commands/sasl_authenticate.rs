use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_SASL_AUTHENTICATE,
    types::CorrelationId,
    ResponseCode,
};

use super::Command;

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
        let (input, mechanism) = Self::decode_str(input)?;
        let (input, sasl_data) = Vec::<u8>::decode(input)?;

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

#[derive(Debug, PartialEq)]
pub struct SaslAuthenticateResponse {
    pub(crate) correlation_id: CorrelationId,
    pub(crate) code: ResponseCode,
    pub(crate) mechanism: String,
}

impl SaslAuthenticateResponse {}

impl Decoder for SaslAuthenticateResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, mechanism) = Self::decode_str(input)?;

        Ok((
            input,
            SaslAuthenticateResponse {
                correlation_id,
                code,
                mechanism: mechanism.unwrap(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::codec::{Decoder, Encoder};
    use crate::commands::sasl_authenticate::SaslAuthenticateResponse;

    use super::SaslAuthenticateCommand;
    use crate::ResponseCode;

    #[test]
    fn sasl_authenticate_request_test() {
        let mut buffer = vec![];

        let peer_properties = SaslAuthenticateCommand {
            correlation_id: 99.into(),
            mechanism: "plain".to_owned(),
            sasl_data: vec![],
        };

        let _ = peer_properties.encode(&mut buffer);

        let (remaining, decoded) = SaslAuthenticateCommand::decode(&buffer).unwrap();

        assert_eq!(peer_properties, decoded);

        assert!(remaining.is_empty());
    }

    impl Encoder for SaslAuthenticateResponse {
        fn encoded_size(&self) -> u32 {
            0
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.correlation_id.encode(writer)?;
            self.code.encode(writer)?;
            self.mechanism.as_str().encode(writer)?;
            Ok(())
        }
    }

    #[test]
    fn sasl_authenticate_response_test() {
        let mut buffer = vec![];

        let peer_properties_response = SaslAuthenticateResponse {
            correlation_id: 1.into(),
            code: ResponseCode::Ok,
            mechanism: "plain".to_owned(),
        };

        let _ = peer_properties_response.encode(&mut buffer);

        let (remaining, decoded) = SaslAuthenticateResponse::decode(&buffer).unwrap();

        assert_eq!(peer_properties_response, decoded);

        assert!(remaining.is_empty());
    }
}
