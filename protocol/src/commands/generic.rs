use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    types::CorrelationId,
    ResponseCode,
};

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct GenericResponse {
    pub(crate) correlation_id: CorrelationId,
    code: ResponseCode,
}

impl Encoder for GenericResponse {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.code.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.code.encode(writer)?;
        Ok(())
    }
}

impl Decoder for GenericResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;

        Ok((
            input,
            GenericResponse {
                correlation_id,
                code,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::GenericResponse;

    #[test]
    fn sasl_authenticate_request_test() {
        command_encode_decode_test::<GenericResponse>()
    }
}
