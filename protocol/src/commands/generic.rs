use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    FromResponse, ResponseCode, ResponseKind,
};

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct GenericResponse {
    pub(crate) correlation_id: u32,
    code: ResponseCode,
}

impl GenericResponse {
    /// Get a reference to the generic response's code.
    pub fn code(&self) -> &ResponseCode {
        &self.code
    }

    pub fn is_ok(&self) -> bool {
        self.code == ResponseCode::Ok
    }
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
        let (input, correlation_id) = u32::decode(input)?;
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

impl FromResponse for GenericResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            ResponseKind::Generic(generic) => Some(generic),
            _ => None,
        }
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
