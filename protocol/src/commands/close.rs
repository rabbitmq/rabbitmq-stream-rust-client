/*CloseRequest => Key Version CorrelationId ClosingCode ClosingReason

CloseResponse => Key Version CorrelationId ResponseCode
Key => uint16 // 22
Version => uint16
CorrelationId => uint32
ResponseCode => uint16
*/
use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CLOSE,
    FromResponse, ResponseCode,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct CloseRequest {
    correlation_id: u32,
    closing_code: ResponseCode,
    closing_reason: String,
}

impl CloseRequest {
    pub fn new(correlation_id: u32, closing_code: ResponseCode, closing_reason: String) -> Self {
        Self {
            correlation_id,
            closing_code,
            closing_reason,
        }
    }
}

impl Encoder for CloseRequest {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.closing_code.encode(writer)?;
        self.closing_reason.as_str().encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.closing_code.encoded_size()
            + self.closing_reason.as_str().encoded_size()
    }
}

impl Command for CloseRequest {
    fn key(&self) -> u16 {
        COMMAND_CLOSE
    }
}
impl Decoder for CloseRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, closing_code) = ResponseCode::decode(input)?;
        let (input, closing_reason) = Option::decode(input)?;

        Ok((
            input,
            CloseRequest {
                correlation_id,
                closing_code,
                closing_reason: closing_reason.unwrap(),
            },
        ))
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct CloseResponse {
    pub(crate) correlation_id: u32,
    response_code: ResponseCode,
}

impl CloseResponse {
    pub fn new(correlation_id: u32, response_code: ResponseCode) -> Self {
        Self {
            correlation_id,
            response_code,
        }
    }
    pub fn is_ok(&self) -> bool {
        self.response_code == ResponseCode::Ok
    }
}

impl Encoder for CloseResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() + self.response_code.encoded_size()
    }
}

impl Decoder for CloseResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;

        Ok((
            input,
            CloseResponse {
                correlation_id,
                response_code,
            },
        ))
    }
}

impl FromResponse for CloseResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::Close(close) => Some(close),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CloseRequest;
    use super::CloseResponse;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn close_request_test() {
        command_encode_decode_test::<CloseRequest>()
    }

    #[test]
    fn close_response_test() {
        command_encode_decode_test::<CloseResponse>()
    }
}
