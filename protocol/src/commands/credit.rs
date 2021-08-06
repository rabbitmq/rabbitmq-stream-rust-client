use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CREDIT,
    ResponseCode,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct CreditCommand {
    correlation_id: u32,
    subscription_id: u8,
    credit: u16,
}

impl CreditCommand {
    pub fn new(correlation_id: u32, subscription_id: u8, credit: u16) -> Self {
        Self {
            correlation_id,
            subscription_id,
            credit,
        }
    }
}

impl Encoder for CreditCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.subscription_id.encoded_size()
            + self.credit.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        self.credit.encode(writer)?;
        Ok(())
    }
}

impl Decoder for CreditCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;
        let (input, credit) = u16::decode(input)?;

        Ok((
            input,
            CreditCommand {
                correlation_id,
                subscription_id,
                credit,
            },
        ))
    }
}

impl Command for CreditCommand {
    fn key(&self) -> u16 {
        COMMAND_CREDIT
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
pub struct CreditResponse {
    pub(crate) correlation_id: u32,
    pub(crate) code: ResponseCode,
    pub(crate) subscription_id: u8,
}

impl Decoder for CreditResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, code) = ResponseCode::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;

        Ok((
            input,
            CreditResponse {
                correlation_id,
                code,
                subscription_id,
            },
        ))
    }
}

impl Encoder for CreditResponse {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        self.correlation_id.encode(writer)?;
        self.code.encode(writer)?;
        self.subscription_id.encode(writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::CreditCommand;
    use crate::commands::credit::CreditResponse;

    #[test]
    fn credit_request_test() {
        command_encode_decode_test::<CreditCommand>()
    }

    #[test]
    fn credit_response_test() {
        command_encode_decode_test::<CreditResponse>();
    }
}
