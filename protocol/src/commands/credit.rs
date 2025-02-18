use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_CREDIT,
    ResponseCode,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct CreditCommand {
    subscription_id: u8,
    credit: u16,
}

impl CreditCommand {
    pub fn new(subscription_id: u8, credit: u16) -> Self {
        Self {
            subscription_id,
            credit,
        }
    }
}

impl Encoder for CreditCommand {
    fn encoded_size(&self) -> u32 {
        self.subscription_id.encoded_size() + self.credit.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.subscription_id.encode(writer)?;
        self.credit.encode(writer)?;
        Ok(())
    }
}

impl Decoder for CreditCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, subscription_id) = u8::decode(input)?;
        let (input, credit) = u16::decode(input)?;

        Ok((
            input,
            CreditCommand {
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
#[derive(Debug, PartialEq, Eq)]
pub struct CreditResponse {
    pub(crate) code: ResponseCode,
    pub(crate) subscription_id: u8,
}

impl Decoder for CreditResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, code) = ResponseCode::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;

        Ok((
            input,
            CreditResponse {
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
