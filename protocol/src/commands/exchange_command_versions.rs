use std::io::Write;

use crate::{
    codec::{decoder::read_vec, Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::{COMMAND_EXCHANGE_COMMAND_VERSIONS, COMMAND_PUBLISH},
    response::{FromResponse, ResponseCode},
};

use super::Command;
use byteorder::{BigEndian, WriteBytesExt};

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
struct ExchangeCommandVersion(u16, u16, u16);

impl Encoder for ExchangeCommandVersion {
    fn encoded_size(&self) -> u32 {
        self.0.encoded_size() + self.1.encoded_size() + self.2.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.0.encode(writer)?;
        self.1.encode(writer)?;
        self.2.encode(writer)?;

        Ok(())
    }
}

impl Decoder for ExchangeCommandVersion {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, key) = u16::decode(input)?;
        let (input, min_version) = u16::decode(input)?;
        let (input, max_version) = u16::decode(input)?;
        Ok((input, ExchangeCommandVersion(key, min_version, max_version)))
    }
}

impl Encoder for Vec<ExchangeCommandVersion> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.len() as u32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Decoder for Vec<ExchangeCommandVersion> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, result) = read_vec(input)?;
        Ok((input, result))
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct ExchangeCommandVersionsRequest {
    correlation_id: u32,
    key: u16,
    min_version: u16,
    max_version: u16,
}

impl ExchangeCommandVersionsRequest {
    pub fn new(correlation_id: u32, min_version: u16, max_version: u16) -> Self {
        Self {
            correlation_id,
            min_version,
            max_version,
            key: COMMAND_PUBLISH,
        }
    }
}

impl Encoder for ExchangeCommandVersionsRequest {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + vec![ExchangeCommandVersion(
                self.key,
                self.min_version,
                self.max_version,
            )]
            .encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        vec![ExchangeCommandVersion(
            self.key,
            self.min_version,
            self.max_version,
        )]
        .encode(writer)?;
        Ok(())
    }
}

impl Command for ExchangeCommandVersionsRequest {
    fn key(&self) -> u16 {
        COMMAND_EXCHANGE_COMMAND_VERSIONS
    }
}

impl Decoder for ExchangeCommandVersionsRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, commands) = <Vec<ExchangeCommandVersion>>::decode(input)?;
        let command = commands.get(0);
        match command {
            Some(&ExchangeCommandVersion(key, min_version, max_version)) => Ok((
                input,
                ExchangeCommandVersionsRequest {
                    correlation_id,
                    key,
                    min_version,
                    max_version,
                },
            )),
            None => Ok((
                input,
                ExchangeCommandVersionsRequest {
                    correlation_id,
                    key: 0,
                    min_version: 0,
                    max_version: 0,
                },
            )),
        }
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct ExchangeCommandVersionsResponse {
    pub(crate) correlation_id: u32,
    response_code: ResponseCode,
    commands: Vec<ExchangeCommandVersion>,
}

impl ExchangeCommandVersionsResponse {
    // pub fn new(
    //     correlation_id: u32,
    //     response_code: ResponseCode,
    //     commands: Vec<ExchangeCommandVersion>,
    // ) -> Self {
    //     Self {
    //         correlation_id,
    //         response_code,
    //         commands,
    //     }
    // }

    pub fn code(&self) -> &ResponseCode {
        &self.response_code
    }

    pub fn is_ok(&self) -> bool {
        self.response_code == ResponseCode::Ok
    }

    pub fn key_version(&self, key_command: u16) -> (u16, u16) {
        for i in &self.commands {
            match i {
                ExchangeCommandVersion(match_key_command, min_version, max_version) => {
                    if *match_key_command == key_command {
                        return (min_version.clone(), max_version.clone());
                    }
                }
            }
        }

        (1, 1)
    }
}

impl Encoder for ExchangeCommandVersionsResponse {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.response_code.encoded_size()
            + self.commands.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.commands.encode(writer)?;
        Ok(())
    }
}

impl Decoder for ExchangeCommandVersionsResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, commands) = <Vec<ExchangeCommandVersion>>::decode(input)?;

        Ok((
            input,
            ExchangeCommandVersionsResponse {
                correlation_id,
                response_code,
                commands,
            },
        ))
    }
}

impl FromResponse for ExchangeCommandVersionsResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::ExchangeCommandVersions(exchange_command_versions) => {
                Some(exchange_command_versions)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::{ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse};

    #[test]
    fn exchange_command_versions_request_test() {
        command_encode_decode_test::<ExchangeCommandVersionsRequest>();
    }

    #[test]
    fn exchange_command_versions_response_test() {
        command_encode_decode_test::<ExchangeCommandVersionsResponse>();
    }
}
