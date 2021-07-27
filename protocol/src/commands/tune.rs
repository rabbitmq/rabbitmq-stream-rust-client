use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_TUNE,
    response::ResponseCode,
    types::CorrelationId,
};

use super::Command;

#[derive(PartialEq, Debug)]
pub struct TunesCommand {
    correlation_id: CorrelationId,
    max_frame_size: u32,
    heartbeat: u32,
}

impl TunesCommand {
    pub fn new(correlation_id: CorrelationId, max_frame_size: u32, heartbeat: u32) -> Self {
        Self {
            correlation_id,
            max_frame_size,
            heartbeat,
        }
    }
}

impl Encoder for TunesCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.heartbeat.encoded_size()
            + self.max_frame_size.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.max_frame_size.encode(writer)?;
        self.heartbeat.encode(writer)?;
        Ok(())
    }
}

impl Decoder for TunesCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, max_frame_size) = u32::decode(input)?;
        let (input, heartbeat) = u32::decode(input)?;

        Ok((
            input,
            TunesCommand {
                correlation_id,
                max_frame_size,
                heartbeat,
            },
        ))
    }
}

impl Command for TunesCommand {
    fn key(&self) -> u16 {
        COMMAND_TUNE
    }
}

#[derive(Debug, PartialEq)]
pub struct TunesResponse {
    pub(crate) correlation_id: CorrelationId,
    pub(crate) code: ResponseCode,
    pub(crate) max_frame_size: u32,
    pub(crate) heartbeat: u32,
}

impl TunesResponse {
    pub fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    pub fn heartbeat(&self) -> u32 {
        self.heartbeat
    }
}

impl Decoder for TunesResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, max_frame_size) = u32::decode(input)?;
        let (input, heartbeat) = u32::decode(input)?;
        Ok((
            input,
            TunesResponse {
                correlation_id,
                max_frame_size,
                heartbeat,
                code: response_code,
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

    use super::TunesCommand;
    use super::TunesResponse;

    #[test]
    fn tune_request_test() {
        let mut buffer = vec![];

        let tune_command = TunesCommand {
            correlation_id: 66.into(),
            max_frame_size: 99999,
            heartbeat: 66,
        };

        let _ = tune_command.encode(&mut buffer);

        let (remaining, decoded) = TunesCommand::decode(&buffer).unwrap();

        assert_eq!(tune_command, decoded);

        assert!(remaining.is_empty());
    }

    impl Encoder for TunesResponse {
        fn encoded_size(&self) -> u32 {
            0
        }

        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.correlation_id.encode(writer)?;
            self.code.encode(writer)?;
            self.max_frame_size.encode(writer)?;
            self.heartbeat.encode(writer)?;
            Ok(())
        }
    }
    #[test]
    fn tune_response_test() {
        let mut buffer = vec![];

        let mut mechanisms: Vec<String> = Vec::new();
        mechanisms.push(String::from("PLAIN"));
        mechanisms.push(String::from("TEST"));

        let tune_response = TunesResponse {
            correlation_id: 77.into(),
            code: ResponseCode::Ok,
            max_frame_size: 88888,
            heartbeat: 99,
        };

        let _ = tune_response.encode(&mut buffer);

        let (remaining, decoded) = TunesResponse::decode(&buffer).unwrap();

        assert_eq!(tune_response, decoded);

        assert!(remaining.is_empty());
    }
}
