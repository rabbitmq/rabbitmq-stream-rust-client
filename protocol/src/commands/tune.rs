use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_TUNE,
    response::ResponseCode,
    types::CorrelationId,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
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

#[cfg_attr(test, derive(fake::Dummy))]
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
    use crate::codec::Encoder;

    use super::TunesCommand;
    use super::TunesResponse;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn tune_request_test() {
        command_encode_decode_test::<TunesCommand>()
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
        command_encode_decode_test::<TunesResponse>()
    }
}
