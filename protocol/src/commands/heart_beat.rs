use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_HEARTBEAT,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct HeartBeatCommand {}

impl HeartBeatCommand {
    pub fn default() -> Self {
        Self {}
    }
}

impl Encoder for HeartBeatCommand {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, _writer: &mut impl Write) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl Command for HeartBeatCommand {
    fn key(&self) -> u16 {
        COMMAND_HEARTBEAT
    }
}

impl Decoder for HeartBeatCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        Ok((input, HeartBeatCommand {}))
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
pub struct HeartbeatResponse {}

impl Decoder for HeartbeatResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        Ok((input, HeartbeatResponse {}))
    }
}

impl Encoder for HeartbeatResponse {
    fn encoded_size(&self) -> u32 {
        0
    }

    fn encode(&self, _writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::HeartBeatCommand;
    use crate::commands::heart_beat::HeartbeatResponse;

    #[test]
    fn heart_request_test() {
        command_encode_decode_test::<HeartBeatCommand>();
    }

    #[test]
    fn heart_response_test() {
        command_encode_decode_test::<HeartbeatResponse>();
    }
}
