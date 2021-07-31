use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_TUNE,
    FromResponseRef,
};

use super::Command;

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct TunesCommand {
    pub max_frame_size: u32,
    pub heartbeat: u32,
}

impl FromResponseRef for TunesCommand {
    fn from_response(response: &crate::Response) -> Option<&Self> {
        match response.kind {
            crate::ResponseKind::Tunes(ref tunes) => Some(tunes),
            _ => None,
        }
    }
}

impl TunesCommand {
    pub fn new(max_frame_size: u32, heartbeat: u32) -> Self {
        Self {
            max_frame_size,
            heartbeat,
        }
    }

    /// Get a reference to the tunes command's heartbeat.
    pub fn heartbeat(&self) -> &u32 {
        &self.heartbeat
    }

    /// Get a reference to the tunes command's max frame size.
    pub fn max_frame_size(&self) -> &u32 {
        &self.max_frame_size
    }
}

impl Encoder for TunesCommand {
    fn encoded_size(&self) -> u32 {
        self.heartbeat.encoded_size() + self.max_frame_size.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.max_frame_size.encode(writer)?;
        self.heartbeat.encode(writer)?;
        Ok(())
    }
}

impl Decoder for TunesCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, max_frame_size) = u32::decode(input)?;
        let (input, heartbeat) = u32::decode(input)?;

        Ok((
            input,
            TunesCommand {
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

#[cfg(test)]
mod tests {

    use super::TunesCommand;
    use crate::commands::tests::command_encode_decode_test;

    #[test]
    fn tune_request_test() {
        command_encode_decode_test::<TunesCommand>()
    }
}
