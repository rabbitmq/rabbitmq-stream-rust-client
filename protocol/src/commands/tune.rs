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
    pub fn new(correlation_id: CorrelationId, max_frame_size: u32, heartbeat: u32 ) -> Self {
        Self {
            correlation_id,
            max_frame_size,
            heartbeat,
        }
    }
}


impl Encoder for TunesCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size() +
            self.heartbeat.encoded_size() +
            self.max_frame_size.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.heartbeat.encode(writer)?;
        self.max_frame_size.encode(writer)?;
        Ok(())
    }
}

impl Decoder for TunesCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = CorrelationId::decode(input)?;
        let (input, heartbeat) = u32::de (input)?;

        Ok((
            input,
            TunesCommand {
                correlation_id,

            },
        ))
    }
}

impl Command for PeerPropertiesCommand {
    fn key(&self) -> u16 {
        COMMAND_PEER_PROPERTIES
    }
}