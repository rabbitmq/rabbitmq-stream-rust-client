use std::io::Write;

#[cfg(test)]
use fake::Fake;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELIVER,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug)]
pub struct DeliverCommand {
    correlation_id: u32,
    subscription_id: u8,
    magic_version: i8,
    num_entries: u16,
    num_records: u32,
    epoch: u64,
    chunk_first_offset: u64,
    chunk_crc: i32,
    data_length: u32,
    data: Vec<u8>,
}
#[allow(clippy::too_many_arguments)]
impl DeliverCommand {
    pub fn new(
        correlation_id: u32,
        subscription_id: u8,
        magic_version: i8,
        num_entries: u16,
        num_records: u32,
        epoch: u64,
        chunk_first_offset: u64,
        chunk_crc: i32,
        data_length: u32,
        data: Vec<u8>,
    ) -> Self {
        Self {
            correlation_id,
            subscription_id,
            magic_version,
            num_entries,
            num_records,
            epoch,
            chunk_first_offset,
            chunk_crc,
            data_length,
            data,
        }
    }
}

impl Encoder for DeliverCommand {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.subscription_id.encoded_size()
            + self.magic_version.encoded_size()
            + self.num_entries.encoded_size()
            + self.num_records.encoded_size()
            + self.epoch.encoded_size()
            + self.chunk_first_offset.encoded_size()
            + self.chunk_crc.encoded_size()
            + self.data_length.encoded_size()
            + self.data.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.subscription_id.encode(writer)?;
        self.magic_version.encode(writer)?;
        self.num_entries.encode(writer)?;
        self.num_records.encode(writer)?;
        self.epoch.encode(writer)?;
        self.chunk_first_offset.encode(writer)?;
        self.chunk_crc.encode(writer)?;
        self.data_length.encode(writer)?;
        self.data.encode(writer)?;
        Ok(())
    }
}

impl Decoder for DeliverCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, subscription_id) = u8::decode(input)?;
        let (input, magic_version) = i8::decode(input)?;
        let (input, num_entries) = u16::decode(input)?;
        let (input, num_records) = u32::decode(input)?;
        let (input, epoch) = u64::decode(input)?;
        let (input, chunk_first_offset) = u64::decode(input)?;
        let (input, chunk_crc) = i32::decode(input)?;
        let (input, data_length) = u32::decode(input)?;
        let (input, data) = Vec::decode(input)?;

        Ok((
            input,
            DeliverCommand {
                correlation_id,
                subscription_id,
                magic_version,
                num_entries,
                num_records,
                epoch,
                chunk_first_offset,
                chunk_crc,
                data_length,
                data,
            },
        ))
    }
}

impl Command for DeliverCommand {
    fn key(&self) -> u16 {
        COMMAND_DELIVER
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::tests::command_encode_decode_test;

    use super::DeliverCommand;

    #[test]
    fn deliver_request_test() {
        command_encode_decode_test::<DeliverCommand>();
    }
}
