use std::io::Write;

use byteorder::{BigEndian, WriteBytesExt};
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
    subscription_id: u8,
    magic_version: i8,
    chunk_type: u8,
    num_entries: u16,
    num_records: u32,
    timestamp: u64,
    epoch: u64,
    chunk_first_offset: u64,
    chunk_crc: i32,
    trailer_length: u32,
    reserved: u32,
    data: Vec<u8>,
}
#[allow(clippy::too_many_arguments)]
impl DeliverCommand {
    pub fn new(
        subscription_id: u8,
        magic_version: i8,
        chunk_type: u8,
        num_entries: u16,
        num_records: u32,
        timestamp: u64,
        epoch: u64,
        chunk_first_offset: u64,
        chunk_crc: i32,
        trailer_length: u32,
        reserved: u32,
        data: Vec<u8>,
    ) -> Self {
        Self {
            subscription_id,
            magic_version,
            chunk_type,
            num_entries,
            num_records,
            timestamp,
            epoch,
            chunk_first_offset,
            chunk_crc,
            trailer_length,
            reserved,
            data,
        }
    }
}

impl Encoder for DeliverCommand {
    fn encoded_size(&self) -> u32 {
        self.subscription_id.encoded_size()
            + self.magic_version.encoded_size()
            + self.chunk_type.encoded_size()
            + self.num_entries.encoded_size()
            + self.num_records.encoded_size()
            + self.timestamp.encoded_size()
            + self.epoch.encoded_size()
            + self.chunk_first_offset.encoded_size()
            + self.chunk_crc.encoded_size()
            + self.trailer_length.encoded_size()
            + self.reserved.encoded_size()
            + self.data.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.subscription_id.encode(writer)?;
        self.magic_version.encode(writer)?;
        self.chunk_type.encode(writer)?;
        self.num_entries.encode(writer)?;
        self.num_records.encode(writer)?;
        self.timestamp.encode(writer)?;
        self.epoch.encode(writer)?;
        self.chunk_first_offset.encode(writer)?;
        self.chunk_crc.encode(writer)?;
        writer.write_u32::<BigEndian>(self.data.len() as u32)?;
        self.trailer_length.encode(writer)?;
        self.reserved.encode(writer)?;
        writer.write_all(&self.data)?;
        Ok(())
    }
}

impl Decoder for DeliverCommand {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, subscription_id) = u8::decode(input)?;
        let (input, magic_version) = i8::decode(input)?;
        let (input, chunk_type) = u8::decode(input)?;
        let (input, num_entries) = u16::decode(input)?;
        let (input, num_records) = u32::decode(input)?;
        let (input, timestamp) = u64::decode(input)?;
        let (input, epoch) = u64::decode(input)?;
        let (input, chunk_first_offset) = u64::decode(input)?;
        let (input, chunk_crc) = i32::decode(input)?;
        let (input, data_length) = u32::decode(input)?;
        let (input, trailer_length) = u32::decode(input)?;
        let (input, reserved) = u32::decode(input)?;

        let (input, data) = (
            &input[data_length as usize..],
            input[..data_length as usize].to_vec(),
        );

        Ok((
            input,
            DeliverCommand {
                subscription_id,
                magic_version,
                chunk_type,
                num_entries,
                num_records,
                timestamp,
                epoch,
                chunk_first_offset,
                chunk_crc,
                trailer_length,
                reserved,
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
