use std::io::Write;

use super::Command;
use crate::codec::decoder::read_vec;
use crate::message::Message;
use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_DELIVER,
};
use byteorder::{BigEndian, WriteBytesExt};
#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Debug, Clone)]
pub struct DeliverCommand {
    pub subscription_id: u8,
    magic_version: i8,
    chunk_type: u8,
    num_entries: u16,
    timestamp: u64,
    epoch: u64,
    pub chunk_first_offset: u64,
    chunk_crc: i32,
    trailer_length: u32,
    reserved: u32,
    pub messages: Vec<Message>,
}

#[allow(clippy::too_many_arguments)]
impl DeliverCommand {
    pub fn new(
        subscription_id: u8,
        magic_version: i8,
        chunk_type: u8,
        num_entries: u16,
        timestamp: u64,
        epoch: u64,
        chunk_first_offset: u64,
        chunk_crc: i32,
        trailer_length: u32,
        reserved: u32,
        messages: Vec<Message>,
    ) -> Self {
        Self {
            subscription_id,
            magic_version,
            chunk_type,
            num_entries,
            timestamp,
            epoch,
            chunk_first_offset,
            chunk_crc,
            trailer_length,
            reserved,
            messages,
        }
    }
}

impl Encoder for DeliverCommand {
    fn encoded_size(&self) -> u32 {
        self.subscription_id.encoded_size()
            + self.magic_version.encoded_size()
            + self.chunk_type.encoded_size()
            + self.num_entries.encoded_size()
            + 4 // num records
            + self.timestamp.encoded_size()
            + self.epoch.encoded_size()
            + self.chunk_first_offset.encoded_size()
            + self.chunk_crc.encoded_size()
            + self.trailer_length.encoded_size()
            + self.reserved.encoded_size()
            + 4 // vec of messages
            + self.messages.iter().fold(0, |acc, message| {
                acc + 1 +  message.encoded_size()
            }) as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.subscription_id.encode(writer)?;
        self.magic_version.encode(writer)?;
        self.chunk_type.encode(writer)?;
        self.num_entries.encode(writer)?;
        u32::encode(&(self.messages.len() as u32), writer)?;
        self.timestamp.encode(writer)?;
        self.epoch.encode(writer)?;
        self.chunk_first_offset.encode(writer)?;
        self.chunk_crc.encode(writer)?;

        let size = self
            .messages
            .iter()
            .fold(0, |acc, message| acc + 1 + message.encoded_size());

        writer.write_u32::<BigEndian>(size as u32)?;
        self.trailer_length.encode(writer)?;
        self.reserved.encode(writer)?;

        for message in &self.messages {
            message.encoded_size().encode(writer)?;
            message.encode(writer)?;
        }
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
        let (input, _data_length) = u32::decode(input)?;
        let (input, trailer_length) = u32::decode(input)?;
        let (mut input, reserved) = u32::decode(input)?;

        let mut messages = Vec::with_capacity(num_records as usize);
        for _ in 0..num_records {
            let (input1, result) = read_vec(input)?;
            let (_, message) = Message::decode(&result)?;
            messages.push(message);
            input = input1;
        }

        Ok((
            input,
            DeliverCommand {
                subscription_id,
                magic_version,
                chunk_type,
                num_entries,
                timestamp,
                epoch,
                chunk_first_offset,
                chunk_crc,
                trailer_length,
                reserved,
                messages,
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
    use fake::{Dummy, Faker};

    use crate::commands::tests::command_encode_decode_test;
    use ntex_amqp_codec::Message as AmpqMessage;

    use super::{DeliverCommand, Message};
    impl Dummy<Faker> for Message {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(_config: &Faker, _rng: &mut R) -> Self {
            Message {
                message: AmpqMessage::default(),
                publishing_id: None,
            }
        }
    }

    #[test]
    fn deliver_request_test() {
        command_encode_decode_test::<DeliverCommand>();
    }
}
