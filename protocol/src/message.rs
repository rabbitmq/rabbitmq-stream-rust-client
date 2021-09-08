use ntex_amqp_codec::{Encode, Message as AmpqMessage};
use ntex_bytes::BytesMut;

use crate::{
    codec::{Decoder, Encoder},
    error::DecodeError,
};

#[derive(Debug, PartialEq)]
pub struct Message(pub(crate) AmpqMessage);

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl Encoder for Message {
    fn encoded_size(&self) -> u32 {
        self.0.encoded_size() as u32
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        let mut buf = BytesMut::with_capacity(self.encoded_size() as usize);

        ntex_amqp_codec::Encode::encode(&self.0, &mut buf);

        writer.write_all(&buf)?;

        Ok(())
    }
}

impl Decoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        ntex_amqp_codec::Decode::decode(input)
            .map_err(|err| DecodeError::MessageParse(err.to_string()))
            .map(|message| (message.0, Message(message.1)))
    }
}
