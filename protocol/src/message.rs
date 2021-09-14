use ntex_amqp_codec::{Encode, Message as AmpqMessage};
use ntex_bytes::BytesMut;

use crate::{
    codec::{Decoder, Encoder},
    error::DecodeError,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Message {
    pub(crate) publishing_id: Option<u64>,
    pub(crate) message: AmpqMessage,
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl Encoder for Message {
    fn encoded_size(&self) -> u32 {
        self.message.encoded_size() as u32
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        let mut buf = BytesMut::with_capacity(self.encoded_size() as usize);

        ntex_amqp_codec::Encode::encode(&self.message, &mut buf);

        writer.write_all(&buf)?;

        Ok(())
    }
}

impl Message {
    pub fn builder() -> MessageBuilder {
        MessageBuilder(Message {
            message: AmpqMessage::default(),
            publishing_id: None,
        })
    }

    pub fn data(&self) -> Option<&[u8]> {
        self.message.body().data().map(|data| data.as_ref())
    }

    /// Set the message's publishing id.
    pub fn set_publishing_id(&mut self, publishing_id: u64) {
        self.publishing_id = Some(publishing_id);
    }

    /// Get a reference to the message's publishing id.
    pub fn publishing_id(&self) -> Option<&u64> {
        self.publishing_id.as_ref()
    }
}

impl Decoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        ntex_amqp_codec::Decode::decode(input)
            .map_err(|err| DecodeError::MessageParse(err.to_string()))
            .map(|message| {
                (
                    message.0,
                    Message {
                        publishing_id: None,
                        message: message.1,
                    },
                )
            })
    }
}

pub struct MessageBuilder(Message);

impl MessageBuilder {
    pub fn body(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.0
            .message
            .set_body(|body| body.set_data(data.into().into()));
        self
    }

    pub fn publising_id(mut self, publishing_id: u64) -> Self {
        self.0.publishing_id = Some(publishing_id);
        self
    }
    pub fn build(self) -> Message {
        self.0
    }
}

impl From<Message> for Vec<Message> {
    fn from(message: Message) -> Self {
        vec![message]
    }
}
