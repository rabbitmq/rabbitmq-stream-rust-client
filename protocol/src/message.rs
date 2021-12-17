use std::sync::Arc;

use ntex_amqp_codec::{Encode, Message as AmpqMessage};
use ntex_bytes::BytesMut;

use crate::{
    codec::{Decoder, Encoder},
    error::DecodeError,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Message(Arc<InternalMessage>);

#[derive(Debug, PartialEq)]
pub struct InternalMessage {
    pub(crate) publishing_id: Option<u64>,
    pub(crate) message: AmpqMessage,
}

unsafe impl Send for Message {}
unsafe impl Sync for Message {}

impl Encoder for Message {
    fn encoded_size(&self) -> u32 {
        self.0.message.encoded_size() as u32
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        let mut buf = BytesMut::with_capacity(self.encoded_size() as usize);

        ntex_amqp_codec::Encode::encode(&self.0.message, &mut buf);

        writer.write_all(&buf)?;

        Ok(())
    }
}

impl Message {
    #[cfg(test)]
    pub(crate) fn new(internal: InternalMessage) -> Message {
        Message(Arc::new(internal))
    }
    pub fn builder() -> MessageBuilder {
        MessageBuilder(InternalMessage {
            message: AmpqMessage::default(),
            publishing_id: None,
        })
    }

    pub fn data(&self) -> Option<&[u8]> {
        self.0.message.body().data().map(|data| data.as_ref())
    }

    /// Get a reference to the message's publishing id.
    pub fn publishing_id(&self) -> Option<&u64> {
        self.0.publishing_id.as_ref()
    }
}

impl Decoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        ntex_amqp_codec::Decode::decode(input)
            .map_err(|err| DecodeError::MessageParse(err.to_string()))
            .map(|message| {
                (
                    message.0,
                    Message(Arc::new(InternalMessage {
                        publishing_id: None,
                        message: message.1,
                    })),
                )
            })
    }
}

pub struct MessageBuilder(InternalMessage);

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
        Message(Arc::new(self.0))
    }
}

impl From<Message> for Vec<Message> {
    fn from(message: Message) -> Self {
        vec![message]
    }
}
