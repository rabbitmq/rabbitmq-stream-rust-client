use std::convert::TryFrom;
use std::sync::Arc;

mod amqp;
mod builder;

use crate::codec::{Decoder, Encoder};
use crate::error::{DecodeError, EncodeError};
use crate::message::amqp::{AmqpDecoder, AmqpEncoder};

use amqp::AmqpMessage;

use self::amqp::{
    AmqpDecodeError, AmqpEncodeError, ApplicationProperties, Header, Properties, Value,
};
use self::builder::MessageBuilder;

#[derive(Debug, PartialEq, Clone)]
pub struct Message(Arc<InternalMessage>);

#[derive(Debug, PartialEq, Default)]
pub struct InternalMessage {
    pub(crate) publishing_id: Option<u64>,
    pub(crate) message: AmqpMessage,
}

impl Encoder for Message {
    fn encoded_size(&self) -> u32 {
        self.0.message.encoded_size()
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), crate::error::EncodeError> {
        self.0.message.encode(writer)?;
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
            message: AmqpMessage::default(),
            publishing_id: None,
        })
    }

    pub fn value_ref<'a, T>(&'a self) -> Result<Option<T>, DecodeError>
    where
        T: TryFrom<&'a Value, Error = DecodeError>,
    {
        self.0
            .message
            .body()
            .value()
            .map(|value| T::try_from(value))
            .transpose()
    }

    pub fn data(&self) -> Option<&[u8]> {
        self.0.message.body().data().map(|data| data.as_slice())
    }

    pub fn properties(&self) -> Option<&Properties> {
        self.0.message.properties()
    }
    pub fn header(&self) -> Option<&Header> {
        self.0.message.header()
    }
    pub fn application_properties(&self) -> Option<&ApplicationProperties> {
        self.0.message.application_properties()
    }

    /// Get a reference to the message's publishing id.
    pub fn publishing_id(&self) -> Option<&u64> {
        self.0.publishing_id.as_ref()
    }
}

impl Decoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        AmqpMessage::decode(input)
            .map(|(remaining, message)| {
                (
                    remaining,
                    Message(Arc::new(InternalMessage {
                        publishing_id: None,
                        message,
                    })),
                )
            })
            .map(Ok)?
    }
}

impl From<Message> for Vec<Message> {
    fn from(message: Message) -> Self {
        vec![message]
    }
}

impl From<AmqpEncodeError> for EncodeError {
    fn from(err: AmqpEncodeError) -> Self {
        match err {
            AmqpEncodeError::Io(err) => EncodeError::Io(err),
        }
    }
}

impl From<AmqpDecodeError> for DecodeError {
    fn from(err: AmqpDecodeError) -> Self {
        match err {
            AmqpDecodeError::InvalidTypeCode(code) => DecodeError::InvalidFormatCode(code),
            AmqpDecodeError::MessageParse(err) => DecodeError::MessageParse(err),
            AmqpDecodeError::Incomplete(err) => DecodeError::Incomplete(err),
            AmqpDecodeError::Utf8Error(err) => DecodeError::Utf8Error(err),
            AmqpDecodeError::UuidError(err) => DecodeError::MessageParse(err.to_string()),
            AmqpDecodeError::InvalidTypeCodeFor { target, code } => {
                DecodeError::MessageParse(format!("Invalid type code {:?} for {}", code, target))
            }
        }
    }
}
