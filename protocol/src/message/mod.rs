use std::convert::TryFrom;
use std::sync::Arc;

mod amqp;
mod builder;

use crate::codec::{Decoder, Encoder};
use crate::error::{DecodeError, EncodeError};
use crate::message::amqp::{AmqpDecoder, AmqpEncoder};

use amqp::AmqpMessage;

pub use self::amqp::{
    AmqpDecodeError, AmqpEncodeError, AnnonationKey, Annotations, ApplicationProperties,
    DeliveryAnnotations, Footer, Header, Map, MessageAnnotations, Properties, SimpleValue, Value,
};

use self::builder::MessageBuilder;

/// API for inbound and outbound messages
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Message(Arc<InternalMessage>);

#[derive(Debug, PartialEq, Eq, Default)]
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

    /// Builder for creating [`Message`]
    pub fn builder() -> MessageBuilder {
        MessageBuilder(InternalMessage {
            message: AmqpMessage::default(),
            publishing_id: None,
        })
    }

    /// Extract a value as reference from the `amqp-value` section of the body if present
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

    /// Get the data associated to the message if any
    pub fn data(&self) -> Option<&[u8]> {
        self.0.message.body().data().map(|data| data.as_slice())
    }

    /// Get the properties of the message
    pub fn properties(&self) -> Option<&Properties> {
        self.0.message.properties()
    }
    /// Get the header of the message
    pub fn header(&self) -> Option<&Header> {
        self.0.message.header()
    }

    /// Get the annotations of the message
    pub fn message_annotations(&self) -> Option<&MessageAnnotations> {
        self.0.message.message_annotations()
    }
    /// Get the application properties of the message
    pub fn application_properties(&self) -> Option<&ApplicationProperties> {
        self.0.message.application_properties()
    }

    /// Get the delivery annotations of the message
    pub fn delivery_annotations(&self) -> Option<&DeliveryAnnotations> {
        self.0.message.delivery_annotations()
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
