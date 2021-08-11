use crate::codec::Decoder;
use ntex_amqp_codec::{
    protocol::{Annotations, Header, MessageFormat, Properties},
    types::{VecStringMap, VecSymbolMap},
    Decode, Message as AmqpMessage, MessageBody,
};

#[derive(Debug, PartialEq)]
pub struct Message(pub InternalMessage);

#[derive(Debug, PartialEq)]
pub struct InternalMessage {
    pub message_format: Option<MessageFormat>,
    pub header: Option<Header>,
    pub delivery_annotations: Option<VecSymbolMap>,
    pub message_annotations: Option<VecSymbolMap>,
    pub properties: Option<Properties>,
    pub application_properties: Option<VecStringMap>,
    pub footer: Option<Annotations>,
    pub body: Vec<Vec<u8>>,
}

impl Decoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        AmqpMessage::decode(input).map(|(remaining, message)| {
            Ok((
                remaining,
                Message(InternalMessage {
                    message_format: message.message_format,
                    header: message.header,
                    delivery_annotations: message.delivery_annotations,
                    message_annotations: message.message_annotations,
                    properties: message.properties,
                    application_properties: message.application_properties,
                    footer: message.footer,
                    body: message
                        .body
                        .data
                        .into_iter()
                        .map(|bytes| bytes.into_iter().collect())
                        .collect(),
                }),
            ))
        })?
    }
}
