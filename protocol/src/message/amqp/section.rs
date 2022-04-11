use crate::{
    message::amqp::codec::constants::MESSAGE_BODY_DATA,
    message::amqp::{types::Binary, AmqpDecoder},
    utils::TupleMapperSecond,
};

use super::{
    codec::constants::{
        MESSAGE_ANNOTATIONS, MESSAGE_APPLICATION_PROPERTIES, MESSAGE_BODY_SEQUENCE,
        MESSAGE_BODY_VALUE, MESSAGE_DELIVERY_ANNOTATIONS, MESSAGE_FOOTER, MESSAGE_HEADER,
        MESSAGE_PROPERTIES,
    },
    header::Header,
    properties::Properties,
    types::{
        AmqpSequence, AmqpValue, ApplicationProperties, DeliveryAnnotations, Descriptor, Footer,
        MessageAnnotations, Value,
    },
    AmqpDecodeError,
};

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MessageSection {
    Header(Header),
    DeliveryAnnotations(DeliveryAnnotations),
    MessageAnnotations(MessageAnnotations),
    ApplicationProperties(ApplicationProperties),
    Data(Binary),
    AmqpSequence(AmqpSequence),
    AmqpValue(AmqpValue),
    Footer(Footer),
    Properties(Properties),
}

impl AmqpDecoder for MessageSection {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (remaining, described) = Descriptor::decode(input)?;

        match described {
            MESSAGE_HEADER => Header::decode(input).map_second(MessageSection::Header),
            MESSAGE_BODY_DATA => Binary::decode(remaining).map_second(MessageSection::Data),
            MESSAGE_PROPERTIES => Properties::decode(input).map_second(MessageSection::Properties),
            MESSAGE_BODY_VALUE => Value::decode(remaining).map_second(MessageSection::AmqpValue),
            MESSAGE_APPLICATION_PROPERTIES => ApplicationProperties::decode(remaining)
                .map_second(MessageSection::ApplicationProperties),
            MESSAGE_ANNOTATIONS => {
                MessageAnnotations::decode(remaining).map_second(MessageSection::MessageAnnotations)
            }
            MESSAGE_BODY_SEQUENCE => {
                AmqpSequence::decode(remaining).map_second(MessageSection::AmqpSequence)
            }
            MESSAGE_DELIVERY_ANNOTATIONS => DeliveryAnnotations::decode(remaining)
                .map_second(MessageSection::DeliveryAnnotations),
            MESSAGE_FOOTER => Footer::decode(remaining).map_second(MessageSection::Footer),
            descriptor => Err(AmqpDecodeError::MessageParse(format!(
                "Invalid section {:?}",
                descriptor
            ))),
        }
    }
}
