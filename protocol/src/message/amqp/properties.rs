use byteorder::{BigEndian, WriteBytesExt};

use crate::message::amqp::codec::constants::TypeCode;
use crate::message::amqp::codec::constants::MESSAGE_PROPERTIES;
use crate::message::amqp::types::list_decoder;
use crate::message::amqp::types::Binary;
use crate::message::amqp::types::Descriptor;
use crate::message::amqp::types::List;
use crate::message::amqp::types::Str;
use crate::message::amqp::AmqpDecodeError;

use super::AmqpDecoder;
use super::{
    types::{Address, MessageId, SequenceNo, Symbol, Timestamp},
    AmqpEncodeError, AmqpEncoder,
};

#[cfg(test)]
use fake::Fake;

/// Properties of the message
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct Properties {
    pub message_id: Option<MessageId>,
    pub user_id: Option<Vec<u8>>,
    pub to: Option<Address>,
    pub subject: Option<String>,
    pub reply_to: Option<Address>,
    pub correlation_id: Option<MessageId>,
    pub content_type: Option<Symbol>,
    pub content_encoding: Option<Symbol>,
    pub absolute_expiry_time: Option<Timestamp>,
    pub creation_time: Option<Timestamp>,
    pub group_id: Option<String>,
    pub group_sequence: Option<SequenceNo>,
    pub reply_to_group_id: Option<String>,
}

impl Properties {
    fn content_size(&self) -> u32 {
        self.message_id.encoded_size()
            + self.user_id.encoded_size()
            + self.to.encoded_size()
            + self.subject.encoded_size()
            + self.reply_to.encoded_size()
            + self.correlation_id.encoded_size()
            + self.content_type.encoded_size()
            + self.content_encoding.encoded_size()
            + self.absolute_expiry_time.encoded_size()
            + self.creation_time.encoded_size()
            + self.group_id.encoded_size()
            + self.group_sequence.encoded_size()
            + self.reply_to_group_id.encoded_size()
    }
}
impl AmqpEncoder for Properties {
    fn encoded_size(&self) -> u32 {
        let size = MESSAGE_PROPERTIES.encoded_size() + self.content_size();

        let header = if size > u8::MAX as u32 { 9 } else { 3 };

        header + size
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        MESSAGE_PROPERTIES.encode(writer)?;

        let content_size = self.content_size();

        if content_size + 1 > u8::MAX as u32 {
            TypeCode::List8.encode(writer)?;
            writer.write_u32::<BigEndian>(content_size + 4)?;
            writer.write_u32::<BigEndian>(13)?;
        } else {
            TypeCode::List8.encode(writer)?;
            writer.write_u8((content_size + 1) as u8)?;
            writer.write_u8(13)?;
        }
        self.message_id.encode(writer)?;
        self.user_id.encode(writer)?;
        self.to.encode(writer)?;
        self.subject.encode(writer)?;
        self.reply_to.encode(writer)?;
        self.correlation_id.encode(writer)?;
        self.content_type.encode(writer)?;
        self.content_encoding.encode(writer)?;
        self.absolute_expiry_time.encode(writer)?;
        self.creation_time.encode(writer)?;
        self.group_id.encode(writer)?;
        self.group_sequence.encode(writer)?;
        self.reply_to_group_id.encode(writer)?;

        Ok(())
    }
}
impl AmqpDecoder for Properties {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), super::AmqpDecodeError> {
        match Descriptor::decode(input)? {
            (input, MESSAGE_PROPERTIES) => {
                let properties = Properties::default();
                List::decode_with_fields(input, list_decoder_properties, properties)
            }
            (_, descriptor) => Err(AmqpDecodeError::MessageParse(format!(
                "Invalid descriptor for properties {:?}",
                descriptor
            ))),
        }
    }
}

list_decoder!(Properties, list_decoder_properties,
    {
     0 => { message_id, MessageId, None, true},
     1 => { user_id, Binary, None, true},
     2 => { to, Address, None, true},
     3 => { subject, Str, None, true},
     4 => { reply_to, Address, None, true},
     5 => { correlation_id, MessageId, None, true},
     6 => { content_type, Symbol, None, true},
     7 => { content_encoding, Symbol, None, true},
     8 => { absolute_expiry_time, Timestamp, None, true},
     9 => { creation_time, Timestamp, None, true},
     10 => { group_id, Str, None, true},
     11 => { group_sequence, SequenceNo, None, true},
     12 => { reply_to_group_id, Str, None, true}
    }
);

#[cfg(test)]
mod tests {
    use crate::message::amqp::tests::type_encode_decode_test_fuzzy;

    use super::Properties;

    #[test]
    fn test_properties_encode_decode() {
        type_encode_decode_test_fuzzy::<Properties>()
    }
}
