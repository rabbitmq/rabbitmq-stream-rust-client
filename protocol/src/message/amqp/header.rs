use byteorder::{BigEndian, WriteBytesExt};

use crate::message::amqp::{
    codec::constants::MESSAGE_HEADER,
    types::{Descriptor, UInt},
};

use super::{
    codec::constants::TypeCode,
    types::Milliseconds,
    types::{list_decoder, Boolean, List},
    AmqpDecodeError, AmqpDecoder, AmqpEncodeError, AmqpEncoder,
};
#[cfg(test)]
use fake::Fake;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct Header {
    pub durable: bool,
    pub priority: u8,
    pub ttl: Option<Milliseconds>,
    pub first_acquirer: bool,
    pub delivery_count: u32,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            durable: Default::default(),
            priority: 4,
            ttl: Default::default(),
            first_acquirer: Default::default(),
            delivery_count: 0,
        }
    }
}

impl Header {
    fn content_size(&self) -> u32 {
        self.durable.encoded_size()
            + self.priority.encoded_size()
            + self.ttl.encoded_size()
            + self.first_acquirer.encoded_size()
            + self.delivery_count.encoded_size()
    }
}

impl AmqpEncoder for Header {
    fn encoded_size(&self) -> u32 {
        let size = self.content_size() + MESSAGE_HEADER.encoded_size();
        let fixed = if size > u8::MAX as u32 { 9 } else { 3 };
        fixed + size
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        MESSAGE_HEADER.encode(writer)?;

        let content_size = self.content_size();

        if content_size + MESSAGE_HEADER.encoded_size() > u8::MAX as u32 {
            TypeCode::List8.encode(writer)?;
            writer.write_u32::<BigEndian>(content_size + 4)?;
            writer.write_u32::<BigEndian>(5)?;
        } else {
            TypeCode::List8.encode(writer)?;
            writer.write_u8((content_size + 4) as u8)?;
            writer.write_u8(5)?;
        }
        self.durable.encode(writer)?;
        self.priority.encode(writer)?;
        self.ttl.encode(writer)?;
        self.first_acquirer.encode(writer)?;
        self.delivery_count.encode(writer)?;
        Ok(())
    }
}

impl AmqpDecoder for Header {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match Descriptor::decode(input)? {
            (input, MESSAGE_HEADER) => {
                let header = Header::default();
                List::decode_with_fields(input, list_decoder_header, header)
            }
            (_, descriptor) => Err(AmqpDecodeError::MessageParse(format!(
                "Invalid descriptor for header {:?}",
                descriptor
            ))),
        }
    }
}

list_decoder!(Header, list_decoder_header,
    {
     0 => { durable, Boolean, false},
     1 => { priority, u8, 4},
     2 => { ttl, u32, None, true},
     3 => { first_acquirer, Boolean, false},
     4 => { delivery_count, UInt, 4}
    }
);

#[cfg(test)]
mod tests {
    use crate::message::amqp::tests::type_encode_decode_test_fuzzy;

    use super::Header;

    #[test]
    fn test_header_encode_decode() {
        type_encode_decode_test_fuzzy::<Header>()
    }
}
