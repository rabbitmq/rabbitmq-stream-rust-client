use super::body::MessageBody;
use super::codec::constants::{
    MESSAGE_ANNOTATIONS, MESSAGE_APPLICATION_PROPERTIES, MESSAGE_DELIVERY_ANNOTATIONS,
    MESSAGE_FOOTER,
};
use super::codec::AmqpEncoder;
use super::error::AmqpEncodeError;
use super::header::Header;
use super::properties::Properties;
use super::section::MessageSection;
use super::types::{ApplicationProperties, DeliveryAnnotations, Footer, MessageAnnotations};
use super::{AmqpDecodeError, AmqpDecoder};

#[cfg(test)]
use fake::Fake;

#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(test, derive(fake::Dummy))]
pub struct Message {
    header: Option<Header>,
    delivery_annotations: Option<DeliveryAnnotations>,
    message_annotations: Option<MessageAnnotations>,
    properties: Option<Properties>,
    application_properties: Option<ApplicationProperties>,
    footer: Option<Footer>,
    body: MessageBody,
}

impl Message {
    /// Message body
    pub fn body(&self) -> &MessageBody {
        &self.body
    }

    pub fn set_body<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut MessageBody),
    {
        f(&mut self.body);
        self
    }

    pub fn with_header<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut Header),
    {
        if self.header.is_none() {
            self.header = Some(Header::default());
        }

        if let Some(ref mut header) = self.header.as_mut() {
            f(header);
        }

        self
    }

    pub fn footer_mut(&mut self) -> &mut Footer {
        if self.footer.is_none() {
            self.footer = Some(Footer::default());
        }
        self.footer.as_mut().unwrap()
    }
    pub fn delivery_annotations_mut(&mut self) -> &mut DeliveryAnnotations {
        if self.delivery_annotations.is_none() {
            self.delivery_annotations = Some(DeliveryAnnotations::default());
        }
        self.delivery_annotations.as_mut().unwrap()
    }
    pub fn message_annotations_mut(&mut self) -> &mut MessageAnnotations {
        if self.message_annotations.is_none() {
            self.message_annotations = Some(MessageAnnotations::default());
        }
        self.message_annotations.as_mut().unwrap()
    }
    #[cfg(test)]
    pub fn with_footer<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut Footer),
    {
        if self.footer.is_none() {
            self.footer = Some(Footer::default());
        }

        if let Some(ref mut footer) = self.footer.as_mut() {
            f(footer)
        }

        self
    }

    #[cfg(test)]
    pub(crate) fn set_properties(&mut self, properties: Properties) -> &mut Self {
        self.properties = Some(properties);
        self
    }
    pub fn with_properties<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut Properties),
    {
        if self.properties.is_none() {
            self.properties = Some(Properties::default());
        }

        if let Some(ref mut properties) = self.properties.as_mut() {
            f(properties);
        }

        self
    }

    /// Get a reference to the message's application properties.
    #[must_use]
    pub fn application_properties(&self) -> Option<&ApplicationProperties> {
        self.application_properties.as_ref()
    }

    /// Get a reference to the message's properties.
    #[must_use]
    pub fn properties(&self) -> Option<&Properties> {
        self.properties.as_ref()
    }

    /// Get a reference to the message's header.
    #[must_use]
    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }
}

impl AmqpDecoder for Message {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let mut message = Message::default();
        let mut input = input;
        loop {
            if input.is_empty() {
                break;
            }
            let (inner, sec) = MessageSection::decode(input)?;
            input = inner;
            match sec {
                MessageSection::Header(val) => {
                    message.header = Some(val);
                }
                MessageSection::DeliveryAnnotations(val) => {
                    message.delivery_annotations = Some(val);
                }
                MessageSection::MessageAnnotations(val) => {
                    message.message_annotations = Some(val);
                }
                MessageSection::ApplicationProperties(val) => {
                    message.application_properties = Some(val);
                }
                MessageSection::Footer(val) => {
                    message.footer = Some(val);
                }
                MessageSection::Properties(val) => {
                    message.properties = Some(val);
                }
                // body
                MessageSection::AmqpSequence(val) => {
                    message.body.sequence.push(val);
                }
                MessageSection::AmqpValue(val) => {
                    message.body.value = Some(val);
                }
                MessageSection::Data(val) => {
                    message.body.data.push(val);
                }
            }
        }
        Ok((input, message))
    }
}

impl AmqpEncoder for Message {
    fn encoded_size(&self) -> u32 {
        let mut size = self.body.encoded_size();

        if let Some(ref h) = self.header {
            size += h.encoded_size();
        }
        if let Some(ref da) = self.delivery_annotations {
            size += da.encoded_size() + MESSAGE_DELIVERY_ANNOTATIONS.encoded_size();
        }
        if let Some(ref ma) = self.message_annotations {
            size += ma.encoded_size() + MESSAGE_ANNOTATIONS.encoded_size();
        }
        if let Some(ref p) = self.properties {
            size += p.encoded_size();
        }
        if let Some(ref ap) = self.application_properties {
            size += ap.encoded_size() + MESSAGE_APPLICATION_PROPERTIES.encoded_size();
        }
        if let Some(ref f) = self.footer {
            size += f.encoded_size() + MESSAGE_FOOTER.encoded_size();
        }

        size as u32
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        if let Some(ref h) = self.header {
            h.encode(writer)?;
        }
        if let Some(ref da) = self.delivery_annotations {
            MESSAGE_DELIVERY_ANNOTATIONS.encode(writer)?;
            da.encode(writer)?;
        }
        if let Some(ref ma) = self.message_annotations {
            MESSAGE_ANNOTATIONS.encode(writer)?;
            ma.encode(writer)?;
        }
        if let Some(ref p) = self.properties {
            p.encode(writer)?;
        }
        if let Some(ref ap) = self.application_properties {
            MESSAGE_APPLICATION_PROPERTIES.encode(writer)?;
            ap.encode(writer)?;
        }

        self.body.encode(writer)?;

        if let Some(ref f) = self.footer {
            MESSAGE_FOOTER.encode(writer)?;
            f.encode(writer)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use fake::Fake;
    use fake::Faker;

    use super::Message;
    use crate::message::amqp::codec::AmqpDecoder;
    use crate::message::amqp::codec::AmqpEncoder;
    use crate::message::amqp::tests::type_encode_decode_test_fuzzy;
    use crate::message::amqp::types::List;

    #[test]
    fn message_with_body_test() {
        let mut message = Message::default();

        message.set_body(|body| {
            body.set_data(b"test:w".as_ref());
        });

        let mut buf = vec![];
        message.encode(&mut buf).unwrap();

        let (remaining, decoded) = Message::decode(&buf).unwrap();

        assert_eq!(0, remaining.len());
        assert_eq!(message, decoded);
    }
    #[test]
    fn message_with_header_test() {
        let mut message = Message::default();
        message
            .set_body(|body| {
                body.set_data(b"test:w".as_ref());
            })
            .with_header(|header| {
                header.ttl = Some(3000);
                header.delivery_count = 32;
                header.first_acquirer = true;
            });

        let mut buf = vec![];
        message.encode(&mut buf).unwrap();

        let (remaining, decoded) = Message::decode(&buf).unwrap();

        assert_eq!(0, remaining.len());
        assert_eq!(message, decoded);
    }
    #[test]
    fn message_with_properties_test() {
        let mut message = Message::default();
        message
            .set_body(|body| {
                body.set_data(b"test:w".as_ref());
            })
            .set_properties(Faker.fake());

        let mut buf = vec![];
        message.encode(&mut buf).unwrap();

        let (remaining, decoded) = match Message::decode(&buf) {
            Ok(result) => result,
            Err(err) => panic!(
                "Failed to decode message {:#?} with error : {:?}",
                message, err
            ),
        };

        assert_eq!(0, remaining.len());
        assert_eq!(message, decoded);
    }
    #[test]
    fn message_with_footer_test() {
        let mut message = Message::default();
        message
            .set_body(|body| {
                body.set_data(b"test:w".as_ref());
            })
            .with_footer(|footer| {
                let mut list = List::new();

                list.push(1);
                list.push("test");
                footer.put("test", 1);
                footer.put(
                    (0..300).map(|idx| format!("{}", idx)).collect::<String>(),
                    1,
                );
                footer.put(1, "test");
                footer.put(1000, "test");
                footer.put("list", list);
            });

        check_message_encode_decode(&message);
    }
    #[test]
    fn message_with_body_value_test() {
        let mut message = Message::default();
        message.set_body(|body| {
            body.set_data(b"test:w".as_ref()).set_value(10);
        });

        check_message_encode_decode(&message);
    }

    #[test]
    fn test_message_encode_decode() {
        type_encode_decode_test_fuzzy::<Message>()
    }
    #[test]
    fn test_message_empty() {
        let content = include_bytes!("fixtures/empty_message");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert_eq!(message.body().data(), Some(&vec![]));
    }
    #[test]
    fn test_message_header_amqp_value() {
        let content = include_bytes!("fixtures/header_amqpvalue_message");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        let header = message.header().unwrap();

        assert_eq!(true, header.durable);
        assert_eq!(true, header.first_acquirer);
        assert_eq!(100, header.priority);
        assert_eq!(300, header.delivery_count);
        assert_eq!(Some(0), header.ttl);

        assert!(message.body().data().is_some());
    }
    #[test]
    fn test_message_random_application_properties_300() {
        let content = include_bytes!("fixtures/message_random_application_properties_300");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert!(message.application_properties().unwrap().len() > 0);

        assert!(message.body().data().unwrap().len() > u8::MAX as usize);
    }
    #[test]
    fn test_message_random_application_properties_500() {
        let content = include_bytes!("fixtures/message_random_application_properties_500");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert!(message.application_properties().unwrap().len() > 0);

        assert!(message.body().data().unwrap().len() > u8::MAX as usize);
    }
    #[test]
    fn test_message_random_application_properties_properties_900() {
        let content =
            include_bytes!("fixtures/message_random_application_properties_properties_900");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert!(message.application_properties().unwrap().len() > 0);
        assert!(message.body().data().unwrap().len() > u8::MAX as usize);

        for (k, v) in message.application_properties().unwrap().iter() {
            assert!(k.len() >= 900);
            let v: &String = v.try_into().unwrap();
            assert!(v.len() >= 900);
        }

        let properties = message.properties().unwrap();

        assert!(properties.reply_to.as_ref().unwrap().len() > 0);
        assert!(properties.content_encoding.as_ref().unwrap().len() > 0);
        assert!(properties.content_type.as_ref().unwrap().len() > 0);
        assert!(properties.group_id.as_ref().unwrap().len() > 0);
        assert!(properties.reply_to.as_ref().unwrap().len() > 0);

        assert!(properties.absolute_expiry_time.is_some());
        assert!(properties.creation_time.is_some());
    }
    #[test]
    fn test_message_static_test_compare() {
        let content = include_bytes!("fixtures/static_test_message_compare");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert_eq!(message.body().data(), Some(&vec![]));
    }
    #[test]
    fn test_message_body_250() {
        let content = include_bytes!("fixtures/message_body_250");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert!(message.body().data().unwrap().len() <= u8::MAX as usize);
    }

    #[test]
    fn test_message_body_700() {
        let content = include_bytes!("fixtures/message_body_700");

        let (remaining, message) = Message::decode(content).unwrap();

        assert!(remaining.is_empty());

        assert!(message.body().data().unwrap().len() > u8::MAX as usize);
    }
    fn check_message_encode_decode(message: &Message) {
        let mut buf = vec![];
        message.encode(&mut buf).unwrap();

        let (remaining, decoded) = match Message::decode(&buf) {
            Ok(result) => result,
            Err(err) => panic!(
                "Failed to decode message {:#?} with error : {:?}",
                message, err
            ),
        };

        assert_eq!(0, remaining.len());
        assert_eq!(message, &decoded);
    }
}
