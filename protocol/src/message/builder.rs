use std::sync::Arc;

use super::{InternalMessage, Message};

use super::amqp::{
    Address, AnnonationKey, Annotations, MessageId, SequenceNo, SimpleValue, Str, Symbol,
    Timestamp, Value,
};
pub struct MessageBuilder(pub(crate) InternalMessage);

impl MessageBuilder {
    pub fn body(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.0.message.set_body(|body| {
            body.set_data(data);
        });
        self
    }

    pub fn properties(self) -> PropertiesBuilder {
        PropertiesBuilder(self)
    }

    pub fn message_annotations(self) -> AnnotationBuider {
        AnnotationBuider(
            self,
            Box::new(|builder| builder.0.message.message_annotations_mut()),
        )
    }

    pub fn application_properties(self) -> ApplicationPropertiesBuider {
        ApplicationPropertiesBuider(self)
    }
    pub fn publising_id(mut self, publishing_id: u64) -> Self {
        self.0.publishing_id = Some(publishing_id);
        self
    }
    pub fn build(self) -> Message {
        Message(Arc::new(self.0))
    }
}

pub struct PropertiesBuilder(MessageBuilder);

impl PropertiesBuilder {
    pub fn message_builder(self) -> MessageBuilder {
        self.0
    }
    pub fn message_id(mut self, message_id: impl Into<MessageId>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.message_id = Some(message_id.into()));
        self
    }
    pub fn user_id(mut self, user_id: impl Into<Vec<u8>>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.user_id = Some(user_id.into()));
        self
    }
    pub fn to(mut self, address: impl Into<Address>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.to = Some(address.into()));
        self
    }
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.subject = Some(subject.into()));
        self
    }
    pub fn reply_to(mut self, address: impl Into<Address>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.reply_to = Some(address.into()));
        self
    }
    pub fn correlation_id(mut self, correlation_id: impl Into<MessageId>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.correlation_id = Some(correlation_id.into()));
        self
    }
    pub fn content_type(mut self, content_type: impl Into<Symbol>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.content_type = Some(content_type.into()));
        self
    }
    pub fn content_encoding(mut self, content_encoding: impl Into<Symbol>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.content_encoding = Some(content_encoding.into()));
        self
    }
    pub fn absolute_expiry_time(mut self, expiry_time: impl Into<Timestamp>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.absolute_expiry_time = Some(expiry_time.into()));
        self
    }
    pub fn creation_time(mut self, creation_time: impl Into<Timestamp>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.creation_time = Some(creation_time.into()));
        self
    }
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.group_id = Some(group_id.into()));
        self
    }
    pub fn group_sequence(mut self, group_sequence: SequenceNo) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.group_sequence = Some(group_sequence));
        self
    }
    pub fn reply_to_group_id(mut self, reply_to_group_id: impl Into<String>) -> Self {
        self.0
             .0
            .message
            .with_properties(|p| p.reply_to_group_id = Some(reply_to_group_id.into()));
        self
    }
}

pub struct AnnotationBuider(
    MessageBuilder,
    Box<dyn Fn(&mut MessageBuilder) -> &mut Annotations>,
);

impl AnnotationBuider {
    pub fn insert<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<AnnonationKey>,
        V: Into<Value>,
    {
        let annotations = self.1(&mut self.0);
        annotations.put(key, value);
        self
    }
    pub fn message_builder(self) -> MessageBuilder {
        self.0
    }
}

pub struct ApplicationPropertiesBuider(MessageBuilder);

impl ApplicationPropertiesBuider {
    pub fn insert<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<Str>,
        V: Into<SimpleValue>,
    {
        let app_properties = self.0 .0.message.application_properties_mut();
        app_properties.0.insert(key.into(), value.into());
        self
    }
    pub fn message_builder(self) -> MessageBuilder {
        self.0
    }
}
