use rabbitmq_stream_protocol::message::Message;

pub trait BaseMessage {
    fn publishing_id(&self) -> Option<u64>;
    fn to_message(self) -> Message;
    fn filter_value(&self) -> Option<String>;
}

impl BaseMessage for Message {
    fn publishing_id(&self) -> Option<u64> {
        self.publishing_id().copied()
    }

    fn to_message(self) -> Message {
        self
    }

    fn filter_value(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct ClientMessage {
    publishing_id: u64,
    message: Message,
    filter_value: Option<String>,
}

impl ClientMessage {
    pub fn new(publishing_id: u64, message: Message, filter_value: Option<String>) -> Self {
        Self {
            publishing_id,
            message,
            filter_value,
        }
    }

    pub fn filter_value_extract(&mut self, filter_value_extractor: impl Fn(&Message) -> String) {
        self.filter_value = Some(filter_value_extractor(&self.message));
    }

    pub fn into_message(self) -> Message {
        self.message
    }
}

impl BaseMessage for ClientMessage {
    fn publishing_id(&self) -> Option<u64> {
        Some(self.publishing_id)
    }

    fn to_message(self) -> Message {
        self.message
    }

    fn filter_value(&self) -> Option<String> {
        self.filter_value.clone()
    }
}
