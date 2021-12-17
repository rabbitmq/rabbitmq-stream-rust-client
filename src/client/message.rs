use rabbitmq_stream_protocol::message::Message;

pub trait BaseMessage {
    fn publishing_id(&self) -> Option<u64>;
    fn to_message(self) -> Message;
}

impl BaseMessage for Message {
    fn publishing_id(&self) -> Option<u64> {
        self.publishing_id().cloned()
    }

    fn to_message(self) -> Message {
        self
    }
}

#[derive(Debug)]
pub struct ClientMessage {
    publishing_id: u64,
    message: Message,
}

impl ClientMessage {
    pub fn new(publishing_id: u64, message: Message) -> Self {
        Self {
            publishing_id,
            message,
        }
    }
}

impl BaseMessage for ClientMessage {
    fn publishing_id(&self) -> Option<u64> {
        Some(self.publishing_id)
    }

    fn to_message(self) -> Message {
        self.message
    }
}
