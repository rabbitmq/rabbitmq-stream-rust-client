use rabbitmq_stream_protocol::error::{DecodeError, EncodeError};

#[derive(Debug)]
pub enum RabbitMqStreamError {
    Io(std::io::Error),
    Protocol(ProtocolError),
    CastError(String),
}

#[derive(Debug)]
pub enum ProtocolError {
    Encode(EncodeError),
    Decode(DecodeError),
}

impl From<std::io::Error> for RabbitMqStreamError {
    fn from(err: std::io::Error) -> Self {
        RabbitMqStreamError::Io(err)
    }
}

impl From<EncodeError> for RabbitMqStreamError {
    fn from(err: EncodeError) -> Self {
        RabbitMqStreamError::Protocol(ProtocolError::Encode(err))
    }
}

impl From<DecodeError> for RabbitMqStreamError {
    fn from(err: DecodeError) -> Self {
        RabbitMqStreamError::Protocol(ProtocolError::Decode(err))
    }
}
