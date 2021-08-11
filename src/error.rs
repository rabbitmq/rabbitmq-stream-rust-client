use rabbitmq_stream_protocol::error::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RabbitMqStreamError {
    #[error("Io Error")]
    Io(#[from] std::io::Error),
    #[error("Protocol Error")]
    Protocol(#[from] ProtocolError),
    #[error("Cast Error")]
    CastError(String),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Encode Error")]
    Encode(EncodeError),
    #[error("Decode Error")]
    Decode(DecodeError),
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
