use rabbitmq_stream_protocol::error::{DecodeError, EncodeError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RabbitMqStreamError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("Cast Error: {0}")]
    CastError(String),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Encode Error {0:?}")]
    Encode(EncodeError),
    #[error("Decode Error {0:?}")]
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
