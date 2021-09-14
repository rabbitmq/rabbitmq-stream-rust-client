use rabbitmq_stream_protocol::{
    error::{DecodeError, EncodeError},
    ResponseCode,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("Cast Error: {0}")]
    CastError(String),

    #[error(transparent)]
    GenericError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Encode Error {0:?}")]
    Encode(EncodeError),
    #[error("Decode Error {0:?}")]
    Decode(DecodeError),
}

impl From<EncodeError> for ClientError {
    fn from(err: EncodeError) -> Self {
        ClientError::Protocol(ProtocolError::Encode(err))
    }
}

impl From<DecodeError> for ClientError {
    fn from(err: DecodeError) -> Self {
        ClientError::Protocol(ProtocolError::Decode(err))
    }
}

#[derive(Error, Debug)]
pub enum StreamCreateError {
    #[error("Failed to create stream {stream} status: {status:?}")]
    CreateError {
        stream: String,
        status: ResponseCode,
    },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

#[derive(Error, Debug)]
pub enum StreamDeleteError {
    #[error("Failed to delete stream {stream} status: {status:?}")]
    DeleteError {
        stream: String,
        status: ResponseCode,
    },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

#[derive(Error, Debug)]
pub enum ProducerCreateError {
    #[error("Failed to create producer for stream {stream} status {status:?}")]
    CreateError {
        stream: String,
        status: ResponseCode,
    },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

#[derive(Error, Debug)]
pub enum ProducerPublishError {
    #[error("Failed to publish message for stream {stream} status {status:?}")]
    CreateError {
        stream: String,
        publisher_id: u8,
        status: ResponseCode,
    },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}
#[derive(Error, Debug)]
pub enum ProducerCloseError {
    #[error("Failed to close producer for stream {stream} status {status:?}")]
    CreateError {
        stream: String,
        status: ResponseCode,
    },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}
