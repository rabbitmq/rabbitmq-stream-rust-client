use std::string::FromUtf8Error;

use crate::error::IncompleteError;

use super::codec::constants::TypeCode;

#[derive(Debug)]
pub enum AmqpEncodeError {
    Io(std::io::Error),
}

impl From<std::io::Error> for AmqpEncodeError {
    fn from(err: std::io::Error) -> Self {
        AmqpEncodeError::Io(err)
    }
}

#[derive(Debug)]
pub enum AmqpDecodeError {
    InvalidTypeCode(u8),
    InvalidTypeCodeFor { target: String, code: TypeCode },
    MessageParse(String),
    Incomplete(IncompleteError),
    Utf8Error(FromUtf8Error),
    UuidError(uuid::Error),
}

impl AmqpDecodeError {
    pub fn parse_error(msg: impl Into<String>) -> AmqpDecodeError {
        AmqpDecodeError::MessageParse(msg.into())
    }
}

impl From<IncompleteError> for AmqpDecodeError {
    fn from(err: IncompleteError) -> Self {
        AmqpDecodeError::Incomplete(err)
    }
}

impl From<FromUtf8Error> for AmqpDecodeError {
    fn from(err: FromUtf8Error) -> Self {
        AmqpDecodeError::Utf8Error(err)
    }
}
impl From<uuid::Error> for AmqpDecodeError {
    fn from(err: uuid::Error) -> Self {
        AmqpDecodeError::UuidError(err)
    }
}
