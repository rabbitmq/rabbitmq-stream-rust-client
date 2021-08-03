use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum DecodeError {
    Incomplete(usize),
    Utf8Error(FromUtf8Error),
    UnknownResponseCode(u16),
    UsupportedResponseType(u16),
    MismatchSize(usize),
    Empty,
}

#[derive(Debug)]
pub enum EncodeError {
    Io(std::io::Error),
    MaxSizeError(usize),
}

impl From<std::io::Error> for EncodeError {
    fn from(err: std::io::Error) -> Self {
        EncodeError::Io(err)
    }
}

impl From<FromUtf8Error> for DecodeError {
    fn from(err: FromUtf8Error) -> Self {
        DecodeError::Utf8Error(err)
    }
}
