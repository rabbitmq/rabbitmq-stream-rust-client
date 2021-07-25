#[derive(Debug)]
pub enum DecodeError {
    Incomplete(usize),
}

#[derive(Debug)]
pub enum EncodeError {
    Io(std::io::Error),
}

impl From<std::io::Error> for EncodeError {
    fn from(err: std::io::Error) -> Self {
        EncodeError::Io(err)
    }
}
