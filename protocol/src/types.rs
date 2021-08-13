#[derive(Debug, PartialEq)]
pub struct Header {
    key: u16,
    version: u16,
}

impl Header {
    pub fn new(key: u16, version: u16) -> Self {
        Self { key, version }
    }

    /// Get a reference to the request header's version.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Get a reference to the request header's key.
    pub fn key(&self) -> u16 {
        self.key
    }
}

#[cfg(test)]
use fake::Fake;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq)]
pub struct PublishedMessage {
    pub(crate) publishing_id: u64,
    pub(crate) message: Vec<u8>,
}

impl PublishedMessage {
    pub fn new(publishing_id: u64, message: Vec<u8>) -> Self {
        Self {
            publishing_id,
            message,
        }
    }
}
