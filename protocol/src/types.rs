#[derive(Debug, PartialEq, Eq)]
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

use crate::{message::Message, ResponseCode};

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq)]
pub struct PublishedMessage {
    pub(crate) publishing_id: u64,
    pub(crate) message: Message,
    #[cfg_attr(test, dummy(expr = "None"))]
    pub(crate) filter_value: Option<String>,
}

impl PublishedMessage {
    pub fn new(publishing_id: u64, message: Message, filter_value: Option<String>) -> Self {
        Self {
            publishing_id,
            message,
            filter_value,
        }
    }

    /// Get a reference to the published message's publishing id.
    pub fn publishing_id(&self) -> u64 {
        self.publishing_id
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(Debug, PartialEq, Eq)]
pub struct PublishingError {
    pub publishing_id: u64,
    pub error_code: ResponseCode,
}

impl PublishingError {
    pub fn new(publishing_id: u64, error_code: ResponseCode) -> Self {
        Self {
            publishing_id,
            error_code,
        }
    }
}
