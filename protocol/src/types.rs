use std::ops::Deref;

#[derive(Debug, PartialEq)]
pub struct CorrelationId(u32);

impl Deref for CorrelationId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<u32> for CorrelationId {
    fn from(correlation_id: u32) -> Self {
        CorrelationId(correlation_id)
    }
}

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
