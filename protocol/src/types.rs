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
