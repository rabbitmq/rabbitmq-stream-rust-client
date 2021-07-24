use std::{collections::HashMap, io::Write};

use crate::types::CorrelationId;

pub struct OpenRequest<'a> {
    correlation_id: CorrelationId,
    virtual_host: &'a str,
}

impl<'a> OpenRequest<'a> {
    fn serialize(&self) -> Vec<u8> {
        let mut vec = vec![];

        vec.write_all(&self.correlation_id.to_be_bytes()).unwrap();
        vec.write_all(&self.virtual_host.len().to_be_bytes())
            .unwrap();
        vec.write_all(&self.virtual_host.as_bytes()).unwrap();

        vec
    }
}

pub enum ResponseCode {
    Ok,
    StreamDoesNotExist,
    SubscriptionIdAlreadyExists,
    SubscriptionIdDoesNotExist,
    StreamAlreadyExists,
    StreamNotAvailable,
    SaslMechanismNotSupported,
    AuthenticationFailure,
    SaslError,
    SaslChallenge,
    AuthenticationFailureLoopback,
    VirtualHostAccessFailure,
    UnknownFrame,
    FrameTooLarge,
    InternalError,
    AccessRefused,
    PreconditionFailed,
    PublisherDoesNotExist,
}
pub struct OpenResponse {
    correlation_id: CorrelationId,
    response_code: ResponseCode,
    connection_properties: HashMap<String, String>,
}

impl OpenResponse {
    fn deserialize(buffer: &[u8]) -> OpenResponse {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::OpenRequest;

    #[test]
    fn open_request_ser_der() {
        let open = OpenRequest {
            correlation_id: 1.into(),
            virtual_host: "test",
        };

        let serialized = open.serialize();
    }
}
