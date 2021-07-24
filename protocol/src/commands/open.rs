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
    STREAM_DOES_NOT_EXIST,
    SUBSCRIPTION_ID_ALREADY_EXISTS,
    SUBSCRIPTION_ID_DOES_NOT_EXIST,
    STREAM_ALREADY_EXISTS,
    STREAM_NOT_AVAILABLE,
    SASL_MECHANISM_NOT_SUPPORTED,
    AUTHENTICATION_FAILURE,
    SASL_ERROR,
    SASL_CHALLENGE,
    AUTHENTICATION_FAILURE_LOOPBACK,
    VIRTUAL_HOST_ACCESS_FAILURE,
    UNKNOWN_FRAME,
    FRAME_TOO_LARGE,
    INTERNAL_ERROR,
    ACCESS_REFUSED,
    PRECONDITION_FAILED,
    PUBLISHER_DOES_NOT_EXIST,
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
