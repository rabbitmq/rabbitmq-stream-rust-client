use std::io::Write;

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_ROUTE,
    FromResponse, ResponseCode,
};

use super::Command;

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct SuperStreamRouteRequest {
    correlation_id: u32,
    routing_key: String,
    super_stream: String,
}

impl SuperStreamRouteRequest {
    pub fn new(correlation_id: u32, routing_key: String, super_stream: String) -> Self {
        Self {
            correlation_id,
            routing_key,
            super_stream,
        }
    }
}

impl Encoder for SuperStreamRouteRequest {
    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.routing_key.as_str().encoded_size()
            + self.super_stream.as_str().encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.routing_key.as_str().encode(writer)?;
        self.super_stream.as_str().encode(writer)?;
        Ok(())
    }
}

impl Decoder for SuperStreamRouteRequest {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, routing_key) = Option::decode(input)?;
        let (input, super_stream) = Option::decode(input)?;

        Ok((
            input,
            SuperStreamRouteRequest {
                correlation_id,
                routing_key: routing_key.unwrap(),
                super_stream: super_stream.unwrap(),
            },
        ))
    }
}

impl Command for SuperStreamRouteRequest {
    fn key(&self) -> u16 {
        COMMAND_ROUTE
    }
}

#[cfg_attr(test, derive(fake::Dummy))]
#[derive(PartialEq, Eq, Debug)]
pub struct SuperStreamRouteResponse {
    pub(crate) correlation_id: u32,
    response_code: ResponseCode,
    pub streams: Vec<String>,
}

impl SuperStreamRouteResponse {
    pub fn new(correlation_id: u32, streams: Vec<String>, response_code: ResponseCode) -> Self {
        Self {
            correlation_id,
            response_code,
            streams,
        }
    }
    pub fn is_ok(&self) -> bool {
        self.response_code == ResponseCode::Ok
    }
}

impl Encoder for SuperStreamRouteResponse {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.correlation_id.encode(writer)?;
        self.response_code.encode(writer)?;
        self.streams.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.correlation_id.encoded_size()
            + self.streams.encoded_size()
            + self.response_code.encoded_size()
    }
}

impl Decoder for SuperStreamRouteResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (input, correlation_id) = u32::decode(input)?;
        let (input, response_code) = ResponseCode::decode(input)?;
        let (input, streams) = Vec::decode(input)?;

        Ok((
            input,
            SuperStreamRouteResponse {
                correlation_id,
                response_code,
                streams,
            },
        ))
    }
}

impl FromResponse for SuperStreamRouteResponse {
    fn from_response(response: crate::Response) -> Option<Self> {
        match response.kind {
            crate::ResponseKind::SuperStreamRoute(route) => Some(route),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::commands::tests::command_encode_decode_test;

    use super::SuperStreamRouteRequest;
    use super::SuperStreamRouteResponse;

    #[test]
    fn super_stream_route_request_test() {
        command_encode_decode_test::<SuperStreamRouteRequest>();
    }

    #[test]
    fn super_stream_route_response_test() {
        command_encode_decode_test::<SuperStreamRouteResponse>();
    }
}
