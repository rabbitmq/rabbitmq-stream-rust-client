use bytes::{Buf, BufMut, BytesMut};
use rabbitmq_stream_protocol::{error::DecodeError, Request, Response};
use tokio_util::codec::{Decoder as TokioDecoder, Encoder as TokioEncoder};

use rabbitmq_stream_protocol::codec::{Decoder, Encoder};

use crate::error::ClientError;

#[derive(Debug)]
pub(crate) struct RabbitMqStreamCodec {}

impl TokioDecoder for RabbitMqStreamCodec {
    type Item = Response;
    type Error = ClientError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, ClientError> {
        match Response::decode(buf) {
            Ok((remaining, response)) => {
                let len = remaining.len();
                buf.advance(buf.len() - len);
                Ok(Some(response))
            }
            Err(DecodeError::Incomplete(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl TokioEncoder<Request> for RabbitMqStreamCodec {
    type Error = ClientError;

    fn encode(&mut self, req: Request, buf: &mut BytesMut) -> Result<(), ClientError> {
        let len = req.encoded_size();
        buf.reserve(len as usize);
        let mut writer = buf.writer();
        req.encode(&mut writer)?;

        Ok(())
    }
}
