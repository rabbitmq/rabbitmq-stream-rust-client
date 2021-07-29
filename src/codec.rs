use bytes::{Buf, BufMut, BytesMut};
use rabbitmq_stream_protocol::{
    error::{DecodeError, EncodeError},
    Request, Response,
};
use tokio_util::codec::{Decoder as TokioDecoder, Encoder as TokioEncoder};

use rabbitmq_stream_protocol::codec::{Decoder, Encoder};

#[derive(Debug)]
pub(crate) struct RabbitMqStreamCodec {}

impl TokioDecoder for RabbitMqStreamCodec {
    type Item = Response;
    type Error = DecodeError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, DecodeError> {
        match Response::decode(&buf) {
            Ok((remaining, response)) => {
                let len = remaining.len();
                buf.advance(buf.len() - len);
                Ok(Some(response))
            }
            Err(DecodeError::Incomplete(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl TokioEncoder<Request> for RabbitMqStreamCodec {
    type Error = EncodeError;

    fn encode(&mut self, req: Request, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let len = req.encoded_size();
        buf.reserve(len as usize);
        let mut writer = buf.writer();
        req.encode(&mut writer)?;

        Ok(())
    }
}
