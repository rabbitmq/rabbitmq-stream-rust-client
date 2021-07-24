use std::io::Write;

use crate::{codec::Encoder, error::EncodeError, types::CorrelationId};

use byteorder::{BigEndian, WriteBytesExt};

pub struct RequestHeader {
    key: u16,
    version: u16,
    correlation_id: Option<CorrelationId>,
}

impl Encoder for RequestHeader {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u16::<BigEndian>(self.key)?;
        writer.write_u16::<BigEndian>(self.version)?;

        if let Some(correletion_id) = &self.correlation_id {
            writer.write_u32::<BigEndian>(**correletion_id)?;
        }
        Ok(())
    }
}

pub struct Request<T: Encoder> {
    header: RequestHeader,
    command: T,
}

impl<T: Encoder> Request<T> {
    pub fn new(
        key: u16,
        version: u16,
        correlation_id: Option<CorrelationId>,
        command: T,
    ) -> Request<T> {
        Request {
            header: RequestHeader {
                key,
                version,
                correlation_id,
            },
            command,
        }
    }
}

impl<T: Encoder> Encoder for Request<T> {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.header.encode(writer)?;
        self.command.encode(writer)?;
        Ok(())
    }
}
