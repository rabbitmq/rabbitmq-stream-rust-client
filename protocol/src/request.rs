use std::io::Write;

use crate::{
    codec::Encoder,
    commands::{Command, Correlated, NoCorrelated},
    error::EncodeError,
    protocol::version::PROTOCOL_VERSION,
    types::CorrelationId,
};

use byteorder::{BigEndian, WriteBytesExt};

#[derive(Debug, PartialEq)]
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

    fn encoded_size(&self) -> u32 {
        2 + 2 + self.correlation_id.as_ref().map(|_| 4).unwrap_or(0)
    }
}

#[derive(Debug, PartialEq)]
pub struct Request<T> {
    header: RequestHeader,
    command: T,
}

impl<T> Request<T>
where
    T: Encoder + Command,
    T: NoCorrelated,
{
    pub fn new(command: T) -> Request<T> {
        Self::new_internal(command, None)
    }
}

impl<T: Encoder + Command> Request<T> {
    fn new_internal(command: T, correlation_id: Option<CorrelationId>) -> Request<T> {
        Request {
            header: RequestHeader {
                key: command.key(),
                version: PROTOCOL_VERSION,
                correlation_id,
            },
            command,
        }
    }
}
impl<T> Request<T>
where
    T: Encoder + Command,
    T: Correlated,
{
    pub fn with_correlation_id(command: T, correlation_id: CorrelationId) -> Request<T> {
        Self::new_internal(command, Some(correlation_id))
    }
}

impl<T: Encoder> Encoder for Request<T> {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.encoded_size())?;
        self.header.encode(writer)?;
        self.command.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        self.header.encoded_size() + self.command.encoded_size()
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        codec::{read_u16, read_u32, Decoder, Encoder},
        commands::open::OpenCommand,
        request::RequestHeader,
    };

    impl Decoder for RequestHeader {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
            let (input, key) = read_u16(input)?;
            let (input, version) = read_u16(input)?;

            // TODO optional based on key
            let (input, correlaton_id) = read_u32(input)?;

            Ok((
                input,
                RequestHeader {
                    key,
                    version,
                    correlation_id: Some(correlaton_id.into()),
                },
            ))
        }
    }
    impl<T: Decoder> Decoder for Request<T> {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
            let (input, _) = read_u32(input)?;
            let (input, header) = RequestHeader::decode(input)?;
            let (input, command) = T::decode(input)?;

            Ok((input, Request { header, command }))
        }
    }
    use super::Request;
    #[test]
    fn correlated_request_test() {
        let request = Request::with_correlation_id(OpenCommand::new("test".into()), 10.into());

        let mut buffer = vec![];
        let _ = request.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Request::<OpenCommand>::decode(&buffer).unwrap();

        assert_eq!(request, decoded);

        assert!(remaining.is_empty());
    }
}
