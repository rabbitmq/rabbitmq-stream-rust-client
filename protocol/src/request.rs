use std::io::Write;

use crate::{
    codec::Encoder, commands::Command, error::EncodeError, protocol::version::PROTOCOL_VERSION,
    types::Header,
};

use byteorder::{BigEndian, WriteBytesExt};

#[derive(Debug, PartialEq)]
pub struct Request<T> {
    header: Header,
    command: T,
}

impl<T> Request<T>
where
    T: Encoder + Command,
{
    pub fn new(command: T) -> Request<T> {
        Request {
            header: Header::new(command.key(), PROTOCOL_VERSION),
            command,
        }
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
        codec::{read_u32, Decoder, Encoder},
        commands::open::OpenCommand,
        request::Header,
    };

    impl<T: Decoder> Decoder for Request<T> {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
            let (input, _) = read_u32(input)?;
            let (input, header) = Header::decode(input)?;
            let (input, command) = T::decode(input)?;

            Ok((input, Request { header, command }))
        }
    }
    use super::Request;
    #[test]
    fn open_request_test() {
        let request = Request::new(OpenCommand::new(1.into(), "test".into()));

        let mut buffer = vec![];
        let _ = request.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Request::<OpenCommand>::decode(&buffer).unwrap();

        assert_eq!(request, decoded);

        assert!(remaining.is_empty());
    }
}
