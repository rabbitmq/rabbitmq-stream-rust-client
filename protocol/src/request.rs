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
        codec::{decoder::read_u32, Decoder, Encoder},
        commands::{
            create_stream::CreateStreamCommand, delete::Delete, open::OpenCommand,
            peer_properties::PeerPropertiesCommand, sasl_authenticate::SaslAuthenticateCommand,
            sasl_handshake::SaslHandshakeCommand, tune::TunesCommand, Command,
        },
        request::Header,
    };

    use std::fmt::Debug;

    use fake::{Dummy, Fake, Faker};

    use super::Request;

    impl<T: Decoder> Decoder for Request<T> {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
            let (input, _) = read_u32(input)?;
            let (input, header) = Header::decode(input)?;
            let (input, command) = T::decode(input)?;

            Ok((input, Request { header, command }))
        }
    }

    #[test]
    fn request_open_test() {
        request_encode_decode_test::<OpenCommand>()
    }

    #[test]
    fn request_peer_properties_test() {
        request_encode_decode_test::<PeerPropertiesCommand>()
    }

    #[test]
    fn request_create_stream_test() {
        request_encode_decode_test::<CreateStreamCommand>()
    }

    #[test]
    fn request_delete_stream_test() {
        request_encode_decode_test::<Delete>()
    }

    #[test]
    fn request_sasl_authenticate_test() {
        request_encode_decode_test::<SaslAuthenticateCommand>()
    }

    #[test]
    fn request_sasl_handshake_test() {
        request_encode_decode_test::<SaslHandshakeCommand>()
    }

    #[test]
    fn request_tune_test() {
        request_encode_decode_test::<TunesCommand>()
    }

    fn request_encode_decode_test<T>()
    where
        T: Dummy<Faker> + Encoder + Decoder + Debug + PartialEq + Command,
    {
        let command: T = Faker.fake();

        let request = Request::new(command);

        let mut buffer = vec![];
        let _ = request.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Request::<T>::decode(&buffer).unwrap();

        assert_eq!(buffer[4..].len(), decoded.encoded_size() as usize);

        assert_eq!(request, decoded);

        assert!(remaining.is_empty());
    }
}
