use std::io::Write;

use crate::{
    codec::{decoder::read_u32, Decoder, Encoder},
    commands::{
        create_stream::CreateStreamCommand, delete::Delete, open::OpenCommand,
        peer_properties::PeerPropertiesCommand, sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::SaslHandshakeCommand, tune::TunesCommand, Command,
    },
    error::{DecodeError, EncodeError},
    protocol::commands::{
        COMMAND_CREATE_STREAM, COMMAND_DELETE_STREAM, COMMAND_OPEN, COMMAND_PEER_PROPERTIES,
        COMMAND_SASL_AUTHENTICATE, COMMAND_SASL_HANDSHAKE, COMMAND_TUNE,
    },
    protocol::version::PROTOCOL_VERSION,
    types::Header,
};

use byteorder::{BigEndian, WriteBytesExt};

#[derive(Debug, PartialEq)]
pub struct Request {
    header: Header,
    kind: RequestKind,
}
#[derive(Debug, PartialEq)]
pub enum RequestKind {
    PeerProperties(PeerPropertiesCommand),
    SaslHandshake(SaslHandshakeCommand),
    SaslAuthenticate(SaslAuthenticateCommand),
    Tunes(TunesCommand),
    Open(OpenCommand),
    Delete(Delete),
    CreateStream(CreateStreamCommand),
}

impl Encoder for RequestKind {
    fn encoded_size(&self) -> u32 {
        match self {
            RequestKind::PeerProperties(peer) => peer.encoded_size(),
            RequestKind::SaslHandshake(handshake) => handshake.encoded_size(),
            RequestKind::SaslAuthenticate(authenticate) => authenticate.encoded_size(),
            RequestKind::Tunes(tunes) => tunes.encoded_size(),
            RequestKind::Open(open) => open.encoded_size(),
            RequestKind::Delete(delete) => delete.encoded_size(),
            RequestKind::CreateStream(create_stream) => create_stream.encoded_size(),
        }
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        match self {
            RequestKind::PeerProperties(peer) => peer.encode(writer),
            RequestKind::SaslHandshake(handshake) => handshake.encode(writer),
            RequestKind::SaslAuthenticate(authenticate) => authenticate.encode(writer),
            RequestKind::Tunes(tunes) => tunes.encode(writer),
            RequestKind::Open(open) => open.encode(writer),
            RequestKind::Delete(delete) => delete.encode(writer),
            RequestKind::CreateStream(create_stream) => create_stream.encode(writer),
        }
    }
}
impl Encoder for Request {
    fn encoded_size(&self) -> u32 {
        self.header.encoded_size() + self.kind.encoded_size()
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.encoded_size())?;
        self.header.encode(writer)?;
        self.kind.encode(writer)?;
        Ok(())
    }
}

impl<T> From<T> for Request
where
    T: Into<RequestKind> + Command,
{
    fn from(cmd: T) -> Self {
        Request {
            header: Header::new(cmd.key(), PROTOCOL_VERSION),
            kind: cmd.into(),
        }
    }
}

impl From<PeerPropertiesCommand> for RequestKind {
    fn from(cmd: PeerPropertiesCommand) -> Self {
        RequestKind::PeerProperties(cmd)
    }
}

impl From<OpenCommand> for RequestKind {
    fn from(cmd: OpenCommand) -> Self {
        RequestKind::Open(cmd)
    }
}
impl From<SaslHandshakeCommand> for RequestKind {
    fn from(cmd: SaslHandshakeCommand) -> Self {
        RequestKind::SaslHandshake(cmd)
    }
}
impl From<SaslAuthenticateCommand> for RequestKind {
    fn from(cmd: SaslAuthenticateCommand) -> Self {
        RequestKind::SaslAuthenticate(cmd)
    }
}

impl From<TunesCommand> for RequestKind {
    fn from(cmd: TunesCommand) -> Self {
        RequestKind::Tunes(cmd)
    }
}

impl From<CreateStreamCommand> for RequestKind {
    fn from(cmd: CreateStreamCommand) -> Self {
        RequestKind::CreateStream(cmd)
    }
}
impl From<Delete> for RequestKind {
    fn from(cmd: Delete) -> Self {
        RequestKind::Delete(cmd)
    }
}
impl Decoder for Request {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), crate::error::DecodeError> {
        let (input, _) = read_u32(input)?;
        let (input, header) = Header::decode(input)?;

        let (input, cmd) = match header.key() {
            COMMAND_OPEN => OpenCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_PEER_PROPERTIES => {
                PeerPropertiesCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_SASL_HANDSHAKE => {
                SaslHandshakeCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_SASL_AUTHENTICATE => {
                SaslAuthenticateCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            COMMAND_TUNE => TunesCommand::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_DELETE_STREAM => Delete::decode(input).map(|(i, kind)| (i, kind.into()))?,
            COMMAND_CREATE_STREAM => {
                CreateStreamCommand::decode(input).map(|(i, kind)| (i, kind.into()))?
            }
            n => return Err(DecodeError::UsupportedResponseType(n)),
        };
        Ok((input, Request { header, kind: cmd }))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        codec::{Decoder, Encoder},
        commands::{
            create_stream::CreateStreamCommand, delete::Delete, open::OpenCommand,
            peer_properties::PeerPropertiesCommand, sasl_authenticate::SaslAuthenticateCommand,
            sasl_handshake::SaslHandshakeCommand, tune::TunesCommand, Command,
        },
    };

    use std::fmt::Debug;

    use fake::{Dummy, Fake, Faker};

    use super::Request;

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
        T: Dummy<Faker> + Encoder + Decoder + Debug + PartialEq + Command + Into<Request>,
    {
        let command: T = Faker.fake();

        let request: Request = command.into();

        let mut buffer = vec![];
        let _ = request.encode(&mut buffer).unwrap();

        let (remaining, decoded) = Request::decode(&buffer).unwrap();

        assert_eq!(buffer[4..].len(), decoded.encoded_size() as usize);

        assert_eq!(request, decoded);

        assert!(remaining.is_empty());
    }
}
