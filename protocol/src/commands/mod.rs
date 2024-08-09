use crate::protocol::version::PROTOCOL_VERSION;

pub mod close;
pub mod create_stream;
pub mod credit;
pub mod declare_publisher;
pub mod delete;
pub mod delete_publisher;
pub mod deliver;
pub mod exchange_command_versions;
pub mod generic;
pub mod heart_beat;
pub mod metadata;
pub mod metadata_update;
pub mod open;
pub mod peer_properties;
pub mod publish;
pub mod publish_confirm;
pub mod publish_error;
pub mod query_offset;
pub mod query_publisher_sequence;
pub mod sasl_authenticate;
pub mod sasl_handshake;
pub mod store_offset;
pub mod subscribe;
pub mod tune;
pub mod unsubscribe;

pub trait Command {
    fn key(&self) -> u16;
    fn version(&self) -> u16 {
        PROTOCOL_VERSION
    }
}

#[cfg(test)]
mod tests {

    use std::fmt::Debug;

    use crate::codec::{Decoder, Encoder};
    use fake::{Dummy, Fake, Faker};
    pub(crate) fn command_encode_decode_test<T>()
    where
        T: Dummy<Faker> + Encoder + Decoder + Debug + PartialEq,
    {
        let mut buffer = vec![];

        let open: T = Faker.fake();

        let _ = open.encode(&mut buffer);

        let (remaining, decoded) = T::decode(&buffer).unwrap();

        assert_eq!(open, decoded);

        assert!(remaining.is_empty());
    }

    pub(crate) fn specific_command_encode_decode_test<T>(command: T)
    where
        T: Encoder + Decoder + Debug + PartialEq,
    {
        let mut buffer = vec![];

        let _ = command.encode(&mut buffer);

        let (remaining, decoded) = T::decode(&buffer).unwrap();

        assert_eq!(command, decoded);

        assert!(remaining.is_empty());
    }
}
