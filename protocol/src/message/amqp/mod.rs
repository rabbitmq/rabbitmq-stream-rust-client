mod body;
mod codec;
mod error;
mod header;
mod message;
mod properties;
mod section;
mod types;

pub use codec::{AmqpDecoder, AmqpEncoder};
pub use error::{AmqpDecodeError, AmqpEncodeError};

pub use header::Header;
pub use message::Message as AmqpMessage;
pub use properties::Properties;
pub use types::{
    Address, AnnonationKey, Annotations, ApplicationProperties, MessageId, SequenceNo, Symbol,
    Timestamp, Value,
};

#[cfg(test)]
mod tests {

    use std::{fmt::Debug, ops::Range};

    use pretty_assertions::assert_eq;

    use fake::{Dummy, Fake, Faker};

    const DEFAULT_LEN_RANGE: Range<usize> = 0..10;

    use super::{AmqpDecoder, AmqpEncoder};
    pub(crate) fn type_encode_decode_test<T>()
    where
        T: Dummy<Faker> + AmqpDecoder + AmqpEncoder + Debug + PartialEq + Debug,
    {
        let mut buffer = vec![];

        let value: T = Faker.fake();

        let _ = value.encode(&mut buffer).unwrap();

        let (remaining, decoded) = T::decode(&buffer).unwrap();

        assert_eq!(value, decoded);

        assert!(remaining.is_empty());

        assert_eq!(buffer.len() as u32, value.encoded_size(), "{:#?}", value);
    }
    pub(crate) fn type_encode_decode_test_fuzzy<T>()
    where
        T: Dummy<Faker> + AmqpDecoder + AmqpEncoder + Debug + PartialEq,
    {
        let mut rng = rand::thread_rng();
        let len: usize = DEFAULT_LEN_RANGE.fake_with_rng(&mut rng);

        for _ in 0..len {
            type_encode_decode_test::<T>()
        }
    }
}
