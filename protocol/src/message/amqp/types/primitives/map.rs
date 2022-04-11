use std::{collections::HashMap, fmt::Debug, hash::Hash, ops::Deref};

use byteorder::{BigEndian, WriteBytesExt};

use crate::{
    codec::decoder::{read_u32, read_u8},
    message::amqp::{
        codec::constants::TypeCode, AmqpDecodeError, AmqpDecoder, AmqpEncodeError, AmqpEncoder,
    },
    utils::TupleMapperSecond,
};

/// API for attaching additional metadata to messages
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Map<K, V>(pub(crate) HashMap<K, V>)
where
    K: Eq + PartialEq + Hash,
    V: PartialEq;

impl<K, V> Default for Map<K, V>
where
    K: Eq + PartialEq + Hash,
    V: PartialEq,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K, V> Map<K, V>
where
    K: Eq + PartialEq + Hash + AmqpEncoder,
    V: PartialEq + AmqpEncoder,
{
    fn content_size(&self) -> u32 {
        self.0
            .iter()
            .fold(0, |acc, (k, v)| acc + k.encoded_size() + v.encoded_size())
    }
}

impl<K, V> Deref for Map<K, V>
where
    K: Eq + PartialEq + Hash + AmqpEncoder,
    V: PartialEq + AmqpEncoder,
{
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<K, V> AmqpDecoder for Map<K, V>
where
    K: Eq + PartialEq + Hash + AmqpDecoder + Debug,
    V: PartialEq + AmqpDecoder + Debug,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        let (mut input, mut count) = match code {
            TypeCode::Map8 => {
                let (input, _) = read_u8(input)?;
                read_u8(input).map_second(u32::from)?
            }
            TypeCode::Map32 => {
                let (input, _) = read_u32(input)?;
                read_u32(input)?
            }
            _ => return Err(Self::invalid_type_code(code)),
        };

        let mut map = Map::default();

        while count > 0 {
            count -= 2;
            let (input_inner, key) = K::decode(input)?;
            let (input_inner, value) = V::decode(input_inner)?;
            map.0.insert(key, value);
            input = input_inner;
        }

        Ok((input, map))
    }
}

impl<K, V> AmqpEncoder for Map<K, V>
where
    K: Eq + PartialEq + Hash + AmqpEncoder,
    V: PartialEq + AmqpEncoder,
{
    fn encoded_size(&self) -> u32 {
        let content_size = self.content_size();

        let type_size = if content_size + 1 > u8::MAX as u32 {
            9
        } else {
            3
        };

        content_size + type_size
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let count = self.0.len() * 2;
        let content_size = self.content_size();

        if content_size + 1 > u8::MAX as u32 {
            TypeCode::Map32.encode(writer)?;
            writer.write_u32::<BigEndian>(content_size + 4)?;
            writer.write_u32::<BigEndian>(count as u32)?;
        } else {
            TypeCode::Map8.encode(writer)?;
            writer.write_u8((content_size + 1) as u8)?;
            writer.write_u8(count as u8)?;
        }

        for (k, v) in &self.0 {
            k.encode(writer)?;
            v.encode(writer)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {

    use crate::message::amqp::tests::type_encode_decode_test_fuzzy;

    use super::Map;

    use crate::message::amqp::types::{SimpleValue, Value};

    use fake::{Dummy, Fake, Faker};
    use rand::Rng;

    use std::{hash::Hash, ops::Range};

    const DEFAULT_LEN_RANGE: Range<usize> = 0..10;

    impl<K> Dummy<Faker> for Map<K, Value>
    where
        K: Dummy<Faker> + Hash + Eq,
    {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let len: usize = DEFAULT_LEN_RANGE.fake_with_rng(rng);
            let mut m = Map::default();

            for _ in 0..len {
                let simple: SimpleValue = config.fake_with_rng(rng);
                m.0.insert(config.fake_with_rng(rng), Value::Simple(simple));
            }
            m
        }

        fn dummy(config: &Faker) -> Self {
            let mut r = rand::thread_rng();
            Dummy::<Faker>::dummy_with_rng(config, &mut r)
        }
    }

    impl<K> Dummy<Faker> for Map<K, SimpleValue>
    where
        K: Dummy<Faker> + Hash + Eq,
    {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let len: usize = DEFAULT_LEN_RANGE.fake_with_rng(rng);
            let mut m = Map::default();

            for _ in 0..len {
                let simple: SimpleValue = config.fake_with_rng(rng);
                m.0.insert(config.fake_with_rng(rng), simple);
            }
            m
        }

        fn dummy(config: &Faker) -> Self {
            let mut r = rand::thread_rng();
            Dummy::<Faker>::dummy_with_rng(config, &mut r)
        }
    }
    #[test]
    fn map_encode_decode_test() {
        type_encode_decode_test_fuzzy::<Map<SimpleValue, Value>>()
    }
}
