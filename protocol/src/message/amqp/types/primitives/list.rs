use crate::{
    codec::decoder::{read_u32, read_u8},
    message::amqp::{
        codec::constants::TypeCode, AmqpDecodeError, AmqpDecoder, AmqpEncodeError, AmqpEncoder,
    },
    utils::TupleMapperSecond,
};

#[derive(Debug, PartialEq, Eq, Clone, Hash, Default)]
pub struct List(pub Vec<Value>);

impl List {
    pub fn new() -> Self {
        Self(vec![])
    }
    pub fn push(&mut self, elem: impl Into<Value>) {
        self.0.push(elem.into())
    }

    fn content_size(&self) -> u32 {
        self.0.iter().fold(0, |acc, item| acc + item.encoded_size())
    }
}

impl AmqpEncoder for List {
    fn encoded_size(&self) -> u32 {
        let content_size = self.content_size();

        let header = if content_size + 1 > u8::MAX as u32 {
            9
        } else {
            3
        };

        header + content_size
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let content_size = self.content_size();

        if content_size + 1 > u8::MAX as u32 {
            TypeCode::List8.encode(writer)?;
            writer.write_u32::<BigEndian>(content_size + 4)?;
            writer.write_u32::<BigEndian>(self.0.len() as u32)?;
        } else {
            TypeCode::List8.encode(writer)?;
            writer.write_u8((content_size + 1) as u8)?;
            writer.write_u8(self.0.len() as u8)?;
        }

        for item in &self.0 {
            item.encode(writer)?;
        }
        Ok(())
    }
}

impl AmqpDecoder for List {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        let (mut input, count) = match code {
            TypeCode::List0 => return Ok((input, List::new())),
            TypeCode::List8 => {
                let (input, _) = read_u8(input)?;
                read_u8(input)
                    .map_second(u32::from)
                    .map_err(AmqpDecodeError::from)?
            }
            TypeCode::List32 => {
                let (input, _) = read_u32(input)?;
                read_u32(input).map_err(AmqpDecodeError::from)?
            }
            _ => return Err(Self::invalid_type_code(code)),
        };

        let mut list = List::new();

        for _ in 0..count {
            let (input_inner, elem) = Value::decode(input)?;
            list.0.push(elem);
            input = input_inner;
        }

        Ok((input, list))
    }
}

impl List {
    pub fn decode_with_fields<'a, T, F>(
        input: &'a [u8],
        f: F,
        mut dest: T,
    ) -> Result<(&'a [u8], T), AmqpDecodeError>
    where
        F: Fn(&'a [u8], u32, &mut T) -> Result<&'a [u8], AmqpDecodeError>,
    {
        let (input, code) = TypeCode::decode(input)?;

        let (mut input, count) = match code {
            TypeCode::List0 => return Ok((input, dest)),
            TypeCode::List8 => {
                let (input, _) = read_u8(input)?;
                read_u8(input).map_second(u32::from)?
            }
            TypeCode::List32 => {
                let (input, _) = read_u32(input)?;
                read_u32(input)?
            }

            _ => {
                return Err(AmqpDecodeError::MessageParse(format!(
                    "Invalid type code {:?} for list",
                    code
                )))
            }
        };

        for idx in 0..count {
            let input1 = f(input, idx, &mut dest)?;
            input = input1;
        }
        Ok((input, dest))
    }
}
#[macro_export(local_inner_macros)]
macro_rules! list_decoder {
    ($ty:ty, $fname:ident, { $($key:expr => $b:tt),* }) => {
        fn $fname<'a>(
            input: &'a [u8],
            idx: u32,
            dest: &mut $ty,
        ) -> Result<&'a [u8], AmqpDecodeError> {

            match idx {
                 $($key => {
                      list_decoder!(@internal dest, input, $b)
                 })*
                _ => {
                    Ok(input)
                }
            }
        }
    };
    (@internal $dest:ident, $input:ident, { $name:ident, $dt:ident, $def:expr }) => {
         {
             let (input, value) = Option::<$dt>::decode($input)?;
             $dest.$name = value.unwrap_or($def);
             Ok(input)
         }
    };
    (@internal $dest:ident, $input:ident, { $name:ident, $dt:ident, $def:expr, $optional:expr }) => {
        {
            let (input, value) = Option::<$dt>::decode($input)?;
            $dest.$name = value;
            Ok(input)
        }
    }
}

use byteorder::{BigEndian, WriteBytesExt};
pub use list_decoder;

use super::Value;

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use fake::{Dummy, Fake, Faker};

    use crate::message::amqp::{
        tests::type_encode_decode_test_fuzzy,
        types::{SimpleValue, Value},
    };

    use super::List;
    const DEFAULT_LEN_RANGE: Range<usize> = 0..10;

    impl Dummy<Faker> for List {
        fn dummy_with_rng<R: fake::rand::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let len: usize = DEFAULT_LEN_RANGE.fake_with_rng(rng);
            let mut m = List::new();

            for _ in 0..len {
                let simple: SimpleValue = config.fake_with_rng(rng);

                m.0.push(Value::Simple(simple));
            }
            m
        }
    }

    #[test]
    fn list_encode_decode_test() {
        type_encode_decode_test_fuzzy::<List>()
    }
}
