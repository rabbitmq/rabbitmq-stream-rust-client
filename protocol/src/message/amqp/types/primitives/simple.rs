use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, TimeZone, Utc};
use derive_more::{From, TryInto};
use ordered_float::OrderedFloat;

use crate::{
    codec::decoder::{
        read_exact, read_f32, read_f64, read_i16, read_i32, read_i64, read_i8, read_u16, read_u32,
        read_u64, read_u8,
    },
    message::amqp::{
        codec::{constants::TypeCode, AmqpEncoder},
        error::AmqpEncodeError,
        types::Symbol,
        AmqpDecodeError, AmqpDecoder,
    },
    utils::TupleMapperSecond,
};

#[cfg(test)]
use fake::Fake;

/// Primitive AMQP 1.0 data type
#[derive(Debug, Eq, PartialEq, Hash, Clone, From, TryInto)]
#[try_into(owned, ref, ref_mut)]
#[cfg_attr(test, derive(fake::Dummy))]
pub enum SimpleValue {
    Null,
    Boolean(Boolean),
    Ubyte(UByte),
    Ushort(UShort),
    Uint(UInt),
    Ulong(ULong),
    Byte(Byte),
    Short(Short),
    Int(Int),
    Long(Long),
    Float(Float),
    Double(Double),
    Char(Char),
    Timestamp(Timestamp),
    Uuid(Uuid),
    Binary(Binary),
    String(Str),
    Symbol(Symbol),
}

impl From<&str> for SimpleValue {
    fn from(string: &str) -> Self {
        SimpleValue::String(string.into())
    }
}
impl AmqpEncoder for SimpleValue {
    fn encoded_size(&self) -> u32 {
        match self {
            SimpleValue::Null => 1,
            SimpleValue::Boolean(value) => value.encoded_size(),
            SimpleValue::Ubyte(value) => value.encoded_size(),
            SimpleValue::Ushort(value) => value.encoded_size(),
            SimpleValue::Uint(value) => value.encoded_size(),
            SimpleValue::Ulong(value) => value.encoded_size(),
            SimpleValue::Byte(value) => value.encoded_size(),
            SimpleValue::Short(value) => value.encoded_size(),
            SimpleValue::Int(value) => value.encoded_size(),
            SimpleValue::Long(value) => value.encoded_size(),
            SimpleValue::Float(value) => value.encoded_size(),
            SimpleValue::Double(value) => value.encoded_size(),
            SimpleValue::Char(value) => value.encoded_size(),
            SimpleValue::Timestamp(value) => value.encoded_size(),
            SimpleValue::Uuid(value) => value.encoded_size(),
            SimpleValue::Binary(value) => value.encoded_size(),
            SimpleValue::String(value) => value.encoded_size(),
            SimpleValue::Symbol(value) => value.encoded_size(),
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        match self {
            SimpleValue::Null => TypeCode::Null.encode(writer),
            SimpleValue::Boolean(value) => value.encode(writer),
            SimpleValue::Ubyte(value) => value.encode(writer),
            SimpleValue::Ushort(value) => value.encode(writer),
            SimpleValue::Uint(value) => value.encode(writer),
            SimpleValue::Ulong(value) => value.encode(writer),
            SimpleValue::Byte(value) => value.encode(writer),
            SimpleValue::Short(value) => value.encode(writer),
            SimpleValue::Int(value) => value.encode(writer),
            SimpleValue::Long(value) => value.encode(writer),
            SimpleValue::Float(value) => value.encode(writer),
            SimpleValue::Double(value) => value.encode(writer),
            SimpleValue::Char(value) => value.encode(writer),
            SimpleValue::Timestamp(value) => value.encode(writer),
            SimpleValue::Uuid(value) => value.encode(writer),
            SimpleValue::Binary(value) => value.encode(writer),
            SimpleValue::String(value) => value.encode(writer),
            SimpleValue::Symbol(value) => value.encode(writer),
        }
    }
}

impl AmqpDecoder for SimpleValue {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (remaining, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Null => Ok((remaining, SimpleValue::Null)),
            TypeCode::Boolean | TypeCode::BooleanFalse | TypeCode::BooleanTrue => {
                Boolean::decode(input).map_second(SimpleValue::Boolean)
            }
            TypeCode::UInt0 | TypeCode::UIntSmall | TypeCode::UInt => {
                UInt::decode(input).map_second(SimpleValue::Uint)
            }
            TypeCode::ULong0 | TypeCode::ULongSmall | TypeCode::ULong => {
                ULong::decode(input).map_second(SimpleValue::Ulong)
            }
            TypeCode::UByte => UByte::decode(input).map_second(SimpleValue::Ubyte),
            TypeCode::UShort => UShort::decode(input).map_second(SimpleValue::Ushort),
            TypeCode::Byte => Byte::decode(input).map_second(SimpleValue::Byte),
            TypeCode::Short => Short::decode(input).map_second(SimpleValue::Short),
            TypeCode::IntSmall | TypeCode::Int => Int::decode(input).map_second(SimpleValue::Int),
            TypeCode::LongSmall | TypeCode::Long => {
                Long::decode(input).map_second(SimpleValue::Long)
            }
            TypeCode::Float => Float::decode(input).map_second(SimpleValue::Float),
            TypeCode::Double => Double::decode(input).map_second(SimpleValue::Double),
            TypeCode::Char => Char::decode(input).map_second(SimpleValue::Char),
            TypeCode::Timestamp => Timestamp::decode(input).map_second(SimpleValue::Timestamp),
            TypeCode::Uuid => Uuid::decode(input).map_second(SimpleValue::Uuid),
            TypeCode::Binary8 | TypeCode::Binary32 => {
                Binary::decode(input).map_second(SimpleValue::Binary)
            }
            TypeCode::String8 | TypeCode::String32 => {
                Str::decode(input).map_second(SimpleValue::String)
            }
            TypeCode::Symbol8 | TypeCode::Symbol32 => {
                Symbol::decode(input).map_second(SimpleValue::Symbol)
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type Boolean = bool;

pub type UByte = u8;

impl AmqpEncoder for UByte {
    fn encoded_size(&self) -> u32 {
        2
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::UByte.encode(writer)?;
        writer.write_u8(*self).map(Ok)?
    }
}

impl AmqpDecoder for UByte {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::UByte) => read_u8(input).map_err(AmqpDecodeError::from),
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type UShort = u16;

impl AmqpEncoder for UShort {
    fn encoded_size(&self) -> u32 {
        3
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::UShort.encode(writer)?;
        writer.write_u16::<BigEndian>(*self)?;
        Ok(())
    }
}

impl AmqpDecoder for UShort {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::UShort) => read_u16(input).map_err(AmqpDecodeError::from),
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}
pub type UInt = u32;

impl AmqpEncoder for UInt {
    fn encoded_size(&self) -> u32 {
        if *self == 0 {
            1
        } else if *self > u32::from(u8::MAX) {
            5
        } else {
            2
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        if *self == 0 {
            TypeCode::UInt0.encode(writer).map(Ok)?
        } else if *self > u32::from(u8::MAX) {
            TypeCode::UInt.encode(writer)?;
            writer.write_u32::<BigEndian>(*self).map(Ok)?
        } else {
            TypeCode::UIntSmall.encode(writer)?;
            writer.write_u8(*self as u8).map(Ok)?
        }
    }
}

impl AmqpDecoder for UInt {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::UInt0 => Ok((input, 0)),
            TypeCode::UIntSmall => read_u8(input)
                .map_second(UInt::from)
                .map_err(AmqpDecodeError::from),
            TypeCode::UInt => read_u32(input).map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type ULong = u64;

impl AmqpEncoder for ULong {
    fn encoded_size(&self) -> u32 {
        if *self == 0 {
            1
        } else if *self > u64::from(u8::MAX) {
            9
        } else {
            2
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        if *self == 0 {
            TypeCode::ULong0.encode(writer).map(Ok)?
        } else if *self > u64::from(u8::MAX) {
            TypeCode::ULong.encode(writer)?;
            writer.write_u64::<BigEndian>(*self).map(Ok)?
        } else {
            TypeCode::ULongSmall.encode(writer)?;
            writer.write_u8(*self as u8).map(Ok)?
        }
    }
}

impl AmqpDecoder for ULong {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::ULong) => read_u64(input)
                .map_second(ULong::from)
                .map_err(AmqpDecodeError::from),
            (input, TypeCode::ULongSmall) => read_u8(input)
                .map_second(ULong::from)
                .map_err(AmqpDecodeError::from),
            (input, TypeCode::ULong0) => Ok((input, 0)),
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type Byte = i8;

impl AmqpDecoder for Byte {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::Byte) => read_i8(input).map_err(AmqpDecodeError::from),
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Byte {
    fn encoded_size(&self) -> u32 {
        2
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Byte.encode(writer)?;
        writer.write_i8(*self)?;
        Ok(())
    }
}

pub type Short = i16;

impl AmqpDecoder for Short {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Short => read_i16(input).map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Short {
    fn encoded_size(&self) -> u32 {
        3
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Short.encode(writer)?;
        writer.write_i16::<BigEndian>(*self)?;
        Ok(())
    }
}

pub type Int = i32;

impl AmqpEncoder for Int {
    fn encoded_size(&self) -> u32 {
        if *self > i32::from(i8::MAX) || *self < i32::from(i8::MIN) {
            5
        } else {
            2
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        if *self > i32::from(i8::MAX) || *self < i32::from(i8::MIN) {
            TypeCode::Int.encode(writer)?;
            writer.write_i32::<BigEndian>(*self)?;
        } else {
            TypeCode::IntSmall.encode(writer)?;
            writer.write_i8(*self as i8)?;
        }
        Ok(())
    }
}
impl AmqpDecoder for Int {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Int => read_i32(input).map_err(AmqpDecodeError::from),
            TypeCode::IntSmall => read_u8(input)
                .map_second(i32::from)
                .map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type Long = i64;

impl AmqpDecoder for Long {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Long => read_i64(input).map_err(AmqpDecodeError::from),
            TypeCode::LongSmall => read_i8(input)
                .map_second(i64::from)
                .map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Long {
    fn encoded_size(&self) -> u32 {
        if *self > i64::from(i8::MAX) || *self < i64::from(i8::MIN) {
            9
        } else {
            3
        }
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        if *self > i64::from(i8::MAX) || *self < i64::from(i8::MIN) {
            TypeCode::Long.encode(writer)?;
            writer.write_i64::<BigEndian>(*self)?;
        } else {
            TypeCode::LongSmall.encode(writer)?;
            writer.write_i8(*self as i8)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub struct Float(OrderedFloat<f32>);
impl From<f32> for Float {
    fn from(float: f32) -> Self {
        Float(float.into())
    }
}

impl AmqpDecoder for Float {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Float => read_f32(input)
                .map_second(Float::from)
                .map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Float {
    fn encoded_size(&self) -> u32 {
        5
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Float.encode(writer)?;
        writer.write_f32::<BigEndian>(self.0 .0)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub struct Double(OrderedFloat<f64>);

impl From<f64> for Double {
    fn from(double: f64) -> Self {
        Double(double.into())
    }
}

impl AmqpDecoder for Double {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Double => read_f64(input)
                .map_second(Double::from)
                .map_err(AmqpDecodeError::from),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Double {
    fn encoded_size(&self) -> u32 {
        9
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Double.encode(writer)?;
        writer.write_f64::<BigEndian>(self.0 .0)?;
        Ok(())
    }
}

pub type Char = char;

impl AmqpDecoder for Char {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Char => read_u32(input)
                .map_second(Char::from_u32)
                .map_err(AmqpDecodeError::from)
                .and_then(|(input, c)| {
                    Ok((input, c.ok_or_else(|| Self::invalid_type_code(code))?))
                }),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Char {
    fn encoded_size(&self) -> u32 {
        5
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Char.encode(writer)?;
        writer.write_u32::<BigEndian>(*self as u32)?;
        Ok(())
    }
}

pub type Uuid = uuid::Uuid;

impl AmqpDecoder for Uuid {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Uuid => read_exact(input, 16)
                .map_err(AmqpDecodeError::from)
                .and_then(|(input, bytes)| Ok((input, Uuid::from_slice(bytes)?))),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}
impl AmqpEncoder for Uuid {
    fn encoded_size(&self) -> u32 {
        TypeCode::Uuid.encoded_size() + 16
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Uuid.encode(writer)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}
// Binary
pub type Binary = Vec<u8>;

impl<T: AmqpEncoder> AmqpEncoder for Option<T> {
    fn encoded_size(&self) -> u32 {
        self.as_ref().map_or(1, T::encoded_size)
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        match self {
            Some(value) => value.encode(writer),
            None => TypeCode::Null.encode(writer),
        }
    }
}

impl<T: AmqpDecoder> AmqpDecoder for Option<T> {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        if let (input, TypeCode::Null) = TypeCode::decode(input)? {
            Ok((input, None))
        } else {
            T::decode(input).map_second(Some)
        }
    }
}

impl AmqpEncoder for Boolean {
    fn encoded_size(&self) -> u32 {
        1
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let code = if *self {
            TypeCode::BooleanTrue
        } else {
            TypeCode::BooleanFalse
        };
        code.encode(writer)?;
        Ok(())
    }
}

impl AmqpDecoder for Boolean {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;

        match code {
            TypeCode::Boolean => read_u8(input)
                .map_second(|boolean| boolean != 0)
                .map_err(AmqpDecodeError::from),
            TypeCode::BooleanTrue => Ok((input, true)),
            TypeCode::BooleanFalse => Ok((input, false)),
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

impl AmqpEncoder for Binary {
    fn encoded_size(&self) -> u32 {
        let len = self.len() as u32;
        let size = if len > u8::MAX as u32 { 5 } else { 2 };
        size + len
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let len = self.len() as u32;
        if len > u8::MAX as u32 {
            TypeCode::Binary32.encode(writer)?;
            writer.write_u32::<BigEndian>(len)?;
        } else {
            TypeCode::Binary8.encode(writer)?;
            writer.write_u8(len as u8)?;
        };
        writer.write_all(self)?;
        Ok(())
    }
}

impl AmqpDecoder for Binary {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::Binary8 => {
                let (input, len) = read_u8(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
            }
            TypeCode::Binary32 => {
                let (input, len) = read_u32(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

pub type Str = String;

impl AmqpEncoder for Str {
    fn encoded_size(&self) -> u32 {
        let length = self.len() as u32;
        let size = if length > u8::MAX as u32 { 5 } else { 2 };
        size + length
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let length = self.len();
        if length > u8::MAX as usize {
            TypeCode::String32.encode(writer)?;
            writer.write_u32::<BigEndian>(length as u32)?;
        } else {
            TypeCode::String8.encode(writer)?;
            writer.write_u8(length as u8)?;
        }

        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}
impl AmqpDecoder for Str {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = TypeCode::decode(input)?;
        match code {
            TypeCode::String8 => {
                let (input, len) = read_u8(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
                    .and_then(|(input, bytes)| Ok((input, String::from_utf8(bytes)?)))
            }
            TypeCode::String32 => {
                let (input, len) = read_u32(input)?;
                read_exact(input, len as usize)
                    .map_second(|bytes| bytes.to_vec())
                    .map(Ok)?
                    .and_then(|(input, bytes)| Ok((input, String::from_utf8(bytes)?)))
            }
            _ => Err(Self::invalid_type_code(code)),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone, From)]
pub struct Timestamp(DateTime<Utc>);

impl AmqpDecoder for Timestamp {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        match TypeCode::decode(input)? {
            (input, TypeCode::Timestamp) => read_i64(input)
                .map_second(|millis| Timestamp(Utc.timestamp_millis(millis)))
                .map(Ok)?,
            (_, code) => Err(Self::invalid_type_code(code)),
        }
    }
}

impl AmqpEncoder for Timestamp {
    fn encoded_size(&self) -> u32 {
        TypeCode::Timestamp.encoded_size() + 8
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        TypeCode::Timestamp.encode(writer)?;
        writer.write_i64::<BigEndian>(self.0.timestamp_millis())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use chrono::{DateTime, Timelike, Utc};
    use fake::{Dummy, Fake, Faker};
    use ordered_float::OrderedFloat;

    use super::{Double, Float, Timestamp};
    use crate::message::amqp::{tests::type_encode_decode_test_fuzzy, types::SimpleValue};

    impl Dummy<Faker> for Float {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let num: f32 = config.fake_with_rng(rng);
            Float(OrderedFloat::from(num))
        }
    }
    impl Dummy<Faker> for Double {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let num: f64 = config.fake_with_rng(rng);
            Double(OrderedFloat::from(num))
        }
    }

    impl Dummy<Faker> for Timestamp {
        fn dummy_with_rng<R: rand::Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let mut dt: DateTime<Utc> = config.fake_with_rng(rng);
            dt = dt.with_nanosecond(0).unwrap();
            Timestamp(dt)
        }
    }

    #[test]
    fn simple_value_encode_decode_test() {
        type_encode_decode_test_fuzzy::<SimpleValue>()
    }
}
