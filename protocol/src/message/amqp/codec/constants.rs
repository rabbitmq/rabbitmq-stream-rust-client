use std::convert::TryInto;

use crate::{
    codec::decoder::read_u8,
    message::amqp::{
        error::{AmqpDecodeError, AmqpEncodeError},
        types::Descriptor,
    },
};

pub const MESSAGE_HEADER: Descriptor = Descriptor::Ulong(112);
pub const MESSAGE_DELIVERY_ANNOTATIONS: Descriptor = Descriptor::Ulong(113);
pub const MESSAGE_ANNOTATIONS: Descriptor = Descriptor::Ulong(114);
pub const MESSAGE_PROPERTIES: Descriptor = Descriptor::Ulong(115);
pub const MESSAGE_APPLICATION_PROPERTIES: Descriptor = Descriptor::Ulong(116);
pub const MESSAGE_BODY_DATA: Descriptor = Descriptor::Ulong(117);
pub const MESSAGE_BODY_SEQUENCE: Descriptor = Descriptor::Ulong(118);
pub const MESSAGE_BODY_VALUE: Descriptor = Descriptor::Ulong(119);
pub const MESSAGE_FOOTER: Descriptor = Descriptor::Ulong(120);

pub const SECTION_PREFIX_LENGTH: u32 = 3;
use byteorder::WriteBytesExt;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use super::{AmqpDecoder, AmqpEncoder};

#[derive(Debug, TryFromPrimitive, Clone, Copy, IntoPrimitive)]
#[repr(u8)]
pub enum TypeCode {
    Described = 0x00,
    Null = 0x40, // fixed width --V
    Boolean = 0x56,
    BooleanTrue = 0x41,
    BooleanFalse = 0x42,
    UInt0 = 0x43,
    ULong0 = 0x44,
    UByte = 0x50,
    UShort = 0x60,
    UInt = 0x70,
    ULong = 0x80,
    Byte = 0x51,
    Short = 0x61,
    Int = 0x71,
    Long = 0x81,
    UIntSmall = 0x52,
    ULongSmall = 0x53,
    IntSmall = 0x54,
    LongSmall = 0x55,
    Float = 0x72,
    Double = 0x82,
    Char = 0x73,
    Timestamp = 0x83,
    Uuid = 0x98,
    Binary8 = 0xa0, // variable --V
    Binary32 = 0xb0,
    String8 = 0xa1,
    String32 = 0xb1,
    Symbol8 = 0xa3,
    Symbol32 = 0xb3,
    List0 = 0x45, // compound --V
    List8 = 0xc0,
    List32 = 0xd0,
    Map8 = 0xc1,
    Map32 = 0xd1,
    Array8 = 0xe0,
    Array32 = 0xf0,
}

impl AmqpEncoder for TypeCode {
    fn encoded_size(&self) -> u32 {
        1
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), AmqpEncodeError> {
        let code: u8 = (*self).into();
        writer.write_u8(code).map(Ok)?
    }
}

impl AmqpDecoder for TypeCode {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), AmqpDecodeError> {
        let (input, code) = read_u8(input).unwrap();
        Ok((
            input,
            code.try_into()
                .map_err(|_| AmqpDecodeError::InvalidTypeCode(code))?,
        ))
    }
}
