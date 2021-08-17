use std::{collections::HashMap, io::Write};

use byteorder::{BigEndian, WriteBytesExt};

use crate::{error::EncodeError, types::Header, types::PublishedMessage, ResponseCode};

use crate::types::PublishingError;

use super::Encoder;

impl Encoder for i8 {
    fn encoded_size(&self) -> u32 {
        1
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i8(*self)?;
        Ok(())
    }
}

impl Encoder for i32 {
    fn encoded_size(&self) -> u32 {
        4
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(*self)?;
        Ok(())
    }
}

impl Encoder for u8 {
    fn encoded_size(&self) -> u32 {
        1
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u8(*self)?;
        Ok(())
    }
}

impl Encoder for u16 {
    fn encoded_size(&self) -> u32 {
        2
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u16::<BigEndian>(*self)?;
        Ok(())
    }
}

impl Encoder for u32 {
    fn encoded_size(&self) -> u32 {
        4
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(*self)?;
        Ok(())
    }
}

impl Encoder for u64 {
    fn encoded_size(&self) -> u32 {
        8
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u64::<BigEndian>(*self)?;
        Ok(())
    }
}

impl Encoder for i64 {
    fn encoded_size(&self) -> u32 {
        8
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i64::<BigEndian>(*self)?;
        Ok(())
    }
}

impl Encoder for Header {
    fn encoded_size(&self) -> u32 {
        2 + 2
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u16::<BigEndian>(self.key())?;
        writer.write_u16::<BigEndian>(self.version())?;

        Ok(())
    }
}

impl Encoder for PublishedMessage {
    fn encoded_size(&self) -> u32 {
        8 + self.message.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publishing_id.encode(writer)?;
        self.message.encode(writer)?;
        Ok(())
    }
}

impl Encoder for Vec<PublishedMessage> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.len() as u32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for &str {
    fn encoded_size(&self) -> u32 {
        2 + self.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i16::<BigEndian>(self.len() as i16)?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl Encoder for HashMap<String, String> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, (k, v)| {
            acc + k.as_str().encoded_size() + v.as_str().encoded_size()
        })
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.len() as u32)?;

        for (k, v) in self {
            k.as_str().encode(writer)?;
            v.as_str().encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for Vec<String> {
    fn encoded_size(&self) -> u32 {
        4 + self
            .iter()
            .fold(0, |acc, v| acc + v.as_str().encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u32::<BigEndian>(self.len() as u32)?;
        for x in self {
            x.as_str().encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for Vec<u8> {
    fn encoded_size(&self) -> u32 {
        4 + self.len() as u32
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        writer.write_all(self)?;
        Ok(())
    }
}

impl Encoder for Vec<u32> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for Vec<u64> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}

impl Encoder for ResponseCode {
    fn encoded_size(&self) -> u32 {
        2
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_u16::<BigEndian>(self.into())?;
        Ok(())
    }
}

pub fn encode_response_code(code: u16) -> u16 {
    code | 0b1000_0000_0000_0000
}

impl Encoder for PublishingError {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.publishing_id.encode(writer)?;
        self.error_code.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        8 + 2
    }
}

impl Encoder for Vec<PublishingError> {
    fn encoded_size(&self) -> u32 {
        4 + self.iter().fold(0, |acc, v| acc + v.encoded_size())
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        for x in self {
            x.encode(writer)?;
        }
        Ok(())
    }
}
