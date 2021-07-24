use std::io::Write;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub trait Encoder {
    fn encode(&self, writer: &mut impl Write) -> Result<(), ()>;

    fn encode_str(&self, writer: &mut impl Write, input: &str) -> Result<(), ()> {
        writer.write_i16::<BigEndian>(input.len() as i16);
        writer.write_all(input.as_bytes());
        Ok(())
    }
}

pub(crate) trait Decoder
where
    Self: Sized,
{
    fn decode(input: &[u8]) -> Result<(&[u8], Self), ()>;

    fn decode_str(mut input: &[u8]) -> Result<(&[u8], Option<String>), ()> {
        let len = input.read_i16::<BigEndian>().unwrap();

        if len == -1 {
            return Ok((input, None));
        }

        let string_slice = &input[0..len as usize];

        let string = String::from_utf8(string_slice.to_vec()).unwrap();

        Ok((&input[len as usize..], Some(string)))
    }
}
