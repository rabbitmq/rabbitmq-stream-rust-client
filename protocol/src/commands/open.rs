use std::{collections::HashMap, io::Write};

use crate::{
    codec::{read_u32, Decoder, Encoder},
    error::{DecodeError, EncodeError},
};

#[derive(PartialEq, Debug)]
pub struct OpenRequest {
    virtual_host: String,
}

impl Encoder for OpenRequest {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.encode_str(writer, &self.virtual_host)?;
        Ok(())
    }
}
#[derive(Debug, PartialEq)]
pub struct OpenResponse {
    connection_properties: HashMap<String, String>,
}

impl OpenResponse {
    /// Get a reference to the open response's connection properties.
    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.connection_properties
    }
}

impl Decoder for OpenResponse {
    fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
        let (mut input, num_properties) = read_u32(input)?;

        let mut connection_properties = HashMap::with_capacity(num_properties as usize);
        for _ in 0..num_properties {
            let (input1, key) = Self::decode_str(input)?;
            let (input2, value) = Self::decode_str(input1)?;

            if let (Some(k), Some(v)) = (key, value) {
                connection_properties.insert(k, v);
            }
            input = input2;
        }

        Ok((
            input,
            OpenResponse {
                connection_properties,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::OpenRequest;
    use crate::{
        codec::{Decoder, Encoder},
        commands::open::OpenResponse,
        error::DecodeError,
    };
    use byteorder::{BigEndian, WriteBytesExt};

    impl Decoder for OpenRequest {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
            let (remaining, virtual_host) = Self::decode_str(input)?;

            Ok((
                remaining,
                OpenRequest {
                    virtual_host: virtual_host.unwrap(),
                },
            ))
        }
    }

    #[test]
    fn open_request_ser_der() {
        let mut buffer = vec![];

        let open = OpenRequest {
            virtual_host: "test".to_owned(),
        };

        let _ = open.encode(&mut buffer);

        let (remaining, decoded) = OpenRequest::decode(&buffer).unwrap();

        assert_eq!(open, decoded);

        assert!(remaining.is_empty());
    }

    impl Encoder for OpenResponse {
        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            writer.write_u32::<BigEndian>(self.connection_properties.len() as u32)?;

            for (k, v) in &self.connection_properties {
                self.encode_str(writer, k)?;
                self.encode_str(writer, v)?;
            }
            Ok(())
        }
    }

    #[test]
    fn open_response_ser_der() {
        let mut buffer = vec![];

        let mut properties = HashMap::new();

        properties.insert("test".to_owned(), "test".to_owned());

        let open_response = OpenResponse {
            connection_properties: properties,
        };

        let _ = open_response.encode(&mut buffer);

        let (remaining, decoded) = OpenResponse::decode(&buffer).unwrap();

        assert_eq!(open_response, decoded);

        assert!(remaining.is_empty());
    }
}
