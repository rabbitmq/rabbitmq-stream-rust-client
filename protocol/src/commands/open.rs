use std::{collections::HashMap, io::Write};

use crate::{
    codec::{Decoder, Encoder},
    error::{DecodeError, EncodeError},
    protocol::commands::COMMAND_OPEN,
};

use super::{Command, Correlated};

#[derive(PartialEq, Debug)]
pub struct OpenCommand {
    virtual_host: String,
}

impl OpenCommand {
    pub fn new(virtual_host: String) -> Self {
        Self { virtual_host }
    }
}

impl Encoder for OpenCommand {
    fn encode(&self, writer: &mut impl Write) -> Result<(), EncodeError> {
        self.encode_str(writer, &self.virtual_host)?;
        Ok(())
    }

    fn encoded_size(&self) -> u32 {
        2 + self.virtual_host.len() as u32
    }
}

impl Correlated for OpenCommand {}

impl Command for OpenCommand {
    fn key(&self) -> u16 {
        COMMAND_OPEN
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
        let (input, connection_properties) = Self::decode_map(input)?;

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

    use super::OpenCommand;
    use crate::{
        codec::{Decoder, Encoder},
        commands::open::OpenResponse,
        error::DecodeError,
    };

    impl Decoder for OpenCommand {
        fn decode(input: &[u8]) -> Result<(&[u8], Self), DecodeError> {
            let (remaining, virtual_host) = Self::decode_str(input)?;

            Ok((
                remaining,
                OpenCommand {
                    virtual_host: virtual_host.unwrap(),
                },
            ))
        }
    }

    #[test]
    fn open_request_test() {
        let mut buffer = vec![];

        let open = OpenCommand {
            virtual_host: "test".to_owned(),
        };

        let _ = open.encode(&mut buffer);

        let (remaining, decoded) = OpenCommand::decode(&buffer).unwrap();

        assert_eq!(open, decoded);

        assert!(remaining.is_empty());
    }

    impl Encoder for OpenResponse {
        fn encode(
            &self,
            writer: &mut impl std::io::Write,
        ) -> Result<(), crate::error::EncodeError> {
            self.encode_map(writer, &self.connection_properties)?;
            Ok(())
        }

        fn encoded_size(&self) -> u32 {
            0
        }
    }

    #[test]
    fn open_response_test() {
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
