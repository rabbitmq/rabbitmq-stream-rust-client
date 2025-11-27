use std::{collections::HashMap, time::Duration};

use crate::{byte_capacity::ByteCapacity, environment::Environment, error::StreamCreateError};

/// Builder for creating a RabbitMQ stream
pub struct StreamCreator {
    pub(crate) env: Environment,
    pub options: HashMap<String, String>,
}

impl StreamCreator {
    pub fn new(env: Environment) -> Self {
        let creator = Self {
            env,
            options: HashMap::new(),
        };

        creator.leader_locator(LeaderLocator::LeastLeaders)
    }

    /// Create a stream with name and options
    pub async fn create(self, stream: &str) -> Result<(), StreamCreateError> {
        let client = self.env.create_client().await?;
        let response = client.create_stream(stream, self.options).await?;
        client.close().await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamCreateError::Create {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub async fn create_super_stream(
        self,
        super_stream: &str,
        number_of_partitions: usize,
        binding_keys: Option<Vec<String>>,
    ) -> Result<(), StreamCreateError> {
        let mut partitions_names = Vec::with_capacity(number_of_partitions);
        let new_binding_keys: Vec<String> = if let Some(keys) = binding_keys {
            // Use the provided binding keys
            keys.iter()
                .map(|binding_key| {
                    partitions_names.push(super_stream.to_owned() + "-" + binding_key);
                    binding_key.clone()
                })
                .collect()
        } else {
            (0..number_of_partitions)
                .map(|i| {
                    partitions_names.push(super_stream.to_owned() + "-" + &i.to_string());
                    i.to_string()
                })
                .collect()
        };

        let client = self.env.create_client().await?;
        let response = client
            .create_super_stream(
                super_stream,
                partitions_names,
                new_binding_keys,
                self.options,
            )
            .await?;
        client.close().await?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamCreateError::Create {
                stream: super_stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.options
            .insert("max-age".to_owned(), format!("{}s", max_age.as_secs()));
        self
    }
    pub fn leader_locator(mut self, leader_locator: LeaderLocator) -> Self {
        self.options.insert(
            "queue-leader-locator".to_owned(),
            leader_locator.as_ref().to_string(),
        );
        self
    }
    pub fn max_length(mut self, byte_capacity: ByteCapacity) -> Self {
        self.options.insert(
            "max-length-bytes".to_owned(),
            byte_capacity.bytes().to_string(),
        );
        self
    }
    pub fn max_segment_size(mut self, byte_capacity: ByteCapacity) -> Self {
        self.options.insert(
            "stream-max-segment-size-bytes".to_owned(),
            byte_capacity.bytes().to_string(),
        );
        self
    }
}

pub enum LeaderLocator {
    ClientLocal,
    Random,
    LeastLeaders,
}

impl AsRef<str> for LeaderLocator {
    fn as_ref(&self) -> &str {
        match self {
            LeaderLocator::ClientLocal => "client-local",
            LeaderLocator::Random => "random",
            LeaderLocator::LeastLeaders => "least-leaders",
        }
    }
}
