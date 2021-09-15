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

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamCreateError::CreateError {
                stream: stream.to_owned(),
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
