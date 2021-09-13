pub mod byte_capacity;
mod client;
mod environment;
pub mod error;
pub mod offset_specification;
pub mod producer;
pub mod stream_creator;
pub type RabbitMQStreamResult<T> = Result<T, ClientError>;

pub use client::{Broker, Client, ClientOptions, StreamMetadata};
use error::ClientError;

pub use environment::{Environment, EnvironmentBuilder};
