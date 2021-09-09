mod client;
mod environment;
pub mod error;
pub mod offset_specification;
pub type RabbitMQStreamResult<T> = Result<T, RabbitMqStreamError>;

pub use client::{Client, ClientOptions};
use error::RabbitMqStreamError;

pub use environment::{Environment, EnvironmentBuilder};
