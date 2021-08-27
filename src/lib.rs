mod channel;
mod client;
mod codec;
mod dispatcher;
pub mod error;
pub mod handler;
pub mod metadata;
pub mod offset_specification;
mod options;

pub type RabbitMQStreamResult<T> = Result<T, RabbitMqStreamError>;

pub use client::Client;
use error::RabbitMqStreamError;
pub use options::ClientOptions;
