pub mod byte_capacity;
mod client;
mod environment;
pub mod error;
pub mod offset_specification;
pub mod producer;
pub mod stream_creator;
pub type RabbitMQStreamResult<T> = Result<T, ClientError>;

use error::ClientError;

pub mod prelude {

    pub use crate::client::{Broker, Client, ClientOptions, StreamMetadata};
    pub use rabbitmq_stream_protocol::message::Message;

    pub use crate::environment::{Environment, EnvironmentBuilder};
}
