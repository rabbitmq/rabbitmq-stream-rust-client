//! rabbitmq-stream-client
//!
//! Experimental Rust client for [RabbitMQ Stream](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)
//!
//! The main access point is [`Environment`], which is used to connect to a node.
//!
//! ## Example
//!
//! ### Building the environment
//!
//! ```rust,no_run
//! # async fn doc_fn() -> Result<(), Box<dyn std::error::Error>> {
//! use rabbitmq_stream_client::Environment;
//!
//! let environment = Environment::builder().build().await?;
//! # Ok(())
//! # }
//! ```
//!
//! For more connection options check [`EnvironmentBuilder`]
//!
//! ### Publishing messages
//!
//! ```rust,no_run
//! # async fn doc_fn() -> Result<(), Box<dyn std::error::Error>> {
//! use rabbitmq_stream_client::{Environment, types::Message};
//!
//! let environment = Environment::builder().build().await?;
//! let producer = environment.producer().build("mystream").await?;
//!
//! for i in 0..10 {
//!     producer
//!         .send_with_confirm(Message::builder().body(format!("message{}", i)).build())
//!         .await?;
//! }
//!
//! producer.close().await?;
//!
//! # Ok(())
//! # }
//! ```
//! For more producer options check [`ProducerBuilder`]
//!
//! ### Consuming messages
//!
//! ```rust,no_run
//! # async fn doc_fn() -> Result<(), Box<dyn std::error::Error>> {
//! use rabbitmq_stream_client::{Environment};
//! use futures::StreamExt;
//! use tokio::task;
//! use tokio::time::{sleep, Duration};
//!
//! let environment = Environment::builder().build().await?;
//! let mut consumer = environment.consumer().build("mystream").await?;
//!
//!
//! let handle = consumer.handle();
//! task::spawn(async move {
//!     while let Some(delivery) = consumer.next().await {
//!         println!("Got message {:?}",delivery);
//!     }
//! });
//!
//! // wait 10 second and then close the consumer
//! sleep(Duration::from_secs(10)).await;
//!
//! handle.close().await?;
//!
//! # Ok(())
//! # }
//! ```
//! For more consumer options check [`ConsumerBuilder`]

#![cfg_attr(docsrs, feature(doc_cfg))]

mod byte_capacity;
mod client;
mod consumer;
mod environment;
pub mod error;
mod offset_specification;
mod producer;
mod stream_creator;
mod superstream;
mod superstream_consumer;
mod superstream_producer;

pub type RabbitMQStreamResult<T> = Result<T, error::ClientError>;

pub use crate::client::{
    Client, ClientOptions, MetricsCollector, TlsConfiguration, TlsConfigurationBuilder,
};

pub use crate::consumer::{
    Consumer, ConsumerBuilder, ConsumerHandle, FilterConfiguration, MessageContext,
};
pub use crate::environment::{Environment, EnvironmentBuilder};
pub use crate::producer::{Dedup, NoDedup, Producer, ProducerBuilder};
pub mod types {

    pub use crate::byte_capacity::ByteCapacity;
    pub use crate::client::{Broker, MessageResult, StreamMetadata};
    pub use crate::consumer::{Delivery, MessageContext};
    pub use crate::offset_specification::OffsetSpecification;
    pub use crate::stream_creator::LeaderLocator;
    pub use crate::stream_creator::StreamCreator;
    pub use crate::superstream::HashRoutingMurmurStrategy;
    pub use crate::superstream::RoutingKeyRoutingStrategy;
    pub use crate::superstream::RoutingStrategy;
    pub use crate::superstream_consumer::SuperStreamConsumer;
    pub use crate::superstream_producer::SuperStreamProducer;
    pub use rabbitmq_stream_protocol::message::Message;
    pub use rabbitmq_stream_protocol::{Response, ResponseCode, ResponseKind};

    pub use rabbitmq_stream_protocol::message::{
        AnnonationKey, ApplicationProperties, DeliveryAnnotations, Footer, Header, Map, Properties,
        SimpleValue, Value,
    };
}
