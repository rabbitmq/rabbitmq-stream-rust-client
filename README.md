<h1 align="center">RabbitMQ Stream Rust</h1>

<div align="center">
 <strong>
   A Work in progress Rust Client for RabbitMQ Stream
 </strong>
</div>

<br />

<div align="center">

  <a href="https://github.com/korsmakolnikov/rabbitmq-stream-rust-client/actions?query=workflow%3ATests">
    <img src="https://github.com/korsmakolnikov/rabbitmq-stream-rust-client/workflows/Tests/badge.svg"
    alt="Tests status" />
  </a>
  
  <a href="https://crates.io/crates/rabbitmq-stream-client">
    <img src="https://img.shields.io/crates/d/rabbitmq-stream-client.svg?style=flat-square"
      alt="Download" />
  </a>
  <a href="https://docs.rs/rabbitmq-stream-client">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>

   <a href="https://deps.rs/repo/github/korsmakolnikov/rabbitmq-stream-rust-client">
    <img src="https://deps.rs/repo/github/korsmakolnikov/rabbitmq-stream-rust-client/status.svg"
      alt="deps" />
  </a>
</div>

# RabbitMQ Stream Client

A Rust Client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### Installation

Install from [crates.io](https://crates.io/)

```toml
[dependencies]
rabbitmq-stream-client = "*"
```

### Quick Start

The main access point is `Environment`, which is used to connect to a node.

#### Example

##### Building the environment

```rust,no_run
use rabbitmq_stream_client::Environment;
let environment = Environment::builder().build().await?;
```

##### Publishing messages

```rust,no_run
use rabbitmq_stream_client::{Environment, types::Message};
let environment = Environment::builder().build().await?;
let producer = environment.producer().name("myproducer").build("mystream").await?;
for i in 0..10 {
    producer
        .send(Message::builder().body(format!("message{}", i)).build())
        .await?;
}
producer.close().await?;
```

##### Consuming messages

```rust,no_run
use rabbitmq_stream_client::{Environment};
use futures::StreamExt;
use tokio::task;
use tokio::time::{sleep, Duration};
let environment = Environment::builder().build().await?;
let mut consumer = environment.consumer().build("mystream").await?;
let handle = consumer.handle();
task::spawn(async move {
    while let Some(delivery) = consumer.next().await {
        println!("Got message {:?}",delivery);
    }
});
// wait 10 second and then close the consumer
sleep(Duration::from_secs(10)).await;
handle.close().await?;
```

### Development

#### Compiling

```bash
git clone https://github.com/rabbitmq/rabbitmq-stream-rust-client .
make build
```

#### Running Tests

```bash
make rabbitmq-server
make test
```

#### Running Benchmarks

```bash
make rabbitmq-server
make run-benchmark
```
