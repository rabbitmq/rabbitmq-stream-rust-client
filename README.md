<h1 align="center">RabbitMQ Streams Client for Rust</h1>
<br/>
<div align="center">
 <strong>
   A work-in-progress Rust client for RabbitMQ Streams
 </strong>
</div>

<br />

<div align="center">

  <a href="https://github.com/rabbitmq/rabbitmq-stream-rust-client/actions?query=workflow%3ATests">
    <img src="https://github.com/rabbitmq/rabbitmq-stream-rust-client/workflows/Tests/badge.svg"
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

   <a href="https://deps.rs/repo/github/rabbitmq/rabbitmq-stream-rust-client">
    <img src="https://deps.rs/repo/github/rabbitmq/rabbitmq-stream-rust-client/status.svg"
      alt="deps" />
  </a>

  <a href="https://codecov.io/gh/rabbitmq/rabbitmq-stream-rust-client">
    <img src="https://codecov.io/gh/rabbitmq/rabbitmq-stream-rust-client/branch/main/graph/badge.svg?token=2DHIQ20BDE" alt="codecov"/>
  </a>



</div>


# RabbitMQ Stream Client

This is a Rust client library for working with [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams).

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

##### Building the environment with TLS

```rust,no_run
use rabbitmq_stream_client::Environment;

let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
.add_root_certificates(String::from(".ci/certs/ca_certificate.pem"))
.build();

// Use this configuration if you want to trust the certificates
// without providing the root certificate
let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
     .trust_certificates(true)
     .build();

let environment = Environment::builder()
    .host("localhost")
    .port(5551) // specify the TLS port of the node
    .tls(tls_configuration)
    .build()
```

##### Building the environment with a load balancer

```rust,no_run
use rabbitmq_stream_client::Environment;


let environment = Environment::builder()
    .load_balancer_mode(true)
    .build()
```



##### Publishing messages

```rust,no_run
use rabbitmq_stream_client::{Environment, types::Message};
let environment = Environment::builder().build().await?;
let producer = environment.producer().name("myproducer").build("mystream").await?;
for i in 0..10 {
    producer
      .send_with_confirm(Message::builder().body(format!("message{}", i)).build())
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
            let d = delivery.unwrap();
            println!("Got message: {:#?} with offset: {}",
                     d.message().data().map(|data| String::from_utf8(data.to_vec()).unwrap()),
                     d.offset(),);
        }
    });
// wait 10 second and then close the consumer
sleep(Duration::from_secs(10)).await;
handle.close().await?;
```

### Superstreams

The client supports the superstream functionality.

A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/) for more info.

You can use SuperStreamProducer and SuperStreamConsumer classes which internally uses producers and consumers to operate on the componsing streams.

Have a look to the examples to see on how to work with super streams.

See the [Super Stream Producer Example:](https://github.com/rabbitmq/rabbitmq-stream-rust-client/examples/send_super_stream.rs)

See the [Super Stream Consumer Example:](https://github.com/rabbitmq/rabbitmq-stream-rust-client/examples/receive_super_stream.rs)

### Development

#### Compiling

```bash
git clone https://github.com/rabbitmq/rabbitmq-stream-rust-client .
make build
```

#### Running Tests

To run tests you need to have a running RabbitMQ Stream node with a TLS configuration.
It is mandatory to use `make rabbitmq-server` to create a TLS configuration compatible with the tests.
See the `Environment` TLS tests for more details.

```bash
make rabbitmq-server
make test
```

#### Running Benchmarks

```bash
make rabbitmq-server
make run-benchmark
```
