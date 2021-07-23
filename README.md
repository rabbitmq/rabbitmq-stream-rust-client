<h1 align="center">RabbitMQ Stream Rust</h1>

<div align="center">
 <strong>
   A Rust Client for RabbitMQ Stream
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

### Development

#### Compiling

```bash
git clone https://github.com/korsmakolnikov/rabbitmq-stream-rust-client.git .
make build
```

#### Running Tests

```bash
make rabbitmq-server
make test 
```
