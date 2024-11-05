<h1 align="center">RabbitMQ Streams Client for Rust</h1>
<br/>
<div align="center">
 <strong>
   A Rust client for RabbitMQ Streams
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


Welcome to the documentation for the RabbitMQ Stream Rust Client. This guide provides comprehensive information on installation, usage, and examples.

## Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Getting Started](#getting-started)
4. [Usage](#usage)
    - [Publishing Messages](#publishing-messages)
    - [Consuming Messages](#consuming-messages)
    - [Super Stream](#super-stream)
    - [Filtering](#filtering)
5. [Examples](#examples)
6. [Development](#development)
    - [Compiling](#Compiling)
    - [Running Tests](#running-tests)
    - [Running Benchmarks](#running-benchmarks)
    - [Contributing](#contributing)
    - [License](#license)

## Introduction

The RabbitMQ Stream Rust Client is a library designed for integrating Rust applications with RabbitMQ streams efficiently. It supports high throughput and low latency message streaming.

## Installation

Install from [crates.io](https://crates.io/crates/rabbitmq-stream-client)

```toml
[dependencies]
rabbitmq-stream-client = "*"
```

Then run `cargo build `to include it in your project.

## Getting Started
This section covers the initial setup and necessary steps to incorporate the RabbitMQ Stream client into your Rust application.

Ensure RabbitMQ server with stream support is installed.
The main access point is `Environment`, which is used to connect to a node.

```rust,no_run
use rabbitmq_stream_client::Environment;
let environment = Environment::builder().build().await?;
```
### Environment with TLS

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

### Environment with a load balancer


See the [documentation](https://www.rabbitmq.com/blog/2021/07/23/connecting-to-streams#with-a-load-balancer) about the stream and load-balancer.

```rust,no_run
use rabbitmq_stream_client::Environment;


let environment = Environment::builder()
    .load_balancer_mode(true)
    .build()
```



## Publishing messages

You can publish messages with three different methods:

* `send`: asynchronous, messages are automatically buffered internally and sent at once after a timeout expires. On confirmation a callback is triggered. See the [example](./examples/send_async.rs)
* `batch_send`: asynchronous, the user buffers the messages and sends them. This is the fastest publishing method. On confirmation a callback is triggered. See the [example](./examples/batch_send.rs)
* `send_with_confirm`: synchronous, the caller wait till the message is confirmed. This is the slowest publishing method. See the [example](./examples/send_with_confirm.rs)


## Consuming messages

As streams never delete any messages, any consumer can start reading/consuming from any point in the log

See the Consuming section part of the streaming doc for further info (Most of the examples refer to Java but applies for ths library too):

[Consuming messages from a stream](https://www.rabbitmq.com/docs/streams#consuming)

See also the Rust streaming tutorial-2 on how consume messages starting from different positions and how to enable Server-Side Offset Tracking too:

[RabbitMQ Streams - Rust tutorial 2](https://www.rabbitmq.com/tutorials/tutorial-two-rust-stream)

and the relative examples from the tutorials:

[Rust tutorials examples](https://github.com/rabbitmq/rabbitmq-tutorials/tree/main/rust-stream)

See also a simple example here on how to consume from a stream:

[Consuming messages from a stream example](./examples/simple-consumer.rs)

## Super Stream

The client supports the super-stream functionality.

A super stream is a logical stream made of individual, regular streams. It is a way to scale out publishing and consuming with RabbitMQ Streams: a large logical stream is divided into partition streams, splitting up the storage and the traffic on several cluster nodes.

See the [blog post](https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams/) for more info.

You can use SuperStreamProducer and SuperStreamConsumer classes which internally uses producers and consumers to operate on the componsing streams.

Have a look to the examples to see on how to work with super streams.

See the [Super Stream Producer Example](./examples/superstreams/send_super_stream.rs)

See the [Super Stream Consumer Example](./examples/superstreams/receive_super_stream.rs)


## Filtering

Filtering is a new streaming feature enabled from RabbitMQ 3.13 based on Bloom filter. RabbitMQ Stream provides a server-side filtering feature that avoids reading all the messages of a stream and filtering only on the client side. This helps to save network bandwidth when a consuming application needs only a subset of messages.

See the Java documentation for more details (Same concepts apply here):

[Filtering - Java Doc](https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#filtering)

See Rust filtering examples:

See the [Producer with filtering Example](./examples/filtering/send_with_filtering.rs)

See the [Consumer with filtering Example](./examples/filtering/receive_with_filtering.rs)


### Examples

Refer to the [examples](./examples) directory for detailed code samples illustrating various use cases 
like error handling, batch processing, super streams and different ways to send messages.

## Development

### Compiling

```bash
git clone https://github.com/rabbitmq/rabbitmq-stream-rust-client .
make build
```

### Running Tests

To run tests you need to have a running RabbitMQ Stream node with a TLS configuration.
It is mandatory to use `make rabbitmq-server` to create a TLS configuration compatible with the tests.
See the `Environment` TLS tests for more details.

```bash
make rabbitmq-server
make test
```

### Running Benchmarks

```bash
make rabbitmq-server
make run-benchmark
```

## Contributing
Contributions are welcome! Please read our contributing guide to understand how to submit issues, enhancements, or patches.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
