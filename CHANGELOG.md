# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.8.0...rabbitmq-stream-client-v0.9.0) - 2025-06-09

### Other

- OnClosed callback for producer ([#293](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/293))
- Update derive_more requirement from 0.99 to 2.0 ([#294](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/294))
- Fix task leak ([#292](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/292))
- Fix new clippy version issues ([#291](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/291))
- Bump codecov/codecov-action from 5.4.0 to 5.4.2 ([#285](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/285))
- Bump codecov/codecov-action from 5.3.1 to 5.4.0 ([#277](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/277))
- Remove use of `doc(cfg(...))` and the docsrs configuration ([#282](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/282))
- Update fake requirement from 3.0.0 to 4.2.0 ([#281](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/281))
- Fix doc build failed ([#280](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/280))
- Expose LeaderLocator enum ([#279](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/279))
- Dynamic send ([#276](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/276))
- FnOnce as Producer::send callback ([#273](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/273))

## [0.8.0](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.7.1...rabbitmq-stream-client-v0.8.0) - 2025-02-11

### Other

- Deserialize `ClientOption` with serde ([#271](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/271))
- Update `tokio-rustls` & `rustls-pemfile` ([#269](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/269))

## [0.7.1](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.7.0...rabbitmq-stream-client-v0.7.1) - 2025-02-06

### Other

- Expose SuperStreamProducer struct ([#267](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/267))
- Bump codecov/codecov-action from 5.0.2 to 5.3.1 ([#264](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/264))
- Moving producers/consumers client connection creation to Environment ([#259](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/259))

## [0.7.0](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.6.1...rabbitmq-stream-client-v0.7.0) - 2024-11-21

### Other

- reviewing sac example ([#258](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/258))
- Update thiserror requirement from 1.0 to 2.0 ([#247](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/247))
- Single active consumer implementation ([#248](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/248))
- Bump codecov/codecov-action from 4.6.0 to 5.0.2 ([#256](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/256))
- Bump codecov/codecov-action from 4.5.0 to 4.6.0 ([#254](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/254))
- Bump actions-rs/toolchain from 1.0.6 to 1.0.7 ([#255](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/255))
- Bump docker/build-push-action from 5 to 6 ([#253](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/253))
- add ability to include properties to the consumers during subscriptions ([#249](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/249))

## [0.6.1](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.6.0...rabbitmq-stream-client-v0.6.1) - 2024-11-05

### Fixed

- fix a bug happening during sending of super_stream with routing_key ([#243](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/243))

### Other

- Update readme. A wrong license was written
- Update dependencies ([#244](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/244))
- Bump actions/checkout from 3 to 4 ([#202](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/202))
- Update README.md
- improving filtering examples + others ([#242](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/242))
- Add client properties ([#237](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/237))
- adding producer examples ([#240](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/240))

## [0.5.1](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.5.0...rabbitmq-stream-client-v0.5.1) - 2024-10-31

### Fixed

- fixes [#233](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/233) by incrementing the publisher sequence ([#234](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/234))

### Other

- cleanup unused connections when load balancing mode is active ([#239](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/239))
- Update README.md ([#238](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/238))
- Implement super_stream  ([#232](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/232))
- close client connection in client consumer ([#235](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/235))
- Create/Delete superstream and Partition and route commands ([#230](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/230))

## [0.5.0](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.4.4...rabbitmq-stream-client-v0.5.0) - 2024-08-23

### Other
- Change codecov upload ([#227](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/227))
- Filtering supported ([#225](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/225))

## [0.4.4](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.4.3...rabbitmq-stream-client-v0.4.4) - 2024-08-11

### Fixed
- fixing bug when consuming from OffsetSpecification:Offset ([#223](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/223))

## [0.4.3](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.4.2...rabbitmq-stream-client-v0.4.3) - 2024-07-30

### Other
- Add Client Workaround With a Load Balancer ([#220](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/220))
- Update README.md
- udpate producer and consumer ([#217](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/217))

## [0.4.2](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.4.1...rabbitmq-stream-client-v0.4.2) - 2024-02-16

### Other
- fix release-plz third attempt
- fix release-plz second attempt
- fix release-plz
- raw client example: set handler before subscribtion ([#211](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/211))
- Add ClientOptionsBuilder ([#210](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/210))

## [0.4.1](https://github.com/rabbitmq/rabbitmq-stream-rust-client/compare/rabbitmq-stream-client-v0.4.0...rabbitmq-stream-client-v0.4.1) - 2023-10-25

### Added
- add workflow for release-plz ([#200](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/200))

### Other
- expose store_offset and query_offset in consumer ([#203](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/203))
- release-plz config
- Update ordered-float requirement from 3.0.0 to 4.1.0 ([#199](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/199))
- Bump docker/build-push-action from 4 to 5 ([#198](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/198))
- Bump actions/checkout from 3 to 4 ([#196](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/196))
- Bump docker/setup-buildx-action from 2 to 3 ([#197](https://github.com/rabbitmq/rabbitmq-stream-rust-client/pull/197))
