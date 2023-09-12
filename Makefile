
all: test build

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all -- -D warnings

build: fmt clippy
	cargo build

test: build
	cargo test --all -- --nocapture

watch: build
	cargo watch -x 'test --all -- --nocapture'


run-benchmark:
	cargo run --release -p benchmark

rabbitmq-server:
	docker build -t rabbitmq-tls-test .
	docker run -it --rm --name rabbitmq-tls-test \
		-p 5552:5552 -p 5551:5551 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		rabbitmq-tls-test

help:
	cat Makefile
