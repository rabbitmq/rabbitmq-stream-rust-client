
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
	docker run -it --rm --name rabbitmq-stream-go-client-test \
		-p 5552:5552 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		--pull always \
		pivotalrabbitmq/rabbitmq-stream

build-tls-docker-image:
	cd docker && docker build -t rabbitmq_tls -f Dockerfile .

rabbitmq-server-tls: build-tls-docker-image
	docker run --rm --name rabbitmq-stream-tls \
                    -p 5552:5552 -p 5672:5672 -p 15672:15672 \
                    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
                    rabbitmq_tls

help:
	cat Makefile
