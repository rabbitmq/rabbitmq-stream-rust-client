
all: test build


fmt:
	cargo fmt

clippy:
	cargo clippy --all

build fmt clippy:
	cargo fmt --all -- --check

test build:
	cargo test --all

rabbitmq-server:
	docker run -it --rm --name rabbitmq-stream-go-client-test \
		-p 5552:5552 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		--pull always \
		pivotalrabbitmq/rabbitmq-stream

rabbitmq-ha-proxy:
	cd compose; rm -rf tls-gen;
	cd compose; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	cd compose; docker build -t haproxy-rabbitmq-cluster  .
	cd compose; docker-compose down
	cd compose; docker-compose up
