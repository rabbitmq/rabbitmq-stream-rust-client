use rabbitmq_stream_client::error::{ProducerPublishError, StreamCreateError};
use rabbitmq_stream_client::types::{ByteCapacity, Message, ResponseCode};
use rabbitmq_stream_client::Environment;
use rabbitmq_stream_client::{
    ConfirmationStatus, NoDedup, OnClosed, Producer, RabbitMQStreamResult,
};
use tokio::sync::RwLock;

struct MyHAProducer {
    environment: Environment,
    stream: String,
    producer: RwLock<Producer<NoDedup>>,
}

#[async_trait::async_trait]
impl OnClosed for MyHAProducer {
    async fn on_closed(&self, unconfirmed: Vec<Message>) {
        let mut producer = self.producer.write().await;

        let new_producer = self
            .environment
            .producer()
            .build(&self.stream)
            .await
            .unwrap();

        if let Err(e) = producer.batch_send_with_confirm(unconfirmed).await {
            eprintln!("Error resending unconfirmed messages: {:?}", e);
        }

        *producer = new_producer;
    }
}

impl MyHAProducer {
    async fn new(environment: Environment, stream: &str) -> RabbitMQStreamResult<Self> {
        ensure_stream_exists(&environment, stream).await?;

        let producer = environment.producer().build(stream).await.unwrap();

        Ok(Self {
            environment,
            stream: stream.to_string(),
            producer: RwLock::new(producer),
        })
    }

    async fn send_with_confirm(
        &self,
        message: Message,
    ) -> Result<ConfirmationStatus, ProducerPublishError> {
        let producer = self.producer.read().await;
        producer.send_with_confirm(message).await
    }
}

async fn ensure_stream_exists(environment: &Environment, stream: &str) -> RabbitMQStreamResult<()> {
    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    if let Err(e) = create_response {
        if let StreamCreateError::Create { stream, status } = e {
            match status {
                // we can ignore this error because the stream already exists
                ResponseCode::StreamAlreadyExists => {}
                err => {
                    panic!("Error creating stream: {:?} {:?}", stream, err);
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = Environment::builder().build().await?;
    let stream = "hello-rust-stream";

    let producer = MyHAProducer::new(environment, stream).await?;

    producer
        .send_with_confirm(Message::builder().body("Hello, world!").build())
        .await?;

    /*
    let number_of_messages = 1000000;
    for i in 0..number_of_messages {
        let msg = Message::builder()
            .body(format!("stream message_{}", i))
            .build();
        producer.send_with_confirm(msg).await?;
    }
    producer.close().await?;
    */

    Ok(())
}
