use core::panic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use rabbitmq_stream_client::error::{ProducerPublishError, StreamCreateError};
use rabbitmq_stream_client::types::{ByteCapacity, Message, ResponseCode};
use rabbitmq_stream_client::Environment;
use rabbitmq_stream_client::{
    ConfirmationStatus, NoDedup, OnClosed, Producer, RabbitMQStreamResult,
};
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::info;

struct MyHAProducerInner {
    environment: Environment,
    stream: String,
    producer: RwLock<Producer<NoDedup>>,
    notify: Notify,
    is_opened: AtomicBool,
}

#[derive(Clone)]
struct MyHAProducer(Arc<MyHAProducerInner>);

#[async_trait::async_trait]
impl OnClosed for MyHAProducer {
    async fn on_closed(&self, unconfirmed: Vec<Message>) {
        info!("Producer is closed. Creating new one");

        self.0
            .is_opened
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let mut producer = self.0.producer.write().await;

        let new_producer = self
            .0
            .environment
            .producer()
            .build(&self.0.stream)
            .await
            .unwrap();

        new_producer.set_on_closed(Box::new(self.clone())).await;

        if !unconfirmed.is_empty() {
            info!("Resending {} unconfirmed messages.", unconfirmed.len());
            if let Err(e) = producer.batch_send_with_confirm(unconfirmed).await {
                eprintln!("Error resending unconfirmed messages: {:?}", e);
            }
        }

        *producer = new_producer;

        self.0
            .is_opened
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }
}

impl MyHAProducer {
    async fn new(environment: Environment, stream: &str) -> RabbitMQStreamResult<Self> {
        ensure_stream_exists(&environment, stream).await?;

        let producer = environment.producer().build(stream).await.unwrap();

        let inner = MyHAProducerInner {
            environment,
            stream: stream.to_string(),
            producer: RwLock::new(producer),
            notify: Notify::new(),
            is_opened: AtomicBool::new(true),
        };
        let s = Self(Arc::new(inner));

        let p = s.0.producer.write().await;
        p.set_on_closed(Box::new(s.clone())).await;
        drop(p);

        Ok(s)
    }

    async fn send_with_confirm(
        &self,
        message: Message,
    ) -> Result<ConfirmationStatus, ProducerPublishError> {
        if !self.0.is_opened.load(std::sync::atomic::Ordering::SeqCst) {
            self.0.notify.notified().await;
        }

        let producer = self.0.producer.read().await;
        let err = producer.send_with_confirm(message.clone()).await;

        match err {
            Ok(s) => Ok(s),
            Err(e) => match e {
                ProducerPublishError::Timeout | ProducerPublishError::Closed => {
                    Box::pin(self.send_with_confirm(message)).await
                }
                _ => Err(e),
            },
        }
    }
}

async fn ensure_stream_exists(environment: &Environment, stream: &str) -> RabbitMQStreamResult<()> {
    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    if let Err(StreamCreateError::Create { stream, status }) = create_response {
        match status {
            // we can ignore this error because the stream already exists
            ResponseCode::StreamAlreadyExists => {}
            err => {
                panic!("Error creating stream: {:?} {:?}", stream, err);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let environment = Environment::builder().build().await?;
    let stream = "hello-rust-stream";

    let producer = MyHAProducer::new(environment, stream).await?;

    let number_of_messages = 1000000;
    for i in 0..number_of_messages {
        let msg = Message::builder()
            .body(format!("stream message_{}", i))
            .build();
        producer.send_with_confirm(msg).await?;
        sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
