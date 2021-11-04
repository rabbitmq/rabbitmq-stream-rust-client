use std::sync::Arc;

use futures::StreamExt;
use rabbitmq_stream_client::{
    types::{ByteCapacity, Message, OffsetSpecification},
    Environment,
};
use tokio::sync::Barrier;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

    let message_count = 10;
    environment
        .stream_creator()
        .max_length(ByteCapacity::GB(2))
        .create("test")
        .await?;

    let producer = environment
        .producer()
        .name("test_producer")
        .build("test")
        .await?;

    let barrier = Arc::new(Barrier::new(message_count + 1));
    for i in 0..message_count {
        let b_cloned = barrier.clone();
        producer
            .send_with_callback(
                Message::builder().body(format!("message{}", i)).build(),
                move |confirm_result| {
                    let inner_barrier = b_cloned.clone();
                    info!("Message confirm result {:?}", confirm_result);
                    async move {
                        inner_barrier.wait().await;
                    }
                },
            )
            .await?;
    }

    barrier.wait().await;

    producer.close().await?;

    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .build("test")
        .await
        .unwrap();

    for _ in 0..message_count {
        let delivery = consumer.next().await.unwrap()?;
        info!(
            "Got message : {:?} with offset {}",
            delivery
                .message()
                .data()
                .map(|data| String::from_utf8(data.to_vec())),
            delivery.offset()
        );
    }

    consumer.handle().close().await.unwrap();

    environment.delete_stream("test").await?;
    Ok(())
}
