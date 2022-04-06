use std::sync::Arc;

use chrono::Utc;
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

    let stream_name = "complex_message";
    let _ = environment.delete_stream(stream_name).await;
    let message_count = 1;
    environment
        .stream_creator()
        .max_length(ByteCapacity::GB(2))
        .create(stream_name)
        .await?;

    let producer = environment.producer().build(stream_name).await?;

    let barrier = Arc::new(Barrier::new(message_count + 1));
    for i in 0..message_count {
        let producer_cloned = producer.clone();

        let barrier_cloned = barrier.clone();
        tokio::task::spawn(async move {
            let message = Message::builder()
                .message_annotations()
                .insert("test", i as i32)
                .message_builder()
                .application_properties()
                .insert("test", i as i32)
                .message_builder()
                .properties()
                .content_encoding("application/json")
                .absolute_expiry_time(Utc::now())
                .message_builder()
                .body(format!("message{}", i))
                .build();
            producer_cloned.send_with_confirm(message).await.unwrap();

            barrier_cloned.wait().await;
        });
    }

    barrier.wait().await;

    producer.close().await?;

    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .build(stream_name)
        .await
        .unwrap();

    for _ in 0..message_count {
        let delivery = consumer.next().await.unwrap()?;
        info!(
            "Got message: {:#?} with offset: {}",
            delivery.message(),
            delivery.offset(),
        );
    }

    consumer.handle().close().await.unwrap();

    environment.delete_stream(stream_name).await?;
    Ok(())
}
