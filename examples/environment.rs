use futures::StreamExt;
use rabbitmq_stream_client::{
    types::{ByteCapacity, Message, OffsetSpecification},
    Environment,
};
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

    let mut producer = environment
        .producer()
        .name("test_producer")
        .build("test")
        .await?;

    for i in 0..message_count {
        producer
            .send_with_confirm(Message::builder().body(format!("message{}", i)).build())
            .await?;
    }

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
