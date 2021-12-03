use futures::StreamExt;
use rabbitmq_stream_client::{
    types::{ByteCapacity, Message, OffsetSpecification},
    Environment,
};
use tokio::sync::mpsc::channel;
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

    let _ = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(2))
        .create("test")
        .await;

    let producer = environment.producer().build("test").await?;

    let (tx, mut rx) = channel(message_count);
    for i in 0..message_count {
        let tx_cloned = tx.clone();
        producer
            .send(
                Message::builder().body(format!("message{}", i)).build(),
                move |confirm_result| {
                    info!("Message confirm result {:?}", confirm_result);
                    let tx_cloned = tx_cloned.clone();
                    async move {
                        tx_cloned.send(()).await.unwrap();
                    }
                },
            )
            .await?;
    }
    drop(tx);

    while let Some(_) = rx.recv().await {}

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
