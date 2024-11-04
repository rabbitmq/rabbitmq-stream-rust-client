use futures::StreamExt;
use rabbitmq_stream_client::{
    types::{ByteCapacity, Message},
    Environment,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

    println!("creating batch_send stream");
    let _ = environment.delete_stream("batch_send").await;
    let messages_to_batch = 100;
    let iterations = 10000;
    environment
        .stream_creator()
        .max_length(ByteCapacity::GB(2))
        .create("batch_send")
        .await?;

    let producer = environment.producer().build("batch_send").await?;

    for _ in 0..iterations {
        println!("accumulating messages in buffer");
        let mut messages = Vec::with_capacity(messages_to_batch);
        for i in 0..messages_to_batch {
            let msg = Message::builder().body(format!("message{}", i)).build();
            messages.push(msg);
        }

        println!("sending in batch mode");
        producer
            .batch_send(messages, |confirmation_status| async move {
                println!("Message confirmed with status {:?}", confirmation_status);
            })
            .await?;
    }

    producer.close().await?;

    Ok(())
}
