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

    for i in 0..10 {
        producer
            .send(Message::builder().body(format!("message{}", i)).build())
            .await?;
    }

    producer.close().await?;

    environment.delete_stream("test").await?;
    Ok(())
}
