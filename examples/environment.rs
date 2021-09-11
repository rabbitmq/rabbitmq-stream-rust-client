use rabbitmq_stream_client::{byte_capacity::ByteCapacity, Environment};

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

    environment.delete_stream("test").await?;
    Ok(())
}
