use rabbitmq_stream_client::Environment;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

    environment.stream_creator().create("test").await?;

    environment.delete_stream("test").await?;
    Ok(())
}
