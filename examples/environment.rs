use rabbitmq_stream_client::{error::RabbitMqStreamError, Environment};

#[tokio::main]
async fn main() -> Result<(), RabbitMqStreamError> {
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;
    Ok(())
}
