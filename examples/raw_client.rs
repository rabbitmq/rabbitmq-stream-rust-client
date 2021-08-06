use std::collections::HashMap;

use rabbitmq_stream_client::{
    error::RabbitMqStreamError, offset_specification::OffsetSpecification, Client, ClientOptions,
};
use tracing::info;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), RabbitMqStreamError> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let client = Client::connect(ClientOptions::default()).await?;

    let _ = client
        .subscribe(1, "test", OffsetSpecification::Next, 1, HashMap::new())
        .await?;

    let mut channel = client.subscribe_messages();
    loop {
        let msg = channel.recv().await.unwrap();

        info!("Got {:?}", msg);

        client.credit(1, 1).await.unwrap();
    }
}
