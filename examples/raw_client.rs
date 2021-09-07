use std::collections::HashMap;
use std::sync::Arc;

use rabbitmq_stream_client::{
    error::RabbitMqStreamError, offset_specification::OffsetSpecification, Client, ClientOptions,
};
use rabbitmq_stream_protocol::Response;
use rabbitmq_stream_protocol::ResponseKind;
use tokio::sync::Notify;
use tracing::info;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
#[tokio::main]
async fn main() -> Result<(), RabbitMqStreamError> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    let notifier = Arc::new(Notify::new());

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let client = Client::connect(ClientOptions::default()).await?;

    let _ = client.create_stream("test", HashMap::new()).await?;

    let _ = client
        .subscribe(1, "test", OffsetSpecification::First, 1, HashMap::new())
        .await?;

    let client_inner = client.clone();
    let notifier_inner = notifier.clone();

    let handler = move |response: Response| async move {
        match response.kind() {
            ResponseKind::Deliver(delivery) => {
                info!("Got deliver message {:?}", &delivery);
                client_inner.credit(1, 1).await.unwrap()
            }
            ResponseKind::Heartbeat(_) => notifier_inner.notify_one(),
            _ => {
                info!("Got  message {:?}", &response);
            }
        }
        Ok(())
    };

    client.set_handler(handler).await;

    notifier.notified().await;
    Ok(())
}
