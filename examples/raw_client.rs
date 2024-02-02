use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use rabbitmq_stream_client::types::{Message, MessageResult, ResponseKind};
use rabbitmq_stream_client::{types::OffsetSpecification, Client, ClientOptions};
use tokio::sync::Notify;
use tracing::info;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    let notifier = Arc::new(Notify::new());

    let messages = 10;
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let client = Client::connect(ClientOptions::default()).await?;

    let stream = "test";

    let _ = client.delete_stream(stream).await?;

    let _ = client.create_stream(stream, HashMap::new()).await?;

    start_subscriber(stream, &client, notifier.clone()).await?;

    client
        .declare_publisher(1, Some("my_publisher".to_owned()), stream)
        .await?;
    for i in 0..messages {
        let _ = client
            .publish(
                1,
                Message::builder()
                    .body(format!("message {}", i).as_bytes().to_vec())
                    .build(),
            )
            .await
            .unwrap();
    }
    notifier.notified().await;

    client.delete_publisher(1).await?;

    let _ = client.delete_stream(stream).await?;
    Ok(())
}

async fn start_subscriber(
    stream: &str,
    client: &Client,
    notifier: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_inner = client.clone();
    let notifier_inner = notifier.clone();
    let counter = Arc::new(AtomicI32::new(0));

    let handler = move |msg: MessageResult| async move {
        if let Some(Ok(response)) = msg {
            match response.kind_ref() {
                ResponseKind::Deliver(delivery) => {
                    for message in &delivery.messages {
                        info!(
                            "Got message {:?}",
                            message.data().map(|data| String::from_utf8(data.to_vec()))
                        );
                    }
                    let len = delivery.messages.len();
                    let current = counter
                        .fetch_add(len as i32, std::sync::atomic::Ordering::Relaxed)
                        + len as i32;
                    if current == 10 {
                        client_inner.unsubscribe(1).await.unwrap();
                        notifier_inner.notify_one();
                    } else {
                        client_inner.credit(1, 1).await.unwrap();
                    }
                }
                _ => {
                    info!("Got response {:?}", &response);
                }
            }
        }
        Ok(())
    };
    client.set_handler(handler).await;

    let _ = client
        .subscribe(1, stream, OffsetSpecification::Next, 1, HashMap::new())
        .await?;

    Ok(())
}
