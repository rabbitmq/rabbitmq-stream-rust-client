use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{ByteCapacity, Message, ResponseCode};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let message_count = 1000000;
    let stream = "hello-rust-stream";
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());
    let _ = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    let delete_stream = environment.delete_stream(stream).await;

    match delete_stream {
        Ok(_) => {
            println!("Successfully deleted stream {}", stream);
        }
        Err(err) => {
            println!("Failed to delete stream {}. error {}", stream, err);
        }
    }

    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    if let Err(e) = create_response {
        if let StreamCreateError::Create { stream, status } = e {
            match status {
                // we can ignore this error because the stream already exists
                ResponseCode::StreamAlreadyExists => {}
                err => {
                    println!("Error creating stream: {:?} {:?}", stream, err);
                }
            }
        }
    }
    println!(
        "Send stream example. Sending {} messages to the stream: {}",
        message_count, stream
    );

    let producer = environment.producer().build(stream).await.unwrap();

    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder()
            .body(format!("stream message_{}", i))
            .build();
        producer
            .send(msg, move |confirmation_status| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                println!("Message confirmed with status {:?}", confirmation_status);
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == message_count - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    notify_on_send.notified().await;
    println!(
        "Successfully sent {} messages to the stream {}",
        message_count, stream
    );
    let _ = producer.close().await;

    Ok(())
}
