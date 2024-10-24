use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, HashRoutingMurmurStrategy, Message, ResponseCode, RoutingStrategy,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

fn hash_strategy_value_extractor(message: &Message) -> String {
    String::from_utf8(Vec::from(message.data().unwrap())).expect("Found invalid UTF-8")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let message_count = 100;
    let stream = "hello-rust-stream";
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());

    let _ = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(stream, 3, None)
        .await;

    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(stream, 3, None)
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

    let mut super_stream_producer = environment
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .build(stream)
        .await
        .unwrap();

    for i in 0..message_count {
        println!("sending message {}", i);
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder().body(format!("message{}", i)).build();
        super_stream_producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
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
    let _ = super_stream_producer.close().await;
    Ok(())
}
