use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, HashRoutingMurmurStrategy, Message, ResponseCode, RoutingStrategy,
};
use std::convert::TryInto;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time;

fn hash_strategy_value_extractor(message: &Message) -> String {
    message
        .application_properties()
        .unwrap()
        .get("id")
        .unwrap()
        .clone()
        .try_into()
        .unwrap()
}

pub async fn start_producer() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    println!("Super Stream Producer connected to RabbitMQ");
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());
    let _ = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(crate::SUPER_STREAM, 3, None)
        .await;

    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(crate::SUPER_STREAM, 3, None)
        .await;

    if let Err(e) = create_response {
        if let StreamCreateError::Create { stream, status } = e {
            match status {
                // we can ignore this error because the stream already exists
                ResponseCode::StreamAlreadyExists => {}
                err => {
                    println!(
                        "[Super Stream Producer] Error creating stream: {:?} {:?}",
                        stream, err
                    );
                }
            }
        }
    }

    let super_stream_producer = environment
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("rust stream producer - sac example")
        .build(crate::SUPER_STREAM)
        .await;

    match super_stream_producer {
        Ok(mut producer) => {
            println!("[Super Stream Producer] Successfully created super stream producer");
            let mut idx = 0;
            loop {
                let counter = confirmed_messages.clone();
                let notifier = notify_on_send.clone();
                let msg = Message::builder()
                    .body(format!("super stream message_{}", idx))
                    .application_properties()
                    .insert("id", idx.to_string())
                    .message_builder()
                    .build();

                let send_result = producer
                    .send(msg, move |_| {
                        let inner_counter = counter.clone();
                        let inner_notifier = notifier.clone();
                        async move {
                            if inner_counter.fetch_add(1, Ordering::Relaxed) == idx - 1 {
                                inner_notifier.notify_one();
                            }
                        }
                    })
                    .await;

                match send_result {
                    Ok(_) => {
                        idx += 1;
                        println!(
                            "[Super Stream Producer] Message {} sent to {}",
                            idx,
                            crate::SUPER_STREAM
                        );
                    }
                    Err(err) => {
                        println!(
                            "[Super Stream Producer] Failed to send message. error: {}",
                            err
                        );
                    }
                }

                time::sleep(time::Duration::from_millis(1_000)).await;
            }
        }
        Err(err) => {
            println!("Failed to create super stream producer. error {}", err);
            Ok(())
        }
    }
}
