/* Send 100 messages with filter of Region "California" and 100 messages with filter "Texas"
Filters are specified in the application_properties of the messages using the custom field "Region"
in the filter_value_extractor callback
The main thread wait on a condition variable until all the messages have been confirmed */

use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::ResponseCode;
use rabbitmq_stream_client::types::{ByteCapacity, Message};
use rabbitmq_stream_client::Environment;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

// This callback instruct the Producer on what filter we want to apply
// In this case we are returning the value of the application_property "region" value
fn filter_value_extractor(message: &Message) -> String {
    message
        .application_properties()
        .unwrap()
        .get("region")
        .unwrap()
        .clone()
        .try_into()
        .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());
    let stream = "test_stream_filtering";

    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

    let message_count = 200;
    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(&stream)
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

    let mut producer = environment
        .producer()
        .name("test_producer")
        // we are telling the producer to use the callback filter_value_extractor to compute the filter
        .filter_value_extractor(filter_value_extractor)
        .build("test_stream_filtering")
        .await?;

    // Sending first 200 messages with filter California
    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();

        let msg = Message::builder()
            .body(format!("super stream message_{}", i))
            .application_properties()
            .insert("region", "California")
            .message_builder()
            .build();

        producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == (message_count * 2) - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    // Sending 200 messages with filter Texas
    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder()
            .body(format!("super stream message_{}", i))
            .application_properties()
            .insert("region", "Texas")
            .message_builder()
            .build();

        producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == (message_count * 2) - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    notify_on_send.notified().await;
    producer.close().await?;

    Ok(())
}
