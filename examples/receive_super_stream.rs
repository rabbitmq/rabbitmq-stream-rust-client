use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode, SuperStreamConsumer,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let message_count = 10;
    let super_stream = "hello-rust-stream";

    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(super_stream, 3, None)
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

    let mut super_stream_consumer: SuperStreamConsumer = environment
        .super_stream_consumer()
        .offset(OffsetSpecification::First)
        .build(super_stream)
        .await
        .unwrap();

    let received_messages = Arc::new(AtomicU32::new(0));

    for mut consumer in super_stream_consumer.get_consumers().await.into_iter() {
        let received_messages_outer = received_messages.clone();

        task::spawn(async move {
            let mut inner_received_messages = received_messages_outer.clone();
            while let Some(delivery) = consumer.next().await {
                let d = delivery.unwrap();
                println!(
                    "Got message: {:#?} from stream: {} with offset: {}",
                    d.message()
                        .data()
                        .map(|data| String::from_utf8(data.to_vec()).unwrap()),
                    d.stream(),
                    d.offset(),
                );
                let value = inner_received_messages.fetch_add(1, Ordering::Relaxed);
                if value == message_count {
                    let handle = consumer.handle();
                    _ = handle.close().await;
                    break;
                }
            }
        });
    }

    sleep(Duration::from_millis(20000)).await;

    Ok(())
}
