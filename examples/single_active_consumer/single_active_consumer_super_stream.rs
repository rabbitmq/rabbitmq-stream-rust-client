use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode, SuperStreamConsumer,
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let message_count = 1000000;
    let super_stream = "hello-rust-super-stream";

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
    println!(
        "Super stream consumer example, consuming messages from the super stream {}",
        super_stream
    );

    let mut properties = HashMap::new();

    properties.insert("single-active-consumer".to_string(), "true".to_string());
    properties.insert("name".to_string(), "consumer-group-1".to_string());
    properties.insert("super-stream".to_string(), "hello-rust-super-stream".to_string());

    let mut super_stream_consumer: SuperStreamConsumer = environment
        .super_stream_consumer()
        .offset(OffsetSpecification::First)
        .client_provided_name("my super stream consumer for hello rust")
        .consumer_update(move |active, message_context| {
            println!("single active consumer: is active: {} on stream {}", active, message_context.get_stream());
            OffsetSpecification::First
        })
        .properties(properties)
        .build(super_stream)
        .await
        .unwrap();

    for _ in 0..message_count {
        let delivery = super_stream_consumer.next().await.unwrap();
        {
            let delivery = delivery.unwrap();
            println!(
                "Got message: {:#?} from stream: {} with offset: {}",
                delivery
                    .message()
                    .data()
                    .map(|data| String::from_utf8(data.to_vec()).unwrap())
                    .unwrap(),
                delivery.stream(),
                delivery.offset()
            );
        }
    }

    println!("Stopping super stream consumer...");
    let _ = super_stream_consumer.handle().close().await;
    println!("Super stream consumer stopped");
    Ok(())
}