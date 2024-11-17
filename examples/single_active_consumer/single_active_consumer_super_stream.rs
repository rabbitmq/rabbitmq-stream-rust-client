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

    let mut super_stream_consumer: SuperStreamConsumer = environment
        .super_stream_consumer()
        // Mandatory if sac is enabled
        .name("consumer-group-1")
        .offset(OffsetSpecification::First)
        .enable_single_active_consumer(true)
        .client_provided_name("my super stream consumer for hello rust")
        .consumer_update(move |active, message_context|  async move {
            let name = message_context.name();
            let stream = message_context.stream();
            let client = message_context.client();

            println!(
                "single active consumer: is active: {} on stream: {} with consumer_name: {}",
                active, stream, name
            );
            let stored_offset = client.query_offset(name, stream.as_str()).await;

            if let Err(e) = stored_offset {
                return OffsetSpecification::First;
            }
            let stored_offset_u = stored_offset.unwrap();
            println!("stored_offset_u {}", stored_offset_u.clone());
            OffsetSpecification::Offset(stored_offset_u)

        })
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

            // Store an offset for every consumer
            if delivery.consumer_name().is_some() && delivery.offset() == 1000   {
                super_stream_consumer.client().store_offset(delivery.consumer_name().unwrap().as_str(), delivery.stream().as_str(), delivery.offset()).await;
            }

        }
    }

    println!("Stopping super stream consumer...");
    let _ = super_stream_consumer.handle().close().await;
    println!("Super stream consumer stopped");
    Ok(())
}