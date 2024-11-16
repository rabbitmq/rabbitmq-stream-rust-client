use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode,
};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let message_count = 1000000;
    let stream = "hello-rust-super-stream-2";

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
        "Super stream consumer example, consuming messages from the super stream {}",
        stream
    );

    let mut consumer = environment
        .consumer()
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
            println!("restarting from stored_offset:  {}", stored_offset_u);
            OffsetSpecification::Offset(stored_offset_u)

        })
        .build(stream)
        .await
        .unwrap();

    for i in 0..message_count {
        let delivery = consumer.next().await.unwrap();
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

            //store an offset
            if i == 10000  {
                let _ = consumer
                    .store_offset(i)
                    .await
                    .unwrap_or_else(|e| println!("Err: {}", e));
            }
        }
    }

    println!("Stopping consumer...");
    let _ = consumer.handle().close().await;
    println!("consumer stopped");
    Ok(())
}