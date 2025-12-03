use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{ByteCapacity, OffsetSpecification, ResponseCode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let stream = "hello-rust-stream";

    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    if let Err(StreamCreateError::Create { stream, status }) = create_response {
        match status {
            // we can ignore this error because the stream already exists
            ResponseCode::StreamAlreadyExists => {}
            err => {
                println!("Error creating stream: {:?} {:?}", stream, err);
            }
        }
    }

    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .build(stream)
        .await
        .unwrap();

    while let Some(Ok(delivery)) = consumer.next().await {
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

    let _ = consumer.handle().close().await;

    println!("Super stream consumer stopped");
    Ok(())
}
