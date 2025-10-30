use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{ByteCapacity, Message, ResponseCode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let stream = "hello-rust-stream";
    let number_of_messages = 1000000;
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

    let producer = environment.producer().build(stream).await?;

    for i in 0..number_of_messages {
        let msg = Message::builder()
            .body(format!("stream message_{}", i))
            .build();
        producer.send_with_confirm(msg).await?;
    }
    producer.close().await?;
    Ok(())
}
