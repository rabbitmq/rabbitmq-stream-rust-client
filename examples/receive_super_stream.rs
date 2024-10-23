use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode, SuperStreamConsumer,
};

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

    let mut received_messages = 0;

    while let delivery = super_stream_consumer.next().await.unwrap() {
        println!("inside while delivery loop");
        let d = delivery.unwrap();
        println!(
            "Got message: {:#?} from stream: {} with offset: {}",
            d.message()
                .data()
                .map(|data| String::from_utf8(data.to_vec()).unwrap()),
            d.stream(),
            d.offset()
        );

        received_messages = received_messages + 1;
        if received_messages == 10 {
            break;
        }
    }

    Ok(())
}
