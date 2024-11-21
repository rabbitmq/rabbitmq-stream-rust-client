use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode, SuperStreamConsumer,
};

pub async fn start_consumer(consumer_name: String) -> Result<(), Box<dyn std::error::Error>> {
    use rabbitmq_stream_client::Environment;
    let environment = Environment::builder().build().await?;
    let mut consumer_name_tmp = String::from("");


    println!("Super Stream Consumer connected to RabbitMQ. ConsumerName {}", consumer_name);

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
                    println!("Error creating stream: {:?} {:?}", stream, err);
                }
            }
        }
    }
    println!(
        "Super stream consumer example, consuming messages from the super stream {}",
        crate::SUPER_STREAM
    );

    let mut super_stream_consumer: SuperStreamConsumer = environment
        .super_stream_consumer()
        // Mandatory if sac is enabled
        .name("consumer-group-1")
        .offset(OffsetSpecification::First)
        .enable_single_active_consumer(true)
        .client_provided_name("my super stream consumer for hello rust")
        .consumer_update(move |active, message_context| async move {
            let name = message_context.name();
            let stream = message_context.stream();
            let client = message_context.client();

            println!(
                "single active consumer: is active: {} on stream: {} with consumer_name: {}",
                active, stream, name
            );
            let stored_offset = client.query_offset(name, stream.as_str()).await;

            if let Err(_) = stored_offset {
                return OffsetSpecification::First;
            }
            let stored_offset_u = stored_offset.unwrap();
            println!("offset: {} stored", stored_offset_u.clone());
            OffsetSpecification::Offset(stored_offset_u)
        })
        .build(crate::SUPER_STREAM)
        .await
        .unwrap();

    loop {
        let delivery = super_stream_consumer.next().await.unwrap();
        {
            let delivery = delivery.unwrap();
            println!(
                "Consumer name {}: Got message: {:#?} from stream: {} with offset: {}",
                consumer_name,
                delivery
                    .message()
                    .data()
                    .map(|data| String::from_utf8(data.to_vec()).unwrap())
                    .unwrap(),
                delivery.stream(),
                delivery.offset()
            );

            // Store an offset for every consumer
            // store the offset each time a message is consumed
            // that is not a best practice, but it is done here for demonstration purposes
            super_stream_consumer
                .client()
                .store_offset(
                    delivery.consumer_name().unwrap().as_str(),
                    delivery.stream().as_str(),
                    delivery.offset(),
                )
                .await?;
        }
    }
}
