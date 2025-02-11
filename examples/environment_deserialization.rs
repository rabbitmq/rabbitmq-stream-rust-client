#[cfg(feature = "serde")]
mod example {
    use tracing::{info, Level};
    use tracing_subscriber::FmtSubscriber;

    use futures::StreamExt;
    use rabbitmq_stream_client::{
        types::{ByteCapacity, Message, OffsetSpecification},
        ClientOptions, Environment,
    };
    use serde::*;

    #[derive(Deserialize)]
    struct QueueConfig {
        #[serde(flatten)]
        rabbitmq: ClientOptions,
        stream_name: String,
        capabity: ByteCapacity,
        producer_client_name: String,
        consumer_client_name: String,
    }

    #[derive(Deserialize)]
    struct MyConfig {
        queue_config: QueueConfig,
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        // The configuration is loadable from a file.
        // Here, just for semplification, we use a JSON string.
        // NB: we use `serde` internally, so you can use any format supported by serde.
        let j = r#"
{
    "queue_config": {
        "host": "localhost",
        "tls": {
            "enabled": false
        },
        "stream_name": "test",
        "capabity": "5GB",
        "producer_client_name": "producer",
        "consumer_client_name": "consumer"
    }
}
        "#;
        let my_config: MyConfig = serde_json::from_str(j).unwrap();
        let environment = Environment::from_client_option(my_config.queue_config.rabbitmq).await?;

        let message_count = 10;
        environment
            .stream_creator()
            .max_length(my_config.queue_config.capabity)
            .create(&my_config.queue_config.stream_name)
            .await?;

        let producer = environment
            .producer()
            .client_provided_name(&my_config.queue_config.producer_client_name)
            .build(&my_config.queue_config.stream_name)
            .await?;

        for i in 0..message_count {
            producer
                .send_with_confirm(Message::builder().body(format!("message{}", i)).build())
                .await?;
        }

        producer.close().await?;

        let mut consumer = environment
            .consumer()
            .client_provided_name(&my_config.queue_config.consumer_client_name)
            .offset(OffsetSpecification::First)
            .build(&my_config.queue_config.stream_name)
            .await
            .unwrap();

        for _ in 0..message_count {
            let delivery = consumer.next().await.unwrap()?;
            info!(
                "Got message : {:?} with offset {}",
                delivery
                    .message()
                    .data()
                    .map(|data| String::from_utf8(data.to_vec())),
                delivery.offset()
            );
        }

        consumer.handle().close().await.unwrap();

        environment.delete_stream("test").await?;

        Ok(())
    }
}
#[cfg(not(feature = "serde"))]
mod example {
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        panic!("This example requires the 'serde' feature to be enabled. Add `--features serde` to the cargo command.");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    example::run().await?;

    Ok(())
}
