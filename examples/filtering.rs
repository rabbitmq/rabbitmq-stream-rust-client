use futures::StreamExt;
use rabbitmq_stream_client::types::{Message, OffsetSpecification};
use rabbitmq_stream_client::{Environment, FilterConfiguration};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

    let message_count = 10;
    environment.stream_creator().create("test").await?;

    let mut producer = environment
        .producer()
        .name("test_producer")
        .filter_value_extractor(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap()
        })
        .build("test")
        .await?;

    // publish filtering message
    for i in 0..message_count {
        producer
            .send_with_confirm(Message::builder().body(i.to_string()).build())
            .await?;
    }

    producer.close().await?;

    // publish filtering message
    let mut producer = environment
        .producer()
        .name("test_producer")
        .build("test")
        .await?;

    // publish unset filter value
    for i in 0..message_count {
        producer
            .send_with_confirm(Message::builder().body(i.to_string()).build())
            .await?;
    }

    producer.close().await?;

    // filter configuration: https://www.rabbitmq.com/blog/2023/10/16/stream-filtering
    let filter_configuration =
        FilterConfiguration::new(vec!["1".to_string()], false).post_filter(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap_or("".to_string())
                == "1".to_string()
        });
    // let filter_configuration = FilterConfiguration::new(vec!["1".to_string()], true);

    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .filter_input(Some(filter_configuration))
        .build("test")
        .await
        .unwrap();

    let task = tokio::task::spawn(async move {
        loop {
            let delivery = consumer.next().await.unwrap().unwrap();
            info!(
                "Got message : {:?} with offset {}",
                delivery
                    .message()
                    .data()
                    .map(|data| String::from_utf8(data.to_vec())),
                delivery.offset()
            );
        }
    });
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(3), task).await;

    environment.delete_stream("test").await?;
    Ok(())
}
