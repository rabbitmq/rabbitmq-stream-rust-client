/* Receives just Messages where filter California is applied.
This is assured by having added to the vector filter_values of FilterConfiguration the value California
and by the post_filter function to skip false positives
*/

use futures::StreamExt;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::types::ResponseCode;
use rabbitmq_stream_client::types::{ByteCapacity, OffsetSpecification};
use rabbitmq_stream_client::{Environment, FilterConfiguration};
use std::convert::TryInto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = "test_stream_filtering";
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .build()
        .await?;

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

    // filter configuration: https://www.rabbitmq.com/blog/2023/10/16/stream-filtering
    // We are telling the Consumer to ask the server just messages with filter California
    // The post_filler is Optional and needed to skip false positives
    let filter_configuration = FilterConfiguration::new(vec!["California".to_string()], false)
        .post_filter(|message| {
            let region: String = message
                .application_properties()
                .unwrap()
                .get("region")
                .unwrap()
                .clone()
                .try_into()
                .unwrap();

            region == "California".to_string()
        });

    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .filter_input(Some(filter_configuration))
        .build(stream)
        .await
        .unwrap();

    // Just Messages with filter California will appear
    while let Some(delivery) = consumer.next().await {
        let d = delivery.unwrap();
        println!(
            "Got message : {:?} with offset {}",
            d.message()
                .data()
                .map(|data| String::from_utf8(data.to_vec())),
            d.offset()
        );
    }

    Ok(())
}
