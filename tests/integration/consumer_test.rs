use crate::common::TestEnvironment;
use fake::{Fake, Faker};
use futures::StreamExt;
use rabbitmq_stream_client::types::{Message, OffsetSpecification};

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let message_count = 10;
    let producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let _ = producer
            .send(Message::builder().body(format!("message{}", n)).build())
            .await
            .unwrap();
    }

    let mut counter = 0;

    while let Some(delivery) = consumer.next().await {
        let data = String::from_utf8(delivery.message.data().unwrap().to_vec()).unwrap();
        assert!(data.contains("message"));
        counter += 1;
        if counter == message_count {
            consumer.handle().close().await.unwrap();
            break;
        }
    }

    producer.close().await.unwrap();
}
