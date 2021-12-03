use std::time::Duration;

use crate::common::TestEnvironment;
use fake::{Fake, Faker};
use futures::StreamExt;
use rabbitmq_stream_client::{
    error::{ConsumerCloseError, ProducerCloseError},
    types::{Message, OffsetSpecification},
};

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
            .send_with_confirm(Message::builder().body(format!("message{}", n)).build())
            .await
            .unwrap();
    }

    for _ in 0..message_count {
        let delivery = consumer.next().await.unwrap();
        let data = String::from_utf8(delivery.unwrap().message().data().unwrap().to_vec()).unwrap();
        assert!(data.contains("message"));
    }

    consumer.handle().close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_close_test() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    let _ = producer
        .send_with_confirm(Message::builder().body("message").build())
        .await
        .unwrap();

    let handle = consumer.handle();
    let delivery = consumer.next().await;

    assert_eq!(false, consumer.is_closed());
    assert!(delivery.is_some());

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.close().await.unwrap();
    });

    let delivery = consumer.next().await;

    assert!(delivery.is_none());
    assert_eq!(true, consumer.is_closed());

    assert!(matches!(
        consumer.handle().close().await,
        Err(ConsumerCloseError::AlreadyClosed),
    ));
    producer.clone().close().await.unwrap();

    assert!(matches!(
        producer.close().await,
        Err(ProducerCloseError::AlreadyClosed),
    ));
}
