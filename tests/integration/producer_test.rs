use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::offset_specification::OffsetSpecification;
use rabbitmq_stream_protocol::{message::Message, Response, ResponseKind};
use tokio::sync::mpsc::channel;

use crate::common::{TestClient, TestEnvironment};

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_ok() {
    let test = TestClient::create().await;
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();
    let (tx, mut rx) = channel(1);

    let handler = move |response: Response| async move {
        match response.kind() {
            ResponseKind::Deliver(delivery) => {
                tx.send(delivery.clone()).await.unwrap();
            }
            _ => {}
        }
        Ok(())
    };
    test.client.set_handler(handler).await;
    let _ = test
        .client
        .subscribe(
            1,
            &env.stream,
            OffsetSpecification::First,
            1,
            HashMap::new(),
        )
        .await
        .unwrap();

    let producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    let _ = producer
        .send(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    let delivery = rx.recv().await.unwrap();

    let _ = test.client.unsubscribe(1).await.unwrap();

    producer.close().await.unwrap();

    assert_eq!(1, delivery.subscription_id);
    assert_eq!(1, delivery.messages.len());
    assert_eq!(
        Some(b"message".as_ref()),
        delivery.messages.get(0).unwrap().data()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_error() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    let _ = producer
        .send(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    producer.close().await.unwrap();
}
