use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::{
    offset_specification::OffsetSpecification,
    prelude::{Broker, Client, ClientOptions, StreamMetadata},
};
use rabbitmq_stream_protocol::{Response, ResponseCode, ResponseKind};

use rabbitmq_stream_protocol::message::Message;
use tokio::sync::mpsc::channel;

use crate::common::TestClient;
#[tokio::test]
async fn client_connection_test() {
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    assert_ne!(client.server_properties().await.len(), 0);
    assert_ne!(client.connection_properties().await.len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn client_create_stream_test() {
    TestClient::create().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn client_create_stream_error_test() {
    let test = TestClient::create().await;

    // Second ko
    let response = test
        .client
        .create_stream(&test.stream, HashMap::new())
        .await
        .unwrap();
    assert_eq!(&ResponseCode::StreamAlreadyExists, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_delete_stream_test() {
    let test = TestClient::create().await;

    let response = test.client.delete_stream(&test.stream).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test]
async fn client_delete_stream_error_test() {
    let stream: String = Faker.fake();
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    let response = client.delete_stream(&stream).await.unwrap();
    assert_eq!(&ResponseCode::StreamDoesNotExist, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_metadata_test() {
    let test = TestClient::create().await;

    let response = test
        .client
        .metadata(vec![test.stream.clone()])
        .await
        .unwrap();

    assert_eq!(
        Some(&StreamMetadata {
            stream: test.stream.clone(),
            response_code: ResponseCode::Ok,
            leader: Broker {
                host: String::from("localhost"),
                port: 5552,
            },
            replicas: vec![]
        }),
        response.get(&test.stream)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn client_create_subscribe_test() {
    let test = TestClient::create().await;

    let response = test
        .client
        .subscribe(
            1,
            &test.stream,
            OffsetSpecification::Next,
            1,
            HashMap::new(),
        )
        .await
        .unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response = test.client.unsubscribe(1).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_store_and_query_offset_test() {
    let test = TestClient::create().await;

    let offset: u64 = Faker.fake();
    let reference: String = Faker.fake();

    test.client
        .store_offset(&reference, &test.stream, offset)
        .await
        .unwrap();

    let response = test
        .client
        .query_offset(reference.clone(), &test.stream)
        .await
        .unwrap();

    assert_eq!(offset, response);
}

/*
 * Do not close a stream with a publisher declared.
 * It turns out to unparsable response
 */
#[tokio::test(flavor = "multi_thread")]
async fn client_declare_delete_publisher() {
    let test = TestClient::create().await;

    let reference: String = Faker.fake();

    let response = test
        .client
        .declare_publisher(1, &reference, &test.stream)
        .await
        .unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response = test.client.delete_publisher(1).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_query_publisher() {
    let test = TestClient::create().await;

    let reference: String = Faker.fake();

    let response = test
        .client
        .declare_publisher(1, &reference, &test.stream)
        .await
        .unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response = test
        .client
        .query_publisher_sequence(&reference, &test.stream)
        .await
        .unwrap();

    assert_eq!(0, response);

    let response = test.client.delete_publisher(1).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_publish() {
    let test = TestClient::create().await;

    let (tx, mut rx) = channel(1);
    let reference: String = Faker.fake();

    let handler = move |response: Response| async move {
        match response.kind() {
            ResponseKind::Deliver(delivery) => {
                tx.send(delivery.clone()).await.unwrap();
            }
            _ => {}
        }
        Ok(())
    };
    let _ = test
        .client
        .subscribe(
            1,
            &test.stream,
            OffsetSpecification::First,
            1,
            HashMap::new(),
        )
        .await
        .unwrap();

    test.client.set_handler(handler).await;

    let _ = test
        .client
        .declare_publisher(1, &reference, &test.stream)
        .await
        .unwrap();

    let sequences = test
        .client
        .publish(1, Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    assert_eq!(1, sequences.len());
    let delivery = rx.recv().await.unwrap();

    let _ = test.client.unsubscribe(1).await.unwrap();
    let _ = test.client.delete_publisher(1).await.unwrap();

    assert_eq!(1, delivery.subscription_id);
    assert_eq!(1, delivery.messages.len());
    assert_eq!(
        Some(b"message".as_ref()),
        delivery.messages.get(0).unwrap().data()
    );
}
