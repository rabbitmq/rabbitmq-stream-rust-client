use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::{
    metadata::{Broker, StreamMetadata},
    Client, ClientOptions,
};
use rabbitmq_stream_protocol::ResponseCode;
mod common;

use common::TestClient;
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
