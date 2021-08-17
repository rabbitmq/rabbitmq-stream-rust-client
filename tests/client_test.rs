use fake::{Fake, Faker};
use rabbitmq_stream_client::{Client, ClientOptions};
use rabbitmq_stream_protocol::ResponseCode;

#[tokio::test]
async fn client_connection_test() {
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    assert_ne!(client.server_properties().await.len(), 0);
    assert_ne!(client.connection_properties().await.len(), 0);
}

#[tokio::test]
async fn client_create_stream_test() {
    let stream: String = Faker.fake();
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    let response = client.create_stream(&stream).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test]
async fn client_create_stream_error_test() {
    let stream: String = Faker.fake();
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    // First ok
    let response = client.create_stream(&stream).await.unwrap();
    assert_eq!(&ResponseCode::Ok, response.code());

    // Second ko
    let response = client.create_stream(&stream).await.unwrap();
    assert_eq!(&ResponseCode::StreamAlreadyExists, response.code());
}

#[tokio::test]
async fn client_delete_stream_test() {
    let stream: String = Faker.fake();
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    let response = client.create_stream(&stream).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response = client.delete_stream(&stream).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test]
async fn client_delete_stream_error_test() {
    let stream: String = Faker.fake();
    let client = Client::connect(ClientOptions::default()).await.unwrap();

    let response = client.delete_stream(&stream).await.unwrap();
    assert_eq!(&ResponseCode::StreamDoesNotExist, response.code());
}
