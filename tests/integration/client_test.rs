use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_protocol::commands::close::CloseRequest;
use tokio::sync::mpsc::channel;

use rabbitmq_stream_client::error::ClientError;
use rabbitmq_stream_client::{
    types::{
        Broker, Message, MessageResult, OffsetSpecification, ResponseCode, ResponseKind,
        StreamMetadata,
    },
    Client, ClientOptions,
};

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
            replicas: vec![],
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
async fn client_create_subscribe_twice_error_test() {
    // test the errors in case of double subscription
    // test the errors in case of double unsubscription
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
    // first consumer with id 1 it is ok
    assert_eq!(&ResponseCode::Ok, response.code());

    let response_error = test
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
    // second consumer with id 1 it is not ok since it is already used
    assert_eq!(
        &ResponseCode::SubscriptionIdAlreadyExists,
        response_error.code()
    );

    let response = test.client.unsubscribe(1).await.unwrap();
    assert_eq!(&ResponseCode::Ok, response.code());

    // trying to delete a consumer that does not exist
    let response_error = test.client.unsubscribe(1).await.unwrap();
    assert_eq!(
        &ResponseCode::SubscriptionIdDoesNotExist,
        response_error.code()
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn client_store_query_offset_error_test() {
    let test = TestClient::create().await;

    let reference: String = Faker.fake();

    // the stream exists but the offset does not
    let response_off_not_found = test
        .client
        .query_offset(reference.clone(), &test.stream)
        .await;

    // it should raise OffsetNotFound error
    match response_off_not_found {
        Ok(_) => panic!("Should not be ok"),
        Err(e) => {
            assert_eq!(
                matches!(e, ClientError::RequestError(ResponseCode::OffsetNotFound)),
                true
            )
        }
    }

    // the stream does not exist
    let response_stream_does_not_exist = test
        .client
        .query_offset(reference.clone(), "response_stream_does_not_exist")
        .await;

    // it should raise StreamDoesNotExist error
    match response_stream_does_not_exist {
        Ok(_) => panic!("Should not be ok"),
        Err(e) => {
            assert_eq!(
                matches!(
                    e,
                    ClientError::RequestError(ResponseCode::StreamDoesNotExist)
                ),
                true
            )
        }
    }
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
        .declare_publisher(1, Some(reference.clone()), &test.stream)
        .await
        .unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response = test.client.delete_publisher(1).await.unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_declare_delete_publisher_twice_error() {
    // test the errors in case of double publisher declaration
    // the first one is ok the second one is not
    // since the publisher id is already used

    let test = TestClient::create().await;
    let reference: String = Faker.fake();

    let response = test
        .client
        .declare_publisher(1, Some(reference.clone()), &test.stream)
        .await
        .unwrap();

    assert_eq!(&ResponseCode::Ok, response.code());

    let response_error = test
        .client
        .declare_publisher(1, Some(reference.clone()), &test.stream)
        .await
        .unwrap();

    assert_eq!(&ResponseCode::PrecoditionFailed, response_error.code());

    let response = test.client.delete_publisher(1).await.unwrap();
    assert_eq!(&ResponseCode::Ok, response.code());

    let response_error = test.client.delete_publisher(1).await.unwrap();
    assert_eq!(&ResponseCode::PublisherDoesNotExist, response_error.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_declare_publisher_not_existing_stream() {
    let test = TestClient::create().await;

    let reference: String = Faker.fake();

    let response = test
        .client
        .declare_publisher(1, Some(reference.clone()), "not_existing_stream")
        .await
        .unwrap();

    assert_eq!(&ResponseCode::StreamDoesNotExist, response.code());
}

#[tokio::test(flavor = "multi_thread")]
async fn client_query_publisher() {
    let test = TestClient::create().await;

    let reference: String = Faker.fake();

    let response = test
        .client
        .declare_publisher(1, Some(reference.clone()), &test.stream)
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

    let handler = move |msg: MessageResult| async move {
        if let Some(Ok(response)) = msg {
            if let ResponseKind::Deliver(delivery) = response.kind() {
                tx.send(delivery.clone()).await.unwrap()
            }
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
        .declare_publisher(1, Some(reference.clone()), &test.stream)
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

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
async fn client_handle_unexpected_connection_interruption() {
    let mut options = ClientOptions::default();
    options.set_port(5672);
    let res = Client::connect(options).await;
    assert!(matches!(res, Err(ClientError::ConnectionClosed)));
}
