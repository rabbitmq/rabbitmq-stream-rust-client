use fake::{Fake, Faker};

use rabbitmq_stream_client::{error, Environment};
use rabbitmq_stream_protocol::ResponseCode;

use crate::common::TestEnvironment;

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_test() {
    let _ = TestEnvironment::create().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_fail_to_connect_wrong_config() {
    // test the wrong config
    // the client should fail to connect
    // and return an error

    let env = Environment::builder().host("does_not_exist").build().await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::Io { .. })
    ));

    let env = Environment::builder().port(1).build().await;
    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::Io { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_fail_to_connect_wrong_credentials() {
    let env = Environment::builder()
        .password("wrong_password")
        .build()
        .await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::RequestError(
            ResponseCode::AuthenticationFailure
        ))
    ));

    let env = Environment::builder()
        .username("wrong_username")
        .build()
        .await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::RequestError(
            ResponseCode::AuthenticationFailure
        ))
    ));

    let env = Environment::builder()
        .virtual_host("wrong_virtual_host")
        .build()
        .await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::RequestError(
            ResponseCode::AuthenticationFailure
        ))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_delete_stream_twice() {
    // In this test we don't use the TestEnvironment because we want to test
    // the error handling of the Environment::create_stream method.
    // when we create a stream twice, we expect the second call to fail.

    let env = Environment::builder().build().await.unwrap();
    let stream_to_test: String = Faker.fake();
    let response = env.stream_creator().create(&stream_to_test).await;
    assert_eq!(response.is_ok(), true);

    let response = env.stream_creator().create(&stream_to_test).await;

    assert!(matches!(
        response,
        Err(error::StreamCreateError::Create {
            stream: _, // ?
            status: ResponseCode::StreamAlreadyExists,
        })
    ));

    // The first delete should succeed since the stream was created
    let delete_response = env.delete_stream(&stream_to_test).await;
    assert_eq!(delete_response.is_ok(), true);

    // the second delete should fail since the stream was already deleted
    let delete_response = env.delete_stream(&stream_to_test).await;

    assert!(matches!(
        delete_response,
        Err(error::StreamDeleteError::Delete {
            stream: _, //
            status: ResponseCode::StreamDoesNotExist,
        })
    ));
}
