use fake::{Fake, Faker};

use rabbitmq_stream_client::{error, Environment};
use rabbitmq_stream_protocol::ResponseCode;

use crate::common::TestEnvironment;

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_test() {
    let _ = TestEnvironment::create().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_stream_twice() {
    let env = Environment::builder().build().await.unwrap();
    let stream_to_test: String = Faker.fake();
    let response = env.stream_creator().create(&stream_to_test).await;
    assert_eq!(response.is_ok(), true);

    let response = env.stream_creator().create(&stream_to_test).await;
    assert_eq!(response.is_ok(), false);
    assert_eq!(response.is_err(), true);

    assert!(matches!(
        response.err().unwrap(),
        error::StreamCreateError::Create {
            stream: _, // ?
            status: ResponseCode::StreamAlreadyExists,
        }
    ));

    let delete_response = env.delete_stream(&stream_to_test).await;
    assert_eq!(delete_response.is_ok(), true);

    let delete_response = env.delete_stream(&stream_to_test).await;

    assert_eq!(delete_response.is_ok(), false);
    assert_eq!(delete_response.is_err(), true);

    assert!(matches!(
        delete_response.err().unwrap(),
        error::StreamDeleteError::Delete {
            stream: _, //
            status: ResponseCode::StreamDoesNotExist,
        }
    ));
}
