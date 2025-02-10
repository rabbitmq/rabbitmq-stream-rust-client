use std::time::Duration;

use fake::{Fake, Faker};

use rabbitmq_stream_client::types::ByteCapacity;
use rabbitmq_stream_client::{error, Environment, TlsConfiguration};
use rabbitmq_stream_protocol::ResponseCode;

use crate::common::TestEnvironment;

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_test() {
    let _ = TestEnvironment::create().await;
}

#[cfg(all(feature = "serde", test))]
mod tests {
    use rabbitmq_stream_client::ClientOptions;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_environment_build_from_client_option() {
        let j = r#"
{
    "host": "localhost",
    "tls": {
        "enabled": false
    }
}
        "#;
        let stream: String = Faker.fake();
        let client_options: ClientOptions = serde_json::from_str(j).unwrap();
        let env = Environment::from_client_option(client_options)
            .await
            .unwrap();
        env.stream_creator().create(&stream).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_and_delete_super_stream_test() {
    let super_stream = "super_stream_test";
    let env = Environment::builder().build().await.unwrap();

    let response = env
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(super_stream, 3, None)
        .await;

    assert_eq!(response.is_ok(), true);

    let response = env.delete_super_stream(super_stream).await;

    assert_eq!(response.is_ok(), true);
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
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_fail_to_connect_v_host() {
    let env = Environment::builder()
        .virtual_host("wrong_virtual_host")
        .build()
        .await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::RequestError(
            ResponseCode::VirtualHostAccessFailure
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

#[tokio::test(flavor = "multi_thread")]
async fn environment_create_streams_with_parameters() {
    // In this test we don't use the TestEnvironment because we want to test
    // The stream creation with parameters.
    // here we can just assert that the stream creation was successful
    // we cannot assert that the parameters were set correctly

    let env = Environment::builder().build().await.unwrap();
    let stream_to_test: String = Faker.fake();
    let response = env
        .stream_creator()
        .max_age(Duration::from_secs(10))
        .max_length(ByteCapacity::B(1))
        .max_segment_size(ByteCapacity::GB(1))
        .create(&stream_to_test)
        .await;
    assert_eq!(response.is_ok(), true);

    let delete_response = env.delete_stream(&stream_to_test).await;
    assert_eq!(delete_response.is_ok(), true);
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_tls_connection_trust_certificates() {
    // the test validates that the client can connect to a server
    // that uses tls and the client trusts the server certificate
    let tls_configuration: TlsConfiguration =
        TlsConfiguration::builder().enable(true).build().unwrap();

    let env = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await;

    assert!(matches!(env, Ok(Environment { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_fail_tls_connection_wrong_certificates() {
    let pwd = std::env::current_dir().unwrap();
    // here we pass the wrong certificate
    // the connection should fail with IO error
    let path = pwd
        .join(".ci/certs/server_certificate.pem") // wrong certificate
        .to_str()
        .unwrap()
        .to_string();

    let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
        .add_root_certificates(path)
        .build()
        .unwrap();

    let env = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await;

    assert!(matches!(
        env,
        Err(rabbitmq_stream_client::error::ClientError::Io { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_tls_connection_with_root_ca() {
    let pwd = std::env::current_dir().unwrap();
    let path = pwd
        .join(".ci/certs/ca_certificate.pem")
        .to_str()
        .unwrap()
        .to_string();

    let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
        .add_root_certificates(path)
        .build()
        .unwrap();

    let env = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await;

    assert!(matches!(env, Ok(Environment { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_tls_connection_with_root_ca_and_client_certificates() {
    // the test validates that the client can connect to a server
    // that uses tls and the client certificates trusted by the server
    // to have a 100% the ssl_options.fail_if_no_peer_cert should be
    // ssl_options.fail_if_no_peer_cert = true
    // but for the scope of this test the current server configuration is enough
    let pwd = std::env::current_dir().unwrap();
    let path_ca = pwd
        .join(".ci/certs/ca_certificate.pem")
        .to_str()
        .unwrap()
        .to_string();

    let path_client_cert = pwd
        .join(".ci/certs/client_certificate.pem")
        .to_str()
        .unwrap()
        .to_string();

    let path_client_key = pwd
        .join(".ci/certs/client_key.pem")
        .to_str()
        .unwrap()
        .to_string();

    let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
        .add_root_certificates(path_ca)
        .add_client_certificates_keys(path_client_cert, path_client_key)
        .build()
        .unwrap();

    let env = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await;

    assert!(matches!(env, Ok(Environment { .. })));
}
