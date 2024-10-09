use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::{Client, ClientOptions, Environment};
use rabbitmq_stream_protocol::ResponseCode;

pub struct TestClient {
    pub client: Client,
    pub stream: String,
    pub super_stream: String,
    pub partitions: Vec<String>,
}

pub struct TestEnvironment {
    pub env: Environment,
    pub stream: String,
}

impl TestClient {
    pub async fn create() -> TestClient {
        let stream: String = Faker.fake();
        let client = Client::connect(ClientOptions::default()).await.unwrap();

        let response = client.create_stream(&stream, HashMap::new()).await.unwrap();

        assert_eq!(&ResponseCode::Ok, response.code());
        TestClient {
            client,
            stream,
            super_stream: String::new(),
            partitions: Vec::new(),
        }
    }

    pub async fn create_super_stream() -> TestClient {
        let super_stream: String = Faker.fake();
        let client = Client::connect(ClientOptions::default()).await.unwrap();

        let partitions: Vec<String> = [
            super_stream.to_string() + "-0",
            super_stream.to_string() + "-1",
            super_stream.to_string() + "-2",
        ]
        .iter()
        .map(|x| x.into())
        .collect();

        let binding_keys: Vec<String> = ["0", "1", "2"].iter().map(|&x| x.into()).collect();

        let response = client
            .create_super_stream(
                &super_stream,
                partitions.clone(),
                binding_keys,
                HashMap::new(),
            )
            .await
            .unwrap();

        assert_eq!(&ResponseCode::Ok, response.code());
        TestClient {
            client,
            stream: String::new(),
            super_stream,
            partitions,
        }
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        if self.stream != "" {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { self.client.delete_stream(&self.stream).await.unwrap() })
            });
        }
        if self.super_stream != "" {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    self.client
                        .delete_super_stream(&self.super_stream)
                        .await
                        .unwrap()
                })
            });
        }
    }
}

impl TestEnvironment {
    pub async fn create() -> TestEnvironment {
        let stream: String = Faker.fake();
        let env = Environment::builder().build().await.unwrap();
        env.stream_creator().create(&stream).await.unwrap();

        TestEnvironment { env, stream }
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.env.delete_stream(&self.stream).await.unwrap() })
        });
    }
}
