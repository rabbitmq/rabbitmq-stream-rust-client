use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::prelude::{Client, ClientOptions, Environment};
use rabbitmq_stream_protocol::ResponseCode;

pub struct TestClient {
    pub client: Client,
    pub stream: String,
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
        TestClient { client, stream }
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.client.delete_stream(&self.stream).await.unwrap() })
        });
    }
}

impl TestEnvironment {
    pub async fn create() -> TestEnvironment {
        let stream: String = Faker.fake();
        let env = Environment::builder().build().await.unwrap();
        let _ = env.stream_creator().create(&stream).await.unwrap();

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
