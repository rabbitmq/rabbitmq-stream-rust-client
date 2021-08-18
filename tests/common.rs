use std::collections::HashMap;

use fake::{Fake, Faker};
use rabbitmq_stream_client::{Client, ClientOptions};
use rabbitmq_stream_protocol::ResponseCode;

pub struct TestClient {
    pub client: Client,
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
