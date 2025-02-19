use std::{collections::HashMap, future::Future, sync::Arc};

use fake::{Fake, Faker};
use rabbitmq_stream_client::{Client, ClientOptions, Environment};
use rabbitmq_stream_protocol::commands::generic::GenericResponse;
use rabbitmq_stream_protocol::ResponseCode;
use tokio::sync::Semaphore;

pub struct TestClient {
    pub client: Client,
    pub stream: String,
    pub super_stream: String,
    pub partitions: Vec<String>,
}

#[derive(Clone)]
pub struct Countdown(Arc<Semaphore>);

impl Drop for Countdown {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

impl Countdown {
    pub fn new(n: u32) -> (Self, impl Future + Send) {
        let sem = Arc::new(Semaphore::new(0));
        let latch = Self(sem.clone());

        let wait = async move {
            let _ = sem.acquire_many(n).await;
        };

        (latch, wait)
    }
}

pub struct TestEnvironment {
    pub env: Environment,
    pub stream: String,
    pub super_stream: String,
    pub partitions: Vec<String>,
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

        let (response, partitions) = create_generic_super_stream(&super_stream, &client).await;

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
                tokio::runtime::Handle::current().block_on(async {
                    let r = self.client.delete_stream(&self.stream).await;
                    if let Err(e) = r {
                        eprintln!("Error deleting stream: {:?}", e);
                    }
                })
            });
        }
        if self.super_stream != "" {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let r = self.client.delete_super_stream(&self.super_stream).await;
                    if let Err(e) = r {
                        eprintln!("Error deleting super stream: {:?}", e);
                    }
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

        TestEnvironment {
            env,
            stream,
            super_stream: String::new(),
            partitions: Vec::new(),
        }
    }

    pub async fn create_super_stream() -> TestEnvironment {
        let super_stream: String = Faker.fake();
        let client = Client::connect(ClientOptions::default()).await.unwrap();
        let env = Environment::builder().build().await.unwrap();

        let (response, partitions) = create_generic_super_stream(&super_stream, &client).await;

        assert_eq!(&ResponseCode::Ok, response.code());
        TestEnvironment {
            env,
            stream: String::new(),
            super_stream,
            partitions,
        }
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        if self.stream != "" {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let r = self.env.delete_stream(&self.stream).await;
                    // Since we generate stream name randomly,
                    // it doesn't matter if the deletion goes wrong
                    if let Err(e) = r {
                        eprintln!("Error deleting stream: {:?}", e);
                    }
                })
            });
        }
        if self.super_stream != "" {
            tokio::task::block_in_place(|| {
                println!("Deleting super stream: {}", self.super_stream);
                tokio::runtime::Handle::current().block_on(async {
                    let r = self.env.delete_super_stream(&self.super_stream).await;
                    // Since we generate super stream name randomly,
                    // it doesn't matter if the deletion goes wrong
                    if let Err(e) = r {
                        eprintln!("Error deleting super stream: {:?}", e);
                    }
                })
            });
        }
    }
}

pub async fn create_generic_super_stream(
    super_stream: &String,
    client: &Client,
) -> (GenericResponse, Vec<String>) {
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

    return (response, partitions);
}
