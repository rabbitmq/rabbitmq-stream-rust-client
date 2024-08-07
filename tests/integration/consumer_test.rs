use std::time::Duration;

use crate::common::TestEnvironment;
use fake::{Fake, Faker};
use futures::StreamExt;
use rabbitmq_stream_client::{
    error::{
        ClientError, ConsumerCloseError, ConsumerDeliveryError, ConsumerStoreOffsetError,
        ProducerCloseError,
    },
    types::{Delivery, Message, OffsetSpecification},
    Consumer, NoDedup, Producer,
};

use rabbitmq_stream_protocol::ResponseCode;

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let message_count = 10;
    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let _ = producer
            .send_with_confirm(Message::builder().body(format!("message{}", n)).build())
            .await
            .unwrap();
    }

    for _ in 0..message_count {
        let delivery = consumer.next().await.unwrap();
        let data = String::from_utf8(delivery.unwrap().message().data().unwrap().to_vec()).unwrap();
        assert!(data.contains("message"));
    }

    consumer.handle().close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_close_test() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    let _ = producer
        .send_with_confirm(Message::builder().body("message").build())
        .await
        .unwrap();

    let handle = consumer.handle();
    let delivery = consumer.next().await;

    assert!(!consumer.is_closed());
    assert!(delivery.is_some());

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.close().await.unwrap();
    });

    let delivery = consumer.next().await;

    assert!(delivery.is_none());
    assert!(consumer.is_closed());

    assert!(matches!(
        consumer.handle().close().await,
        Err(ConsumerCloseError::AlreadyClosed),
    ));
    producer.clone().close().await.unwrap();

    assert!(matches!(
        producer.close().await,
        Err(ProducerCloseError::AlreadyClosed),
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_safe_test() {
    let message_body = "test0";

    struct Wrapper {
        producer: Producer<NoDedup>,
        consumer: Consumer,
    }

    impl Wrapper {
        pub async fn init() -> Self {
            let env = TestEnvironment::create().await;
            let producer = env.env.producer().build(&env.stream).await.unwrap();
            let consumer = env
                .env
                .consumer()
                .offset(OffsetSpecification::Next)
                .build(&env.stream)
                .await
                .unwrap();
            Self { producer, consumer }
        }

        pub async fn produce(&mut self, body: &str) {
            self.producer
                .send_with_confirm(Message::builder().body(body).build())
                .await
                .unwrap();
        }

        pub async fn consume(&mut self) -> Option<Result<Delivery, ConsumerDeliveryError>> {
            self.consumer.next().await
        }

        pub async fn close(self) {
            self.producer.close().await.unwrap();
            self.consumer.handle().close().await.unwrap();
        }
    }

    // move the Wrapper struct to test that Consumer and Producer are thread-safe
    tokio::spawn(async move {
        let mut wrapper = Wrapper::init().await;
        wrapper.produce(message_body).await;
        let delivery = wrapper.consume().await.unwrap();

        let data = String::from_utf8(delivery.unwrap().message().data().unwrap().to_vec()).unwrap();
        assert_eq!(data, message_body);
        tokio::time::sleep(Duration::from_millis(100)).await;
        wrapper.close().await;
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_create_stream_not_existing_error() {
    let env = TestEnvironment::create().await;
    let consumer = env.env.consumer().build("stream_not_existing").await;

    match consumer {
        Err(e) => assert_eq!(
            matches!(
                e,
                rabbitmq_stream_client::error::ConsumerCreateError::StreamDoesNotExist { .. }
            ),
            true
        ),
        _ => panic!("Should be StreamNotFound error"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_store_and_query_offset_test() {
    let env = TestEnvironment::create().await;
    let consumer = env
        .env
        .consumer()
        .name("test-name")
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    let offset: u64 = Faker.fake();

    consumer.store_offset(offset).await.unwrap();

    let response = consumer.query_offset().await.unwrap();

    assert_eq!(offset, response);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_query_missing_offset_test() {
    let env = TestEnvironment::create().await;
    let consumer = env
        .env
        .consumer()
        .name("test-name-new")
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    assert!(matches!(
        consumer.query_offset().await,
        Err(ConsumerStoreOffsetError::Client(ClientError::RequestError(
            ResponseCode::OffsetNotFound
        ))),
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_store_and_query_offset_missing_name_test() {
    let env = TestEnvironment::create().await;
    let consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();

    let offset: u64 = Faker.fake();

    assert!(matches!(
        consumer.store_offset(offset).await,
        Err(ConsumerStoreOffsetError::NameMissing),
    ));

    assert!(matches!(
        consumer.query_offset().await,
        Err(ConsumerStoreOffsetError::NameMissing),
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test_with_store_offset() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();
    let offset_to_store = 4;

    let message_count = 10;
    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    let mut consumer_store = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .name("consumer-1")
        .build(&env.stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let _ = producer
            .send_with_confirm(Message::builder().body(format!("message{}", n)).build())
            .await
            .unwrap();
    }

    for i in 0..message_count {
        let delivery = consumer_store.next().await.unwrap();

        // Store an offset
        if i == offset_to_store {
            //Store the 5th element produced
            let result = consumer_store
                .store_offset(delivery.unwrap().offset())
                .await;
        }
    }

    consumer_store.handle().close().await.unwrap();

    let consumer_query = env
        .env
        .consumer()
        .offset(OffsetSpecification::First)
        .name("consumer-1")
        .build(&env.stream)
        .await
        .unwrap();

    let offset = consumer_query.query_offset().await.unwrap();

    assert!(offset == offset_to_store);

    consumer_query.handle().close().await.unwrap();
    producer.close().await.unwrap();
}
