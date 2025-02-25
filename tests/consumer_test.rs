use std::time::Duration;

#[path = "./common.rs"]
mod common;

use common::*;

use fake::{Fake, Faker};
use futures::StreamExt;
use rabbitmq_stream_client::{
    error::{
        ClientError, ConsumerCloseError, ConsumerDeliveryError, ConsumerStoreOffsetError,
        ProducerCloseError,
    },
    types::{Delivery, Message, OffsetSpecification, SuperStreamConsumer},
    Consumer, FilterConfiguration, NoDedup, Producer,
};

use rabbitmq_stream_client::types::{
    HashRoutingMurmurStrategy, RoutingKeyRoutingStrategy, RoutingStrategy,
};
use rabbitmq_stream_protocol::ResponseCode;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;
use tokio::task;
use {std::sync::Arc, std::sync::Mutex};

pub fn routing_key_strategy_value_extractor(_: &Message) -> String {
    "0".to_string()
}

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
        .client_provided_name("my consumer")
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

fn hash_strategy_value_extractor(message: &Message) -> String {
    let s = String::from_utf8(Vec::from(message.data().unwrap())).expect("Found invalid UTF-8");
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn super_stream_consumer_test() {
    let env = TestEnvironment::create_super_stream().await;

    let message_count = 10;
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("test super stream consumer ")
        .build(&env.super_stream)
        .await
        .unwrap();

    let mut super_stream_consumer: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .offset(OffsetSpecification::First)
        .build(&env.super_stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let msg = Message::builder().body(format!("message{}", n)).build();
        super_stream_producer
            .send(msg, |_confirmation_status| async move {})
            .await
            .unwrap();
    }

    let mut received_messages = 0;
    let handle = super_stream_consumer.handle();

    while let Some(_) = super_stream_consumer.next().await {
        received_messages += 1;
        if received_messages == 10 {
            break;
        }
    }

    assert!(received_messages == message_count);

    super_stream_producer.close().await.unwrap();
    _ = handle.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test_offset_specification_offset() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();
    let mut messages_received = 0;
    let mut first_offset = 0;

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
        .offset(OffsetSpecification::Offset(5))
        .build(&env.stream)
        .await
        .unwrap();

    for n in 0..message_count - 1 {
        let _ = producer
            .send_with_confirm(Message::builder().body(format!("message{}", n)).build())
            .await
            .unwrap();
    }

    let _ = producer
        .send_with_confirm(
            Message::builder()
                .body(format!("marker{}", message_count - 1))
                .build(),
        )
        .await
        .unwrap();

    while let Some(delivery) = consumer.next().await {
        let d = delivery.unwrap();
        if first_offset == 0 {
            first_offset = d.offset();
        }
        messages_received += 1;

        if String::from_utf8_lossy(d.message().data().unwrap()).contains("marker") {
            break;
        }
    }

    consumer.handle().close().await.unwrap();
    producer.close().await.unwrap();

    assert!(first_offset == 5);
    assert!(messages_received == 5);
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
        Err(e) => assert!(matches!(
            e,
            rabbitmq_stream_client::error::ConsumerCreateError::StreamDoesNotExist { .. }
        )),
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
            let _result = consumer_store
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

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test_with_filtering() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let message_count = 10;
    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .filter_value_extractor(|_| "filtering".to_string())
        .build(&env.stream)
        .await
        .unwrap();

    let filter_configuration = FilterConfiguration::new(vec!["filtering".to_string()], false)
        .post_filter(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap_or("".to_string())
                == *"filtering"
        });

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::First)
        .filter_input(Some(filter_configuration))
        .build(&env.stream)
        .await
        .unwrap();

    for _ in 0..message_count {
        let _ = producer
            .send_with_confirm(Message::builder().body("filtering").build())
            .await
            .unwrap();

        let _ = producer
            .send_with_confirm(Message::builder().body("not filtering").build())
            .await
            .unwrap();
    }

    let response = Arc::new(tokio::sync::Mutex::new(vec![]));
    let response_clone = Arc::clone(&response);

    let task = tokio::task::spawn(async move {
        loop {
            let delivery = consumer.next().await.unwrap();

            let d = delivery.unwrap();
            let data = d
                .message()
                .data()
                .map(|data| String::from_utf8(data.to_vec()).unwrap())
                .unwrap();

            let mut r = response_clone.lock().await;
            r.push(data);
        }
    });

    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(3), task).await;
    let repsonse_length = response.lock().await.len();
    let filtering_response_length = response
        .lock()
        .await
        .iter()
        .filter(|item| item == &&"filtering")
        .collect::<Vec<_>>()
        .len();

    assert!(repsonse_length == filtering_response_length);
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn super_stream_consumer_test_with_filtering() {
    let env = TestEnvironment::create_super_stream().await;

    let message_count = 10;
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::RoutingKeyStrategy(
            RoutingKeyRoutingStrategy {
                routing_extractor: &routing_key_strategy_value_extractor,
            },
        ))
        .filter_value_extractor(|_| "filtering".to_string())
        .build(&env.super_stream)
        .await
        .unwrap();

    let filter_configuration = FilterConfiguration::new(vec!["filtering".to_string()], false)
        .post_filter(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap_or("".to_string())
                == *"filtering"
        });

    let mut super_stream_consumer = env
        .env
        .super_stream_consumer()
        .offset(OffsetSpecification::First)
        .filter_input(Some(filter_configuration))
        .build(&env.super_stream)
        .await
        .unwrap();

    for _ in 0..message_count {
        let _ = super_stream_producer
            .send(
                Message::builder().body("filtering").build(),
                |_| async move {},
            )
            .await;

        let _ = super_stream_producer
            .send(
                Message::builder().body("filtering").build(),
                |_| async move {},
            )
            .await;
    }

    let response = Arc::new(tokio::sync::Mutex::new(vec![]));
    let response_clone = Arc::clone(&response);

    let task = tokio::task::spawn(async move {
        loop {
            let delivery = super_stream_consumer.next().await.unwrap();

            let d = delivery.unwrap();
            let data = d
                .message()
                .data()
                .map(|data| String::from_utf8(data.to_vec()).unwrap())
                .unwrap();

            let mut r = response_clone.lock().await;
            r.push(data);
        }
    });

    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(3), task).await;
    let repsonse_length = response.lock().await.len();
    let filtering_response_length = response
        .lock()
        .await
        .iter()
        .filter(|item| item == &&"filtering")
        .collect::<Vec<_>>()
        .len();

    assert!(repsonse_length == filtering_response_length);
    super_stream_producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_test_with_filtering_match_unfiltered() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let message_count = 10;
    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .filter_value_extractor(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap()
        })
        .build(&env.stream)
        .await
        .unwrap();

    // publish filtering message
    for i in 0..message_count {
        producer
            .send_with_confirm(Message::builder().body(i.to_string()).build())
            .await
            .unwrap();
    }

    producer.close().await.unwrap();

    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    // publish unset filter value
    for i in 0..message_count {
        producer
            .send_with_confirm(Message::builder().body(i.to_string()).build())
            .await
            .unwrap();
    }

    producer.close().await.unwrap();

    let filter_configuration =
        FilterConfiguration::new(vec!["1".to_string()], true).post_filter(|message| {
            String::from_utf8(message.data().unwrap().to_vec()).unwrap_or("".to_string()) == *"1"
        });

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::First)
        .filter_input(Some(filter_configuration))
        .build(&env.stream)
        .await
        .unwrap();

    let response = Arc::new(tokio::sync::Mutex::new(vec![]));
    let response_clone = Arc::clone(&response);

    let task = tokio::task::spawn(async move {
        loop {
            let delivery = consumer.next().await.unwrap();

            let d = delivery.unwrap();
            let data = d
                .message()
                .data()
                .map(|data| String::from_utf8(data.to_vec()).unwrap())
                .unwrap();

            let mut r = response_clone.lock().await;
            r.push(data);
        }
    });

    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(3), task).await;
    let repsonse_length = response.lock().await.len();
    let filtering_response_length = response
        .lock()
        .await
        .iter()
        .filter(|item| item == &&"1")
        .collect::<Vec<_>>()
        .len();

    assert!(repsonse_length == filtering_response_length);
}

#[tokio::test(flavor = "multi_thread")]
async fn super_stream_single_active_consumer_test() {
    let env = TestEnvironment::create_super_stream().await;

    let message_count = 1000;
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("test super stream consumer ")
        .build(&env.super_stream)
        .await
        .unwrap();

    let notify_received_messages = Arc::new(Notify::new());

    let mut super_stream_consumer: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name("super-stream-with-sac-enabled")
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .build(&env.super_stream)
        .await
        .unwrap();

    let mut super_stream_consumer_2: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name("super-stream-with-sac-enabled")
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .build(&env.super_stream)
        .await
        .unwrap();

    let mut super_stream_consumer_3: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name("super-stream-with-sac-enabled")
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .build(&env.super_stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let msg = Message::builder().body(format!("message{}", n)).build();
        super_stream_producer
            .send(msg, |_confirmation_status| async move {})
            .await
            .unwrap();
    }

    let received_messages = Arc::new(AtomicU32::new(1));
    let handle_consumer_1 = super_stream_consumer.handle();
    let handle_consumer_2 = super_stream_consumer_2.handle();
    let handle_consumer_3 = super_stream_consumer_3.handle();

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer_2.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer_3.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    notify_received_messages.notified().await;

    assert!(received_messages.load(Ordering::Relaxed) == message_count + 1);

    super_stream_producer.close().await.unwrap();
    _ = handle_consumer_1.close().await;
    _ = handle_consumer_2.close().await;
    _ = handle_consumer_3.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn super_stream_single_active_consumer_test_with_callback() {
    let env = TestEnvironment::create_super_stream().await;
    let super_stream_consumer_name = "super-stream-with-sac-enabled";

    let message_count = 1000;
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("test super stream consumer ")
        .build(&env.super_stream)
        .await
        .unwrap();

    let notify_received_messages = Arc::new(Notify::new());

    let result_stream_name_1 = Arc::new(Mutex::new(String::from("")));
    let result_stream_name_2 = Arc::new(Mutex::new(String::from("")));
    let result_stream_name_3 = Arc::new(Mutex::new(String::from("")));

    let result_name_1 = Arc::new(Mutex::new(String::from("")));
    let result_name_2 = Arc::new(Mutex::new(String::from("")));
    let result_name_3 = Arc::new(Mutex::new(String::from("")));

    let result_stream_name_outer = result_stream_name_1.clone();
    let result_stream_name_2_outer = result_stream_name_2.clone();
    let result_stream_name_3_outer = result_stream_name_3.clone();

    let result_name_1_outer = result_name_1.clone();
    let result_name_2_outer = result_name_2.clone();
    let result_name_3_outer = result_name_3.clone();

    let mut super_stream_consumer: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name(super_stream_consumer_name)
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .consumer_update(move |_active, message_context| {
            let result_stream_name_int = result_stream_name_outer.clone();
            let result_consumer_name_int = result_name_1_outer.clone();
            async move {
                *result_stream_name_int.lock().unwrap() = message_context.stream().clone();
                *result_consumer_name_int.lock().unwrap() = message_context.name().clone();

                OffsetSpecification::First
            }
        })
        .build(&env.super_stream)
        .await
        .unwrap();

    let mut super_stream_consumer_2: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name("super-stream-with-sac-enabled")
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .consumer_update(move |_active, message_context| {
            let result_stream_name_int = result_stream_name_2_outer.clone();
            let result_consumer_name_int = result_name_2_outer.clone();
            async move {
                *result_stream_name_int.lock().unwrap() = message_context.stream().clone();
                *result_consumer_name_int.lock().unwrap() = message_context.name().clone();
                OffsetSpecification::First
            }
        })
        .build(&env.super_stream)
        .await
        .unwrap();

    let mut super_stream_consumer_3: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .name("super-stream-with-sac-enabled")
        .enable_single_active_consumer(true)
        .offset(OffsetSpecification::First)
        .consumer_update(move |_active, message_context| {
            let result_stream_name_int = result_stream_name_3_outer.clone();
            let result_consumer_name_int = result_name_3_outer.clone();
            async move {
                *result_stream_name_int.lock().unwrap() = message_context.stream().clone();
                *result_consumer_name_int.lock().unwrap() = message_context.name().clone();
                OffsetSpecification::First
            }
        })
        .build(&env.super_stream)
        .await
        .unwrap();

    for n in 0..message_count {
        let msg = Message::builder().body(format!("message{}", n)).build();
        super_stream_producer
            .send(msg, |_confirmation_status| async move {})
            .await
            .unwrap();
    }

    let received_messages = Arc::new(AtomicU32::new(1));
    let handle_consumer_1 = super_stream_consumer.handle();
    let handle_consumer_2 = super_stream_consumer_2.handle();
    let handle_consumer_3 = super_stream_consumer_3.handle();

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer_2.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer_3.next().await {
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
    });

    notify_received_messages.notified().await;

    assert!(received_messages.load(Ordering::Relaxed) == message_count + 1);
    assert!(env
        .partitions
        .contains(&(*result_stream_name_1.clone().lock().unwrap())));
    assert!(env
        .partitions
        .contains(&(*result_stream_name_2.clone().lock().unwrap())));
    assert!(env
        .partitions
        .contains(&(*result_stream_name_3.clone().lock().unwrap())));
    assert!(super_stream_consumer_name == *result_name_1.clone().lock().unwrap());
    assert!(super_stream_consumer_name == *result_name_2.clone().lock().unwrap());
    assert!(super_stream_consumer_name == *result_name_3.clone().lock().unwrap());

    super_stream_producer.close().await.unwrap();
    _ = handle_consumer_1.close().await;
    _ = handle_consumer_2.close().await;
    _ = handle_consumer_3.close().await;
}
