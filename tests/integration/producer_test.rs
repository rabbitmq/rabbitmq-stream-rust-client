use std::{collections::HashSet, sync::Arc};

use chrono::Utc;
use fake::{Fake, Faker};
use futures::{lock::Mutex, StreamExt};
use tokio::sync::mpsc::channel;

use rabbitmq_stream_client::{
    types::{Message, OffsetSpecification, SimpleValue},
    Environment,
};

use crate::common::{Countdown, TestEnvironment};

use rabbitmq_stream_client::types::{
    HashRoutingMurmurStrategy, RoutingKeyRoutingStrategy,
    RoutingStrategy,
};

use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_no_name_ok() {
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
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    producer.close().await.unwrap();

    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(1, delivery.subscription_id());
    assert_eq!(Some(b"message".as_ref()), delivery.message().data());

    consumer.handle().close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_name_deduplication_unique_ids() {
    let env = TestEnvironment::create().await;

    type Ids = Arc<Mutex<HashSet<u64>>>;

    let ids = Ids::default();

    let run = move |times, env: Environment, stream: String, ids: Ids| async move {
        let (countdown, wait) = Countdown::new(times);
        let mut producer = env
            .producer()
            .name("my_producer")
            .build(&stream)
            .await
            .unwrap();

        for _ in 0..times {
            let cloned_ids = ids.clone();
            let countdown = countdown.clone();
            let _ = producer
                .send(
                    Message::builder().body(b"message".to_vec()).build(),
                    move |result| {
                        let value = cloned_ids.clone();

                        let countdown = countdown.clone();
                        async move {
                            let _countdown = countdown;
                            let id = result.unwrap().publishing_id();
                            let mut guard = value.lock().await;
                            guard.insert(id);
                        }
                    },
                )
                .await
                .unwrap();
        }

        wait.await;

        producer.close().await.unwrap();
    };

    run(10, env.env.clone(), env.stream.to_string(), ids.clone()).await;

    run(10, env.env.clone(), env.stream.to_string(), ids.clone()).await;

    assert_eq!(20, ids.lock().await.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_name_with_deduplication_ok() {
    let env = TestEnvironment::create().await;

    let mut producer = env
        .env
        .producer()
        .name("my_producer")
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

    let _ = producer
        .send_with_confirm(Message::builder().body(b"message0".to_vec()).build())
        .await
        .unwrap();

    // this is not published
    let _ = producer
        .send_with_confirm(
            Message::builder()
                .body(b"message0".to_vec())
                .publising_id(0)
                .build(),
        )
        .await
        .unwrap();

    let _ = producer
        .send_with_confirm(Message::builder().body(b"message1".to_vec()).build())
        .await
        .unwrap();

    producer.close().await.unwrap();

    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(1, delivery.subscription_id());
    assert_eq!(Some(b"message0".as_ref()), delivery.message().data());

    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(1, delivery.subscription_id());
    assert_eq!(Some(b"message1".as_ref()), delivery.message().data());

    consumer.handle().close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_batch_name_with_deduplication_ok() {
    let env = TestEnvironment::create().await;

    let mut producer = env
        .env
        .producer()
        .name("my_producer")
        .build(&env.stream)
        .await
        .unwrap();

    let result = producer
        .batch_send_with_confirm(vec![
            // confirmed
            Message::builder()
                .body(b"message".to_vec())
                .publising_id(0)
                .build(),
            // this won't be confirmed
            // since it will skipped by deduplication
            Message::builder()
                .body(b"message".to_vec())
                .publising_id(0)
                .build(),
            // confirmed since the publishing id is different
            Message::builder()
                .body(b"message".to_vec())
                .publising_id(1)
                .build(),
            // not confirmed since the publishing id is the same
            // message will be skipped by deduplication
            Message::builder()
                .body(b"message".to_vec())
                .publising_id(1)
                .build(),
        ])
        .await
        .unwrap();
    // only 2 messages are confirmed
    assert_eq!(2, result.len());
    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_with_callback() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let (tx, mut rx) = channel(1);
    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    producer
        .send(
            Message::builder().body(b"message".to_vec()).build(),
            move |confirm_result| {
                let inner_tx = tx.clone();
                async move {
                    let _ = inner_tx.send(confirm_result).await;
                }
            },
        )
        .await
        .unwrap();

    let result = rx.recv().await.unwrap();

    assert_eq!(0, result.unwrap().publishing_id());

    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_batch_send_with_callback() {
    let env = TestEnvironment::create().await;

    let (tx, mut rx) = channel(1);
    let producer = env.env.producer().build(&env.stream).await.unwrap();

    producer
        .batch_send(
            vec![Message::builder().body(b"message".to_vec()).build()],
            move |confirm_result| {
                let inner_tx = tx.clone();
                async move {
                    let _ = inner_tx.send(confirm_result).await;
                }
            },
        )
        .await
        .unwrap();

    let result = rx.recv().await.unwrap().unwrap();

    assert_eq!(0, result.publishing_id());
    assert!(result.confirmed());
    assert_eq!(Some(b"message".as_ref()), result.message().data());
    assert!(result.message().publishing_id().is_none());

    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_batch_send() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();

    let result = producer
        .batch_send_with_confirm(vec![Message::builder().body(b"message".to_vec()).build()])
        .await
        .unwrap();

    assert_eq!(1, result.len());

    let confirmation = result.get(0).unwrap();
    assert_eq!(0, confirmation.publishing_id());
    assert!(confirmation.confirmed());
    assert_eq!(Some(b"message".as_ref()), confirmation.message().data());
    assert!(confirmation.message().publishing_id().is_none());

    producer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_with_complex_message_ok() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();

    let mut consumer = env
        .env
        .consumer()
        .offset(OffsetSpecification::Next)
        .build(&env.stream)
        .await
        .unwrap();
    let now = Utc::now();
    let _ = producer
        .send_with_confirm(
            Message::builder()
                .body(b"message".to_vec())
                .application_properties()
                .insert("test_key", "test_value")
                .message_builder()
                .properties()
                .user_id("test_user")
                .correlation_id(444u64)
                .absolute_expiry_time(now)
                .content_encoding("deflate")
                .content_type("application/json")
                .group_id("test_group")
                .group_sequence(1u32)
                .reply_to("test_reply")
                .subject("test_subject")
                .to("test_to")
                .creation_time(Utc::now())
                .reply_to_group_id("test_reply_group")
                .message_id(32u64)
                .message_builder()
                .message_annotations()
                .insert("test_string", "string_value")
                .insert("test_bool", true)
                .insert("test_number", 3u8)
                .message_builder()
                .build(),
        )
        .await
        .unwrap();

    producer.close().await.unwrap();

    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(1, delivery.subscription_id());

    let message = delivery.message();
    assert_eq!(Some(b"message".as_ref()), message.data());
    let properties = message.properties();

    assert_eq!(
        Some(32u64.into()),
        properties.and_then(|properties| properties.message_id.clone())
    );

    assert_eq!(
        Some("test_user".into()),
        properties.and_then(|properties| properties.user_id.clone())
    );

    assert_eq!(
        Some("test_group".into()),
        properties.and_then(|properties| properties.group_id.clone())
    );

    assert_eq!(
        Some("test_reply".into()),
        properties.and_then(|properties| properties.reply_to.clone())
    );

    assert_eq!(
        Some("test_subject".into()),
        properties.and_then(|properties| properties.subject.clone())
    );

    assert_eq!(
        Some("test_to".into()),
        properties.and_then(|properties| properties.to.clone())
    );

    assert_eq!(
        Some("test_reply_group".into()),
        properties.and_then(|properties| properties.reply_to_group_id.clone())
    );

    assert_eq!(
        Some(444u64.into()),
        properties.and_then(|properties| properties.correlation_id.clone())
    );

    assert_eq!(
        Some(1u32.into()),
        properties.and_then(|properties| properties.group_sequence.clone())
    );

    assert_eq!(
        Some("deflate".into()),
        properties.and_then(|properties| properties.content_encoding.clone())
    );

    assert_eq!(
        Some("application/json".into()),
        properties.and_then(|properties| properties.content_type.clone())
    );

    let message_annotations = message.message_annotations();

    assert_eq!(
        Some("string_value".into()),
        message_annotations.and_then(|annotations| annotations.get("test_string").cloned())
    );

    assert_eq!(
        Some(true.into()),
        message_annotations.and_then(|annotations| annotations.get("test_bool").cloned())
    );

    assert_eq!(
        Some(3u8.into()),
        message_annotations.and_then(|annotations| annotations.get("test_number").cloned())
    );

    consumer.handle().close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_create_stream_not_existing_error() {
    let env = TestEnvironment::create().await;
    let producer = env.env.producer().build("stream_not_existing").await;

    match producer {
        Err(e) => assert_eq!(
            matches!(
                e,
                rabbitmq_stream_client::error::ProducerCreateError::StreamDoesNotExist { .. }
            ),
            true
        ),
        _ => panic!("Should be StreamNotFound error"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_after_close_error() {
    let env = TestEnvironment::create().await;
    let producer = env.env.producer().build(&env.stream).await.unwrap();
    producer.clone().close().await.unwrap();
    let closed = producer
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap_err();

    assert_eq!(
        matches!(
            closed,
            rabbitmq_stream_client::error::ProducerPublishError::Closed
        ),
        true
    );
}

pub fn routing_key_strategy_value_extractor(message: &Message) -> String {
    return "0".to_string();
}

fn hash_strategy_value_extractor(message: &Message) -> String {
    let s = String::from_utf8(Vec::from(message.data().unwrap())).expect("Found invalid UTF-8");
    return s;
}

#[tokio::test(flavor = "multi_thread")]
async fn key_super_steam_producer_test() {
    let env = TestEnvironment::create_super_stream().await;
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());
    let message_count = 100;

    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::RoutingKeyStrategy(
            RoutingKeyRoutingStrategy {
                routing_extractor: &routing_key_strategy_value_extractor,
            },
        ))
        .build(&env.super_stream)
        .await
        .unwrap();

    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder().body(format!("message{}", i)).build();
        super_stream_producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == message_count - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    notify_on_send.notified().await;
    _ = super_stream_producer.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_super_steam_producer_test() {
    let env = TestEnvironment::create_super_stream().await;
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());
    let message_count = 100;

    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .build(&env.super_stream)
        .await
        .unwrap();

    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder().body(format!("message{}", i)).build();
        super_stream_producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == message_count - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    notify_on_send.notified().await;
    _ = super_stream_producer.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_send_filtering_message() {
    let env = TestEnvironment::create().await;
    let producer = env
        .env
        .producer()
        .filter_value_extractor(|message| {
            let app_properties = message.application_properties();
            match app_properties {
                Some(properties) => {
                    let value = properties.get("region").and_then(|item| match item {
                        SimpleValue::String(s) => Some(s.clone()),
                        _ => None,
                    });
                    value.unwrap_or(String::from(""))
                }
                None => String::from(""),
            }
        })
        .build(&env.stream)
        .await
        .unwrap();
    producer.clone().close().await.unwrap();

    let message_builder = Message::builder();
    let mut application_properties = message_builder.application_properties();
    application_properties = application_properties.insert("region", "emea");

    let message = application_properties
        .message_builder()
        .body(b"message".to_vec())
        .build();

    let closed = producer.send_with_confirm(message).await.unwrap_err();

    assert_eq!(
        matches!(
            closed,
            rabbitmq_stream_client::error::ProducerPublishError::Closed
        ),
        true
    );
}
