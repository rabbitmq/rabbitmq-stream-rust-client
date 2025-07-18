use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use fake::{Fake, Faker};
use futures::{lock::Mutex, StreamExt};
use tokio::{sync::mpsc::channel, time::sleep};

use rabbitmq_stream_client::{
    error::ClientError,
    types::{Message, OffsetSpecification, SimpleValue},
    Environment, OnClosed,
};

#[path = "./common.rs"]
mod common;

use common::*;

use rabbitmq_stream_client::types::{
    HashRoutingMurmurStrategy, RoutingKeyRoutingStrategy, RoutingStrategy,
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
            .client_provided_name("my producer")
            .build(&stream)
            .await
            .unwrap();

        for _ in 0..times {
            let cloned_ids = ids.clone();
            let countdown = countdown.clone();
            producer
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
                .publishing_id(0)
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
                .publishing_id(0)
                .build(),
            // this won't be confirmed
            // since it will skipped by deduplication
            Message::builder()
                .body(b"message".to_vec())
                .publishing_id(0)
                .build(),
            // confirmed since the publishing id is different
            Message::builder()
                .body(b"message".to_vec())
                .publishing_id(1)
                .build(),
            // not confirmed since the publishing id is the same
            // message will be skipped by deduplication
            Message::builder()
                .body(b"message".to_vec())
                .publishing_id(1)
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
async fn producer_send_with_callback_can_drop() {
    let env = TestEnvironment::create().await;
    let reference: String = Faker.fake();

    let mut producer = env
        .env
        .producer()
        .name(&reference)
        .build(&env.stream)
        .await
        .unwrap();

    // A non copy structure
    struct Foo;

    let f = Foo;
    producer
        .send(
            Message::builder().body(b"message".to_vec()).build(),
            move |_| {
                // this callback is an FnOnce, so we can drop a value here
                drop(f);
                async {}
            },
        )
        .await
        .unwrap();

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

    let confirmation = result.first().unwrap();
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
        Some(1u32),
        properties.and_then(|properties| properties.group_sequence)
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
        Err(e) => assert!(matches!(
            e,
            rabbitmq_stream_client::error::ProducerCreateError::StreamDoesNotExist { .. }
        )),
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

    assert!(matches!(
        closed,
        rabbitmq_stream_client::error::ProducerPublishError::Closed
    ));
}

pub fn routing_key_strategy_value_extractor(_: &Message) -> String {
    "0".to_string()
}

fn hash_strategy_value_extractor(message: &Message) -> String {
    let s = String::from_utf8(Vec::from(message.data().unwrap())).expect("Found invalid UTF-8");
    s
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
async fn key_super_steam_non_existing_producer_test() {
    let env = TestEnvironment::create_super_stream().await;

    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::RoutingKeyStrategy(
            RoutingKeyRoutingStrategy {
                routing_extractor: &routing_key_strategy_value_extractor,
            },
        ))
        .build("non-existing-stream")
        .await
        .unwrap();

    let msg = Message::builder().body(format!("message{}", 0)).build();
    let result = super_stream_producer
        .send(msg, |_| async move {})
        .await
        .unwrap_err();

    assert!(matches!(
        result,
        rabbitmq_stream_client::error::SuperStreamProducerPublishError::ProducerCreateError()
    ));

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
        .client_provided_name("test super stream producer ")
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

    assert!(matches!(
        closed,
        rabbitmq_stream_client::error::ProducerPublishError::Closed
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn super_stream_producer_send_filtering_message() {
    let env = TestEnvironment::create_super_stream().await;
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
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
        .build(&env.super_stream)
        .await
        .unwrap();

    let message_builder = Message::builder();
    let mut application_properties = message_builder.application_properties();
    application_properties = application_properties.insert("region", "emea");

    let message = application_properties
        .message_builder()
        .body(b"message".to_vec())
        .build();

    let closed = super_stream_producer.send(message, |_| async move {}).await;

    match closed {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_drop_connection() {
    let _ = tracing_subscriber::fmt::try_init();
    let client_provided_name: String = Faker.fake();
    let env = TestEnvironment::create().await;
    let producer = env
        .env
        .producer()
        .client_provided_name(&client_provided_name)
        .build(&env.stream)
        .await
        .unwrap();

    producer
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    let connection = wait_for_named_connection(client_provided_name.clone()).await;
    drop_connection(connection).await;

    let closed = producer
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await;

    assert!(matches!(
        closed,
        Err(rabbitmq_stream_client::error::ProducerPublishError::Timeout)
    ));

    let err = producer.close().await.unwrap_err();
    assert!(matches!(
        err,
        rabbitmq_stream_client::error::ProducerCloseError::Client(ClientError::ConnectionClosed)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_close() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();
    let producer2 = producer.clone();

    let metrics = tokio::runtime::Handle::current().metrics();
    assert_eq!(metrics.num_alive_tasks(), 3);

    producer.close().await.unwrap();

    let status = producer2
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await;
    let err = status.unwrap_err();
    assert!(matches!(
        err,
        rabbitmq_stream_client::error::ProducerPublishError::Closed
    ));
    drop(producer2);

    // Ensure that the producer is closed and no tasks are alive
    sleep(Duration::from_millis(500)).await;

    let metrics = tokio::runtime::Handle::current().metrics();
    assert_eq!(metrics.num_alive_tasks(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_drop() {
    let env = TestEnvironment::create().await;

    let producer = env.env.producer().build(&env.stream).await.unwrap();
    let producer2 = producer.clone();

    let metrics = tokio::runtime::Handle::current().metrics();
    assert_eq!(metrics.num_alive_tasks(), 3);

    // This should not close everything: another producer is still alive
    drop(producer);

    // Ensure that if something should drop some tasks, it has time to do so
    sleep(Duration::from_millis(500)).await;

    producer2
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    let metrics = tokio::runtime::Handle::current().metrics();
    assert_eq!(metrics.num_alive_tasks(), 3);

    drop(producer2);

    // If we drop the last reference to internal producer,
    // all tasks should be closed
    // Keep time for tasks to close
    sleep(Duration::from_millis(500)).await;

    let metrics = tokio::runtime::Handle::current().metrics();
    assert_eq!(metrics.num_alive_tasks(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_drop_connection_on_close() {
    struct Foo {
        notifier: Arc<Notify>,
    }
    #[async_trait::async_trait]
    impl OnClosed for Foo {
        async fn on_closed(&self, _: Vec<Message>) {
            self.notifier.notify_one();
        }
    }

    let notifier = Arc::new(Notify::new());
    let _ = tracing_subscriber::fmt::try_init();
    let client_provided_name: String = Faker.fake();
    let env = TestEnvironment::create().await;
    let producer = env
        .env
        .producer()
        .client_provided_name(&client_provided_name)
        .on_closed(Box::new(Foo {
            notifier: notifier.clone(),
        }))
        .build(&env.stream)
        .await
        .unwrap();

    producer
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    let connection = wait_for_named_connection(client_provided_name.clone()).await;
    drop_connection(connection).await;

    notifier.notified().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_timeout() {
    struct Foo {
        notifier: Arc<Notify>,
    }
    #[async_trait::async_trait]
    impl OnClosed for Foo {
        async fn on_closed(&self, _: Vec<Message>) {
            self.notifier.notify_one();
        }
    }

    let notifier = Arc::new(Notify::new());
    let _ = tracing_subscriber::fmt::try_init();
    let client_provided_name: String = Faker.fake();
    let env = TestEnvironment::create().await;
    let producer = env
        .env
        .producer()
        .client_provided_name(&client_provided_name)
        .overwrite_heartbeat(1)
        .on_closed(Box::new(Foo {
            notifier: notifier.clone(),
        }))
        .build(&env.stream)
        .await
        .unwrap();

    producer
        .send_with_confirm(Message::builder().body(b"message".to_vec()).build())
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    let is_stopped = tokio::select! {
        _ = notifier.notified() => true,
        _ = sleep(Duration::from_secs(5)) => false,
    };

    assert!(is_stopped, "Producer did not stop after timeout");
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_got_back_unconfirmed_messages_on_close() {
    struct Foo {
        on_closed_sender: tokio::sync::mpsc::Sender<Vec<Message>>,
    }
    #[async_trait::async_trait]
    impl OnClosed for Foo {
        async fn on_closed(&self, unconfirmed: Vec<Message>) {
            self.on_closed_sender.send(unconfirmed).await.unwrap();
        }
    }

    let (on_closed_sender, mut on_closed_receiver) = tokio::sync::mpsc::channel(1);
    let _ = tracing_subscriber::fmt::try_init();
    let client_provided_name: String = Faker.fake();
    let env = TestEnvironment::create().await;
    let producer = env
        .env
        .producer()
        .client_provided_name(&client_provided_name)
        .on_closed(Box::new(Foo { on_closed_sender }))
        .build(&env.stream)
        .await
        .unwrap();

    let connection = wait_for_named_connection(client_provided_name).await;

    let (sender, receiver) = tokio::sync::oneshot::channel();

    let join_handler = tokio::spawn(async move {
        let mut sender = Some(sender);
        for i in 0..100 {
            if i == 2 {
                if let Some(sender) = sender.take() {
                    // Simulate a delay to ensure the producer is closed before sending more messages
                    sender.send(()).unwrap();
                }
            }

            let message = Message::builder().body(format!("{}", i)).build();
            if producer.send(message, |_| async {}).await.is_err() {
                break;
            }
        }
    });

    receiver.await.unwrap();
    drop_connection(connection).await;

    // Wait for the above task ends
    join_handler.await.unwrap();

    let unconfirmed = on_closed_receiver.recv().await.unwrap();

    // Some messages shouldn't be confirmed
    assert!(!unconfirmed.is_empty());

    // Check that the unconfirmed messages are in order
    for couple in unconfirmed.windows(2) {
        let first = couple[0].data().expect("First message should have data");
        let second = couple[1].data().expect("Second message should have data");
        let first_value =
            String::from_utf8(first.to_vec()).expect("First message should be valid UTF-8");
        let second_value =
            String::from_utf8(second.to_vec()).expect("Second message should be valid UTF-8");
        let first_number: u32 = first_value
            .parse()
            .expect("First message should be a number");
        let second_number: u32 = second_value
            .parse()
            .expect("Second message should be a number");

        assert!(first_number < second_number, "Messages should be in order");
    }
}
