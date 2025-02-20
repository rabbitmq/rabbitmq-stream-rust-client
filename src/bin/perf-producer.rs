#![allow(dead_code)]

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rabbitmq_stream_client::{
    types::{ByteCapacity, Message, OffsetSpecification},
    Environment,
};
use tokio::{sync::mpsc::UnboundedSender, time::sleep};
use tokio_stream::StreamExt;

static ONE_SECOND: Duration = Duration::from_secs(1);
static ONE_MINUTE: Duration = Duration::from_secs(60);

struct Metric {
    created_at: u128,
    received_at: SystemTime,
}

#[derive(Debug)]
struct Stats {
    average_latency: f32,
    messages_received: usize,
}

#[tokio::main]
async fn main() {
    let stream_name = "perf-stream";

    let environment = Environment::builder().build().await.unwrap();
    let _ = environment.delete_stream(stream_name).await;
    environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream_name)
        .await
        .unwrap();

    let environment = Arc::new(environment);

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let consumer_env = environment.clone();
    let consumer_handler = tokio::spawn(async move {
        start_consumer(consumer_env, stream_name, sender).await;
    });

    let produced_messages = AtomicU32::new(0);
    let producer_env = environment.clone();
    let producer_handler = tokio::spawn(async move {
        start_producer(producer_env, stream_name, &produced_messages).await;
    });

    let run_for = Duration::from_secs(5 * 60);

    tokio::spawn(async move {
        sleep(run_for).await;
        producer_handler.abort();
        sleep(Duration::from_secs(1)).await;
        consumer_handler.abort();
    });

    let minutes = run_for.as_secs() / 60;

    let mut now = Instant::now();
    // 5 minutes of metrics
    let mut metrics = Vec::with_capacity(50 * 60 * minutes as usize);
    while let Some(metric) = receiver.recv().await {
        if now.elapsed() > ONE_MINUTE {
            now = Instant::now();

            let last_metrics = metrics;
            metrics = Vec::with_capacity(50 * 60 * minutes as usize);
            tokio::spawn(async move {
                let stats = calculate_stats(last_metrics).await;
                println!("stats: {:?}", stats);
            });
        }
        metrics.push(metric);
    }

    let stats = calculate_stats(metrics).await;
    println!("stats: {:?}", stats);
}

async fn calculate_stats(metrics: Vec<Metric>) -> Stats {
    let mut total_latency = 0;
    let metric_count = metrics.len();
    for metric in metrics {
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_millis(metric.created_at as u64);
        let received_at = metric.received_at;
        let delta = received_at.duration_since(created_at).unwrap();
        total_latency += delta.as_millis();
    }

    Stats {
        average_latency: total_latency as f32 / metric_count as f32,
        messages_received: metric_count,
    }
}

async fn start_consumer(
    environment: Arc<Environment>,
    stream_name: &str,
    sender: UnboundedSender<Metric>,
) {
    let mut consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .build(stream_name)
        .await
        .unwrap();
    while let Some(Ok(delivery)) = consumer.next().await {
        let produced_at = delivery
            .message()
            .data()
            .map(|data| {
                u128::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
                ])
            })
            .unwrap();
        let metric = Metric {
            created_at: produced_at,
            received_at: SystemTime::now(),
        };
        sender.send(metric).unwrap();
    }
}

async fn start_producer(
    environment: Arc<Environment>,
    stream_name: &str,
    produced_messages: &AtomicU32,
) {
    let message_per_second = 50_usize;
    let producer = environment.producer().build(stream_name).await.unwrap();

    loop {
        let start = Instant::now();
        let messages = create_messages(message_per_second);
        let messages_sent = messages.len() as u32;
        for message in messages {
            producer.send(message, |_| async {}).await.unwrap();
        }
        produced_messages.fetch_add(messages_sent, Ordering::Relaxed);

        let elapsed = start.elapsed();

        if ONE_SECOND > elapsed {
            sleep(ONE_SECOND - elapsed).await;
        }
    }
}

fn create_messages(message_count_per_batch: usize) -> Vec<Message> {
    (0..message_count_per_batch)
        .map(|_| {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let since_the_epoch = since_the_epoch.as_millis();
            Message::builder()
                .body(since_the_epoch.to_be_bytes())
                .build()
        })
        .collect()
}
