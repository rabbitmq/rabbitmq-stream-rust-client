use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use futures::StreamExt;
use rabbitmq_stream_client::{types::Message, Environment, MetricsCollector, NoDedup, Producer};
use tracing::info;
use tracing_subscriber::FmtSubscriber;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();
    let subscriber = FmtSubscriber::builder().finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let stats = Stats::default();
    let environment = Environment::builder()
        .host("localhost")
        .port(5552)
        .metrics_collector(stats.clone())
        .build()
        .await?;

    start_reporting_task(stats.clone()).await;

    start_publisher(
        environment.clone(),
        &opts,
        opts.streams.get(0).unwrap().clone(),
        stats.clone(),
    )
    .await?;

    start_consumer(
        environment,
        &opts,
        opts.streams.get(0).unwrap().clone(),
        stats,
    )
    .await?;
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
#[derive(Parser, Debug)]
#[clap(version = "1.0", author = "Enrico R.  <enrico.risa@gmail.com>")]
struct Opts {
    #[clap(short, long, default_value = "default.conf")]
    urls: Vec<String>,
    #[clap(short, long, default_value = "benchmark_stream")]
    streams: Vec<String>,

    #[clap(short, long, default_value = "100")]
    batch_size: usize,

    #[clap(short, long)]
    batch_send: bool,
}

#[derive(Clone, Default)]
pub struct Stats {
    published_count: Arc<AtomicU64>,
    confirmed_message_count: Arc<AtomicU64>,
    consumer_message_count: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl MetricsCollector for Stats {
    async fn publish(&self, count: u64) {
        self.published_count.fetch_add(count, Ordering::Relaxed);
    }
}

async fn start_publisher(
    env: Environment,
    opts: &Opts,
    stream: String,
    stats: Stats,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = env.stream_creator().create(&stream).await;

    let batch_size = opts.batch_size;
    let producer = env
        .producer()
        .batch_size(opts.batch_size)
        .build(&stream)
        .await?;

    let is_batch_send = opts.batch_send;
    tokio::task::spawn(async move {
        info!(
            "Starting producer with batch size {} and batch send {}",
            batch_size, is_batch_send
        );
        loop {
            if is_batch_send {
                batch_send(&producer, batch_size, &stats).await
            } else {
                single_send(&producer, batch_size, &stats).await
            }
        }
    });
    Ok(())
}

async fn single_send(producer: &Producer<NoDedup>, batch_size: usize, stats: &Stats) {
    for _ in 0..batch_size {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let inner_stats = stats.clone();
        producer
            .send_with_callback(
                Message::builder().body(time.to_be_bytes()).build(),
                move |confirmation_status| {
                    let stats = inner_stats.clone();
                    async move {
                        if confirmation_status.unwrap().confirmed() {
                            stats
                                .confirmed_message_count
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
            )
            .await
            .unwrap();
    }
}
async fn batch_send(producer: &Producer<NoDedup>, batch_size: usize, stats: &Stats) {
    let mut msg = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        msg.push(Message::builder().body(time.to_be_bytes()).build());
    }

    let inner_stats = stats.clone();
    producer
        .batch_send_with_callback(msg, move |confirmation_status| {
            let stats = inner_stats.clone();
            async move {
                if confirmation_status.unwrap().confirmed() {
                    stats
                        .confirmed_message_count
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        })
        .await
        .unwrap();
}
async fn start_consumer(
    env: Environment,
    _opts: &Opts,
    stream: String,
    stats: Stats,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = env.consumer().build(&stream).await?;

    tokio::task::spawn(async move {
        loop {
            while consumer.next().await.is_some() {
                stats.consumer_message_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    Ok(())
}

async fn start_reporting_task(stats: Stats) {
    tokio::task::spawn(async move {
        let start = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            let diff = Instant::now() - start;
            let sec = diff.as_secs();
            if sec > 0 {
                info!(
                    "Published {} msg/s, Confirmed {} msg/s, Consumed {} msg/s, elapsed {}",
                    stats.published_count.load(Ordering::Relaxed) / sec,
                    stats.confirmed_message_count.load(Ordering::Relaxed) / sec,
                    stats.consumer_message_count.load(Ordering::Relaxed) / sec,
                    sec
                );
            }
        }
    });
}
