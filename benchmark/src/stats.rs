use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rabbitmq_stream_client::MetricsCollector;
use tracing::info;

#[derive(Clone)]
pub struct Stats {
    start_time: Arc<AtomicU64>,
    published_count: Arc<AtomicU64>,
    confirmed_message_count: Arc<AtomicU64>,
    consumer_message_count: Arc<AtomicU64>,
}

impl Default for Stats {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self {
            start_time: Arc::new(AtomicU64::new(now as u64)),
            published_count: Default::default(),
            confirmed_message_count: Default::default(),
            consumer_message_count: Default::default(),
        }
    }
}
impl Stats {
    pub fn dump(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let start = Duration::from_nanos(self.start_time.load(Ordering::Relaxed));
        let diff = now - start;
        let millis = diff.as_millis() as u64;

        info!(
            "Published {} msg/s, Confirmed {} msg/s, Consumed {} msg/s, elapsed {}",
            self.published_count.load(Ordering::Relaxed) * 1000 / millis,
            self.confirmed_message_count.load(Ordering::Relaxed) * 1000 / millis,
            self.consumer_message_count.load(Ordering::Relaxed) * 1000 / millis,
            diff.as_secs()
        );
    }
}

#[async_trait::async_trait]
impl MetricsCollector for Stats {
    async fn publish(&self, count: u64) {
        self.published_count.fetch_add(count, Ordering::Relaxed);
    }

    async fn consume(&self, count: u64) {
        self.consumer_message_count
            .fetch_add(count, Ordering::Relaxed);
    }

    async fn publish_confirm(&self, count: u64) {
        self.confirmed_message_count
            .fetch_add(count, Ordering::Relaxed);
    }
    async fn publish_error(&self, _count: u64) {}
}
