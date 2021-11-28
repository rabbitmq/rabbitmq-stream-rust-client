#[async_trait::async_trait]
pub trait MetricsCollector: Send + Sync {
    async fn publish(&self, count: u64);
}

pub struct NopMetricsCollector {}

#[async_trait::async_trait]
impl MetricsCollector for NopMetricsCollector {
    async fn publish(&self, _count: u64) {}
}
