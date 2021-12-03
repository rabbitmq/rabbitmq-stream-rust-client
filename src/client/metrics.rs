#[async_trait::async_trait]
pub trait MetricsCollector: Send + Sync {
    async fn publish(&self, count: u64);
    async fn consume(&self, count: u64);
    async fn publish_confirm(&self, count: u64);
    async fn publish_error(&self, count: u64);
}
pub struct NopMetricsCollector {}

#[async_trait::async_trait]
impl MetricsCollector for NopMetricsCollector {
    async fn publish(&self, _count: u64) {}
    async fn publish_confirm(&self, _count: u64) {}
    async fn publish_error(&self, _count: u64) {}

    async fn consume(&self, _count: u64) {}
}
