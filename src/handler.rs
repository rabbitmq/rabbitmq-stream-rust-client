use crate::RabbitMQStreamResult;
use rabbitmq_stream_protocol::Response;
use std::future::Future;

#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    async fn handle_message(&self, item: Response) -> RabbitMQStreamResult<()>;
}

#[async_trait::async_trait]
impl<T, F> MessageHandler for T
where
    F: Future<Output = RabbitMQStreamResult<()>> + Send,
    T: FnOnce(Response) -> F + Send + Sync,
    T: 'static,
    T: Clone,
{
    async fn handle_message(&self, item: Response) -> RabbitMQStreamResult<()> {
        let fun = self.clone();
        fun(item).await
    }
}
