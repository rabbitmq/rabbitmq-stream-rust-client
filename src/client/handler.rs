use crate::{error::ClientError, RabbitMQStreamResult};
use rabbitmq_stream_protocol::Response;
use std::future::Future;

pub type MessageResult = Option<Result<Response, ClientError>>;

#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    async fn handle_message(&self, item: MessageResult) -> RabbitMQStreamResult<()>;
}

#[async_trait::async_trait]
impl<T, F> MessageHandler for T
where
    F: Future<Output = RabbitMQStreamResult<()>> + Send,
    T: FnOnce(MessageResult) -> F + Send + Sync,
    T: 'static,
    T: Clone,
{
    async fn handle_message(&self, item: MessageResult) -> RabbitMQStreamResult<()> {
        let fun = self.clone();
        fun(item).await
    }
}
