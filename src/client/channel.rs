use std::sync::Arc;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use rabbitmq_stream_protocol::{Request, Response};

use tokio::sync::Mutex;

use crate::error::RabbitMqStreamError;

pub fn channel<S, T>(sink: S, stream: T) -> (ChannelSender<S>, ChannelReceiver<T>)
where
    S: Sink<Request>,
    T: Stream<Item = Result<Response, RabbitMqStreamError>>,
{
    (
        ChannelSender {
            inner: Arc::new(Mutex::new(sink)),
        },
        ChannelReceiver { inner: stream },
    )
}

#[derive(Clone)]
pub struct ChannelSender<T>
where
    T: Sink<Request>,
{
    inner: Arc<Mutex<T>>,
}

pub struct ChannelReceiver<T>
where
    T: Stream<Item = Result<Response, RabbitMqStreamError>>,
{
    inner: T,
}

impl<T: Sink<Request> + Unpin> ChannelSender<T> {
    pub async fn send(&self, item: Request) -> Result<(), T::Error> {
        let mut channel = self.inner.lock().await;
        channel.send(item).await
    }
}

impl<T: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin> ChannelReceiver<T> {
    pub async fn next(&mut self) -> Option<Result<Response, RabbitMqStreamError>> {
        self.inner.next().await
    }
}
