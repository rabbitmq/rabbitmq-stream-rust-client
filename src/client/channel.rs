use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use rabbitmq_stream_protocol::{Request, Response};

use tokio::sync::Mutex;

use crate::error::ClientError;

pub fn channel<S, T>(sink: S, stream: T) -> (ChannelSender<S>, ChannelReceiver<T>)
where
    S: Sink<Request>,
    T: Stream<Item = Result<Response, ClientError>>,
{
    let closed = Arc::new(AtomicBool::new(false));
    (
        ChannelSender {
            inner: Arc::new(Mutex::new(sink)),
            closed: closed.clone(),
        },
        ChannelReceiver {
            inner: stream,
            closed,
        },
    )
}

#[derive(Clone)]
pub struct ChannelSender<T>
where
    T: Sink<Request>,
{
    inner: Arc<Mutex<T>>,
    closed: Arc<AtomicBool>,
}

impl<T> ChannelSender<T>
where
    T: Sink<Request>,
{
    pub fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

impl<T> ChannelReceiver<T>
where
    T: Stream<Item = Result<Response, ClientError>>,
{
    pub fn is_closed(&self) -> bool {
        self.closed.load(Relaxed)
    }
}

pub struct ChannelReceiver<T>
where
    T: Stream<Item = Result<Response, ClientError>>,
{
    inner: T,
    closed: Arc<AtomicBool>,
}

impl<T: Sink<Request> + Unpin> ChannelSender<T> {
    pub async fn send(&self, item: Request) -> Result<(), T::Error> {
        let mut channel = self.inner.lock().await;
        channel.send(item).await
    }

    pub async fn close(&self) -> Result<(), T::Error> {
        let mut channel = self.inner.lock().await;
        channel.close().await?;
        self.closed.store(true, Relaxed);

        Ok(())
    }
}

impl<T: Stream<Item = Result<Response, ClientError>> + Unpin> ChannelReceiver<T> {
    pub async fn next(&mut self) -> Option<Result<Response, ClientError>> {
        self.inner.next().await
    }
}
