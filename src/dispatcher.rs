use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};

use futures::Stream;
use rabbitmq_stream_protocol::Response;

use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};

use crate::{channel::ChannelReceiver, error::RabbitMqStreamError};

#[derive(Clone)]
pub struct Dispatcher {
    requests: Arc<Mutex<HashMap<u32, Sender<Response>>>>,
    correlation_id: Arc<AtomicU32>,
}

impl Dispatcher {
    pub async fn create<T>(receiver: ChannelReceiver<T>) -> Dispatcher
    where
        T: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin + Send,
        T: 'static,
    {
        let dispatcher = Dispatcher {
            requests: Arc::new(Mutex::new(HashMap::new())),
            correlation_id: Arc::new(AtomicU32::new(0)),
        };

        handle_response(dispatcher.clone(), receiver).await;
        dispatcher
    }

    pub async fn response_channel(&self) -> (u32, Receiver<Response>) {
        let (tx, rx) = channel(1);

        let correlation_id = self
            .correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut guard = self.requests.lock().await;

        guard.insert(correlation_id, tx);

        (correlation_id, rx)
    }

    pub async fn dispatch(&self, correlation_id: u32, response: Response) {
        let mut guard = self.requests.lock().await;

        let receiver = guard.remove(&correlation_id);

        drop(guard);

        if let Some(rcv) = receiver {
            let _ = rcv.send(response).await;
        }
    }
}

async fn handle_response<T>(dispatcher: Dispatcher, mut stream: ChannelReceiver<T>)
where
    T: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin + Send,
    T: 'static,
{
    tokio::spawn(async move {
        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => {
                    if let Some(correlation_id) = item.correlation_id() {
                        dispatcher.dispatch(correlation_id, item).await;
                    }
                }
                Err(_) => todo!(),
            }
        }
    });
}
