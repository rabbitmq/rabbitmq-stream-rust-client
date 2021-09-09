use futures::Stream;
use rabbitmq_stream_protocol::Response;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};
use tracing::error;

use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, RwLock,
};

use crate::error::RabbitMqStreamError;

use super::{channel::ChannelReceiver, handler::MessageHandler};

#[derive(Clone)]
pub(crate) struct Dispatcher<T>(DispatcherState<T>);

pub(crate) struct DispatcherState<T> {
    requests: Arc<Mutex<HashMap<u32, Sender<Response>>>>,
    correlation_id: Arc<AtomicU32>,
    handler: Arc<RwLock<Option<T>>>,
}

impl<T> Clone for DispatcherState<T> {
    fn clone(&self) -> Self {
        DispatcherState {
            requests: self.requests.clone(),
            correlation_id: self.correlation_id.clone(),
            handler: self.handler.clone(),
        }
    }
}

impl<T: MessageHandler> Dispatcher<T> {
    pub fn new() -> Dispatcher<T> {
        Dispatcher(DispatcherState {
            requests: Arc::new(Mutex::new(HashMap::new())),
            correlation_id: Arc::new(AtomicU32::new(0)),
            handler: Arc::new(RwLock::new(None)),
        })
    }

    #[cfg(test)]
    pub fn with_handler(handler: T) -> Dispatcher<T> {
        Dispatcher(DispatcherState {
            requests: Arc::new(Mutex::new(HashMap::new())),
            correlation_id: Arc::new(AtomicU32::new(0)),
            handler: Arc::new(RwLock::new(Some(handler))),
        })
    }

    pub async fn response_channel(&self) -> (u32, Receiver<Response>) {
        let (tx, rx) = channel(1);

        let correlation_id = self
            .0
            .correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut guard = self.0.requests.lock().await;

        guard.insert(correlation_id, tx);

        (correlation_id, rx)
    }

    #[cfg(test)]
    async fn requests_count(&self) -> usize {
        self.0.requests.lock().await.len()
    }

    pub async fn set_handler(&self, handler: T) {
        let mut guard = self.0.handler.write().await;
        *guard = Some(handler);
    }
    pub async fn start<R>(&self, stream: ChannelReceiver<R>)
    where
        R: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin + Send,
        R: 'static,
    {
        handle_response(self.0.clone(), stream).await
    }
}

impl<T: MessageHandler> DispatcherState<T> {
    pub async fn dispatch(&self, correlation_id: u32, response: Response) {
        let mut guard = self.requests.lock().await;

        let receiver = guard.remove(&correlation_id);

        drop(guard);

        if let Some(rcv) = receiver {
            let _ = rcv.send(response).await;
        }
    }

    pub async fn notify(&self, response: Response) {
        if let Some(handler) = self.handler.read().await.as_ref() {
            let _ = handler.handle_message(response).await;
        }
    }
}

async fn handle_response<H, T>(state: DispatcherState<H>, mut stream: ChannelReceiver<T>)
where
    H: MessageHandler,
    T: Stream<Item = Result<Response, RabbitMqStreamError>> + Unpin + Send,
    T: 'static,
{
    tokio::spawn(async move {
        // TODO implements Error handling and close of dispatcher
        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => match item.correlation_id() {
                    Some(correlation_id) => state.dispatch(correlation_id, item).await,
                    None => state.notify(item).await,
                },
                Err(e) => {
                    error!("Error from stream {:?}", e);
                    panic!("Error {}", e);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {

    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use rabbitmq_stream_protocol::{
        commands::{
            heart_beat::HeartbeatResponse,
            peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
            Command,
        },
        types::Header,
        Request, RequestKind, Response, ResponseCode, ResponseKind,
    };
    use tokio::sync::mpsc::channel as tokio_channel;

    use crate::channel::channel;

    use super::Dispatcher;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Clone)]
    enum MockIO {
        Push,
        Request(Arc<Mutex<Option<Request>>>),
    }

    impl MockIO {
        fn push() -> MockIO {
            MockIO::Push
        }

        fn request() -> MockIO {
            MockIO::Request(Arc::new(Mutex::new(None)))
        }
    }

    impl futures::Stream for MockIO {
        type Item = crate::RabbitMQStreamResult<Response>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = unsafe { Pin::get_unchecked_mut(self) };

            match this {
                MockIO::Push => Poll::Ready(Some(Ok(Response::new(
                    Header::new(23, 1),
                    ResponseKind::Heartbeat(HeartbeatResponse {}),
                )))),
                MockIO::Request(request) => match request.lock().unwrap().take() {
                    Some(request) => match request.kind() {
                        RequestKind::PeerProperties(peer) => {
                            let peer_response = PeerPropertiesResponse::new(
                                peer.correlation_id(),
                                ResponseCode::Ok,
                                HashMap::new(),
                            );
                            let response = Response::new(
                                Header::new(peer.key(), 1),
                                ResponseKind::PeerProperties(peer_response),
                            );
                            Poll::Ready(Some(Ok(response)))
                        }
                        _ => panic!("Only Peer properties supported"),
                    },
                    None => Poll::Ready(None),
                },
            }
        }
    }

    impl futures::Sink<Request> for MockIO {
        type Error = crate::error::RabbitMqStreamError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Request) -> Result<(), Self::Error> {
            let this = unsafe { Pin::get_unchecked_mut(self) };

            match this {
                MockIO::Push => todo!(),
                MockIO::Request(request) => {
                    let mut guard = request.lock().unwrap();
                    *guard = Some(item);
                }
            }
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            todo!()
        }
    }

    #[tokio::test]
    async fn should_dispatch_a_response() {
        let mock_source = MockIO::request();
        let (tx, rx) = channel(mock_source.clone(), mock_source.clone());

        let handler = |_| async { Ok(()) };
        let dispatcher = Dispatcher::with_handler(handler);

        dispatcher.start(rx).await;

        let (correlation_id, mut rx) = dispatcher.response_channel().await;

        let req: Request = PeerPropertiesCommand::new(correlation_id, HashMap::new()).into();

        assert_eq!(1, dispatcher.requests_count().await);
        tx.send(req).await.unwrap();

        let response = rx.recv().await.unwrap();

        assert_eq!(0, dispatcher.requests_count().await);

        assert_eq!(Some(correlation_id), response.correlation_id());
    }

    #[tokio::test]
    async fn should_dispatch_a_push_message() {
        let mock_source = MockIO::push();

        let (push_tx, mut push_rx) = tokio_channel(1);

        let handler = move |response: Response| async move {
            push_tx.send(response).await.unwrap();
            Ok(())
        };
        let (_tx, rx) = channel(mock_source.clone(), mock_source.clone());
        let dispatcher = Dispatcher::with_handler(handler);

        dispatcher.start(rx).await;

        let response = push_rx.recv().await;

        assert!(matches!(response, Some(..)));
    }
}
