use futures::Stream;
use rabbitmq_stream_protocol::{Response, ResponseKind};
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tracing::{trace, warn};

use dashmap::DashMap;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    RwLock,
};

use crate::error::ClientError;

use super::{channel::ChannelReceiver, handler::MessageHandler};

#[derive(Clone)]
pub(crate) struct Dispatcher<T>(DispatcherState<T>);

struct RequestsMap {
    requests: DashMap<u32, Sender<Response>>,
    closed: AtomicBool,
}

impl RequestsMap {
    fn new() -> RequestsMap {
        RequestsMap {
            requests: DashMap::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn insert(&self, correlation_id: u32, sender: Sender<Response>) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        self.requests.insert(correlation_id, sender);
        true
    }

    fn remove(&self, correlation_id: u32) -> Option<Sender<Response>> {
        self.requests.remove(&correlation_id).map(|r| r.1)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.requests.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.requests.len()
    }
}

impl<T> Dispatcher<T>
where
    T: MessageHandler,
{
    pub fn new() -> Dispatcher<T> {
        Dispatcher(DispatcherState {
            requests: Arc::new(RequestsMap::new()),
            correlation_id: Arc::new(AtomicU32::new(0)),
            handler: Arc::new(RwLock::new(None)),
            handler_failed: Arc::new(AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    pub fn with_handler(handler: T) -> Dispatcher<T> {
        Dispatcher(DispatcherState {
            requests: Arc::new(RequestsMap::new()),
            correlation_id: Arc::new(AtomicU32::new(0)),
            handler: Arc::new(RwLock::new(Some(handler))),
            handler_failed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn response_channel(&self) -> Option<(u32, Receiver<Response>)> {
        let (tx, rx) = channel(1);

        let correlation_id = self
            .0
            .correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if self.0.requests.insert(correlation_id, tx) {
            Some((correlation_id, rx))
        } else {
            None
        }
    }

    #[cfg(test)]
    async fn requests_count(&self) -> usize {
        self.0.requests.len()
    }

    pub fn handler_failed_flag(&self) -> Arc<AtomicBool> {
        self.0.handler_failed.clone()
    }

    pub async fn set_handler(&self, handler: T) {
        let mut guard = self.0.handler.write().await;
        *guard = Some(handler);
    }

    pub async fn start<R>(&self, stream: ChannelReceiver<R>)
    where
        R: Stream<Item = Result<Response, ClientError>> + Unpin + Send,
        R: 'static,
    {
        handle_response(self.0.clone(), stream).await
    }
}

pub(crate) struct DispatcherState<T> {
    requests: Arc<RequestsMap>,
    correlation_id: Arc<AtomicU32>,
    handler: Arc<RwLock<Option<T>>>,
    handler_failed: Arc<AtomicBool>,
}

impl<T> Clone for DispatcherState<T> {
    fn clone(&self) -> Self {
        DispatcherState {
            requests: self.requests.clone(),
            correlation_id: self.correlation_id.clone(),
            handler: self.handler.clone(),
            handler_failed: self.handler_failed.clone(),
        }
    }
}

impl<T> DispatcherState<T>
where
    T: MessageHandler,
{
    pub async fn dispatch(&self, correlation_id: u32, response: Response) {
        let receiver = self.requests.remove(correlation_id);

        if let Some(rcv) = receiver {
            let _ = rcv.send(response).await;
        }
    }

    pub async fn notify(&self, response: Response) {
        if let Some(handler) = self.handler.read().await.as_ref() {
            if let Err(err) = handler.handle_message(Some(Ok(response))).await {
                warn!("Message handler returned error during notify: {}", err);
            }
        }
    }

    pub fn is_handler_failed(&self) -> bool {
        self.handler_failed.load(Ordering::Relaxed)
    }

    pub async fn close(self, error: Option<ClientError>) {
        self.requests.close();
        if let Some(handler) = self.handler.read().await.as_ref() {
            if let Some(err) = error {
                if let Err(e) = handler.handle_message(Some(Err(err))).await {
                    warn!("Message handler returned error during close: {}", e);
                }
            }
            if let Err(e) = handler.handle_message(None).await {
                warn!("Message handler returned error during close (None): {}", e);
            }
        }
    }
}

async fn handle_response<H, T>(state: DispatcherState<H>, mut stream: ChannelReceiver<T>)
where
    H: MessageHandler,
    T: Stream<Item = Result<Response, ClientError>> + Unpin + Send,
    T: 'static,
{
    tokio::spawn(async move {
        trace!("Dispatcher task: listening for messages");
        let mut stream_error = None;
        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => match item.correlation_id() {
                    Some(correlation_id) => match item.kind_ref() {
                        ResponseKind::ConsumerUpdate(_) => state.notify(item).await,
                        _ => state.dispatch(correlation_id, item).await,
                    },
                    None => state.notify(item).await,
                },
                Err(e) => {
                    warn!("Error from stream {:?}", e);
                    stream_error = Some(e);
                    break;
                }
            }
            if state.is_handler_failed() {
                warn!("Handler failed, stopping dispatcher");
                break;
            }
        }
        if !stream.is_closed() {
            // TODO handle reconnection if the connection is broken
            warn!("Stream closed");
        }
        state.close(stream_error).await;
    });
}

#[cfg(test)]
mod tests {

    use std::{
        collections::HashMap,
        sync::{atomic::AtomicBool, Arc, Mutex},
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

    use crate::client::MessageResult;

    use super::super::channel::channel;

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
        type Error = crate::error::ClientError;

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

        let (correlation_id, mut rx) = dispatcher.response_channel().unwrap();

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

        let handler = move |response: MessageResult| async move {
            push_tx.send(response).await.unwrap();
            Ok(())
        };
        let (_tx, rx) = channel(mock_source.clone(), mock_source.clone());
        let dispatcher = Dispatcher::with_handler(handler);

        dispatcher.start(rx).await;

        let response = push_rx.recv().await;

        assert!(matches!(response, Some(..)));
    }

    #[tokio::test]
    async fn should_reject_requests_after_closing() {
        let _mock_source = MockIO::push();

        let dispatcher = Dispatcher::with_handler(|_| async { Ok(()) });

        let maybe_channel = dispatcher.response_channel();
        assert!(maybe_channel.is_some());

        dispatcher.0.requests.close();

        let maybe_channel = dispatcher.response_channel();
        assert!(maybe_channel.is_none());
    }

    struct FailingHandler {
        handler_failed: Arc<AtomicBool>,
        call_count: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl crate::client::MessageHandler for FailingHandler {
        async fn handle_message(&self, _item: MessageResult) -> crate::RabbitMQStreamResult<()> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.handler_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Err(crate::error::ClientError::ConnectionClosed)
        }
    }

    #[tokio::test]
    async fn should_stop_dispatch_when_handler_fails() {
        let mock_source = MockIO::push();

        let dispatcher = Dispatcher::<FailingHandler>::new();
        let handler_failed = dispatcher.handler_failed_flag();
        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let handler = FailingHandler {
            handler_failed,
            call_count: call_count.clone(),
        };

        dispatcher.set_handler(handler).await;

        let (_tx, rx) = channel(mock_source.clone(), mock_source.clone());
        dispatcher.start(rx).await;

        // Wait for the dispatcher task to process and stop
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let count = call_count.load(std::sync::atomic::Ordering::SeqCst);
        // Handler called once from notify (which set handler_failed),
        // then once from close(None)
        assert_eq!(
            count, 2,
            "Handler should be called exactly twice: once from notify, once from close"
        );
    }

    struct CountingHandler {
        call_count: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl crate::client::MessageHandler for CountingHandler {
        async fn handle_message(&self, _item: MessageResult) -> crate::RabbitMQStreamResult<()> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn should_keep_dispatching_when_handler_succeeds() {
        let mock_source = MockIO::push();

        let call_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let handler = CountingHandler {
            call_count: call_count.clone(),
        };

        let (_tx, rx) = channel(mock_source.clone(), mock_source.clone());
        let dispatcher = Dispatcher::with_handler(handler);

        dispatcher.start(rx).await;

        // Let the dispatcher run for a bit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let count = call_count.load(std::sync::atomic::Ordering::SeqCst);
        // With a successful handler and infinite Push stream,
        // the dispatcher should have processed many messages
        assert!(
            count > 2,
            "Handler should be called many times when succeeding, got {}",
            count
        );
    }
}
