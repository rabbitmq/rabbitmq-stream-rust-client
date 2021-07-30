use rabbitmq_stream_client::{Broker, Client};

#[tokio::test(flavor = "multi_thread")]
async fn client_connection_test() {
    let _client = Client::connect(Broker::default()).await.unwrap();
}
