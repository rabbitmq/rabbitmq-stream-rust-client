use rabbitmq_stream_client::{Broker, Client};

#[tokio::test(flavor = "multi_thread")]
async fn client_connection_test() {
    let client = Client::connect(Broker::default()).await.unwrap();

    assert_ne!(client.server_properties().len(), 0);
    assert_ne!(client.connection_properties().len(), 0);
}
