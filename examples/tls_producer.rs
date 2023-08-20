use tracing::info;
use tracing_subscriber::FmtSubscriber;

use rabbitmq_stream_client::{types::Message, Environment, NoDedup, Producer, TlsConfiguration};

const BATCH_SIZE: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream_name = String::from("tls_test_stream");
    let subscriber = FmtSubscriber::builder().finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let tls_configuration: TlsConfiguration = TlsConfiguration::builder()
        .add_root_certificates(String::from("/path/to/your/certificate-ca.pem"))
        .build();

    let environment = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await?;

    start_publisher(environment.clone(), &stream_name)
        .await
        .expect("error in publisher");

    Ok(())
}

async fn start_publisher(
    env: Environment,
    stream: &String,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = env.stream_creator().create(&stream).await;

    let producer = env.producer().batch_size(BATCH_SIZE).build(&stream).await?;

    let is_batch_send = true;
    tokio::task::spawn(async move {
        info!(
            "Starting producer with batch size {} and batch send {}",
            BATCH_SIZE, is_batch_send
        );
        info!("Sending {} simple messages", BATCH_SIZE);
        batch_send_simple(&producer).await;
    })
    .await?;
    Ok(())
}

async fn batch_send_simple(producer: &Producer<NoDedup>) {
    let mut msg = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        msg.push(
            Message::builder()
                .body(format!("rust message{}", i))
                .build(),
        );
    }

    producer
        .batch_send(msg, move |_| async move {})
        .await
        .unwrap();
}
