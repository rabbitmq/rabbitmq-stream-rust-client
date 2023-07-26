use tokio_native_tls::native_tls::Certificate;
use tracing::info;
use tracing_subscriber::fmt::time;
use tracing_subscriber::FmtSubscriber;

use rabbitmq_stream_client::{types::Message, Environment, NoDedup, Producer, TlsConfiguration};

const BATCH_SIZE: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream_name = String::from("mixing");
    let subscriber = FmtSubscriber::builder().finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let cert =
        include_bytes!("/Users/gas/sw/rabbitmq_server-3.11.11/sbin/certs/ca_certificate.pem");

    let tls_configuration = TlsConfiguration::builder()
        .trust_hostname(false)
        //.trust_certificate(false)
        .add_root_certificate(Certificate::from_pem(cert).unwrap())
        .build();

    let environment = Environment::builder()
        .host("localhost")
        .username("guest")
        .password("guest")
        .port(5551)
        .tls(tls_configuration)
        .build()
        .await?;
    println!("environment = {:?}", 1);
    environment.stream_creator().create(&stream_name).await?;
    println!("environment = {:?}", 2);
    let producer = environment
        .producer()
        .batch_size(BATCH_SIZE)
        .build(&stream_name)
        .await?;
    println!("environment = {:?}", 3);
    // println!("producer = {:?}", producer);

    batch_send_simple(&producer).await;

    println!("environment = {:?}", 4);

    // start_publisher(environment.clone(), &stream_name).await.expect("TODO: panic message");

    Ok(())
}

async fn start_publisher(
    env: Environment,
    //  opts: &Opts,
    stream: &String,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("im inside start_publisher");
    let r = env.stream_creator().create(&stream).await;

    println!("stream_creator = {:?}", r);

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
    info!("end im inside start_publisher");
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

    println!("batch_send_simple = {:?}", 1);
}

#[derive(Debug)]
enum CertLoadError {
    TlsError(tokio_native_tls::native_tls::Error),
    Io(String, std::io::Error),
}
