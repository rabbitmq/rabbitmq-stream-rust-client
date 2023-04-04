
use rabbitmq_stream_client::{types::Message, Environment, NoDedup, Producer, TlsConfiguration};
use tracing::info;
use tracing_subscriber::FmtSubscriber;
use tokio_native_tls::native_tls::Certificate;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::{PKey, Private};
use openssl::x509::X509;

const BATCH_SIZE: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let stream_name = String::from("mixing");
    let subscriber = FmtSubscriber::builder().finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let cert = include_bytes!("/Users/dpalaia/projects/rabbitmq-stream-go-client/compose/tls/tls-gen/basic/result/ca_certificate.pem");

    let tlsConfiguration = TlsConfiguration::builder()
        .trust_hostname(true)
        //.trust_certificate(false)
        .add_root_certificate(Certificate::from_pem(cert).unwrap())
        .build();


    let environment = Environment::builder()
        .host("localhost")
        .port(5551)
        .tls(tlsConfiguration)
        .build()
        .await?;

    start_publisher(
        environment.clone(),
        &stream_name,
    ).await;

    Ok(())

}

async fn start_publisher(
    env: Environment,
  //  opts: &Opts,
    stream: &String,
) -> Result<(), Box<dyn std::error::Error>> {

    info!("im inside start_publisher");
    let _ = env.stream_creator().create(&stream).await;

    let producer = env
        .producer()
        .batch_size(BATCH_SIZE)
        .build(&stream)
        .await?;

    let is_batch_send = true;
    tokio::task::spawn(async move {
        info!(
            "Starting producer with batch size {} and batch send {}",
            BATCH_SIZE, is_batch_send
        );
        info!("Sending {} simple messages", BATCH_SIZE);
        batch_send_simple(&producer).await;

        
    }).await?;
    info!("end im inside start_publisher");
    Ok(())
}

async fn batch_send_simple(producer: &Producer<NoDedup>) {
    let mut msg = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        msg.push(Message::builder().body(format!("rust message{}", i)).build());
    }

    producer
        .batch_send(msg, move |_| async move {})
        .await
        .unwrap();

}


#[derive(Debug)]
enum CertLoadError {
    TlsError(tokio_native_tls::native_tls::Error),
    Io(String, std::io::Error),
}