mod send_super_stream;
mod single_active_consumer_super_stream;

use std::env;

static SUPER_STREAM: &str = "invoices";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match &arg[..] {
            "-h" | "--help" => help(),
            "--consumer" => {
                single_active_consumer_super_stream::start_consumer().await?;
            }

            "--producer" => send_super_stream::start_producer().await?,

            arg if arg.starts_with("-") => {
                eprintln!("Unknown argument: {}", arg);
            }

            _ => {
                eprintln!("Unknown argument: {}", arg);
                help();
            }
        }
    }
    Ok(())
}

fn help() {
    println!("--consumer or --producer")
}
