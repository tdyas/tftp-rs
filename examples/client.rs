extern crate tftp_rs;

use std::env;
use std::io;
use std::net::SocketAddr;

use tftp_rs::{tftp_get, TftpConfig};

use tokio::fs::File;
use tokio::prelude::*;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "usage: {} read SERVER:PORT REMOTE_FILENAME LOCAL_FILENAME",
            &args[0]
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid arguments",
        ));
    }
    let (command, address, remote_filename, local_filename) =
        (&args[1], &args[2], &args[3], &args[4]);
    if command != "read" {
        eprintln!("error: command '{}' does not exist", command);
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid arguments",
        ));
    }

    let address = address
        .parse::<SocketAddr>()
        .expect("Unable to resolve address");
    let config = TftpConfig::default();
    let file_bytes = tftp_get(&address, remote_filename.as_bytes(), b"octet", &config).await?;
    let mut file = File::create(local_filename.to_owned()).await?;
    file.write_all(&file_bytes).await?;

    Ok(())
}
