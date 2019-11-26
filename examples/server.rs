extern crate tftp_rs;

use pin_utils::pin_mut;
use tftp_rs::TftpServer;

#[tokio::main]
async fn main() {
    let server = TftpServer::builder()
        .port(9090)
        .read_root(".")
        .write_root(".")
        .build()
        .await
        .unwrap();
    pin_mut!(server);
    let _ = server.main_loop().await;
}
