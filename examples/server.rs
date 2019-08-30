extern crate tftp_rs;

use tftp_rs::TftpServer;
use pin_utils::pin_mut;

#[tokio::main]
async fn main() {
    let server = TftpServer::builder()
        .port(9090)
        .read_root(".")
        .build()
        .await
        .unwrap();
    pin_mut!(server);
    let _ = server.main_loop().await;
}
