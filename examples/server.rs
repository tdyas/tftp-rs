use tftp_rs::TftpServer;

#[tokio::main]
async fn main() {
    let mut server = TftpServer::builder()
        .port(9090)
        .read_root(".")
        .write_root(".")
        .build()
        .await
        .unwrap();
    let _ = server.main_loop().await;
}
