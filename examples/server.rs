extern crate tokio_core;
extern crate tftp_rs;

use tokio_core::reactor::Core;

use tftp_rs::TftpServer;

fn main() {
    let mut core = Core::new().unwrap();
    let server = TftpServer::builder()
        .handle(&core.handle())
        .port(9090)
        .read_root(".")
        .build()
        .unwrap();
    core.run(server).unwrap();
}
