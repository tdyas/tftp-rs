extern crate tokio_core;
extern crate tftp_rs;

use std::net::SocketAddr;
use std::path::Path;

use tokio_core::reactor::Core;
use tftp_rs::TftpServer;

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
    let server = TftpServer::bind(&addr, handle, Path::new(".")).unwrap();
    core.run(server).unwrap();
}
