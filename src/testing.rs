use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::try_join;

use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::config::TftpConfig;
use crate::proto::TftpPacket;

pub enum Op {
    Send(Bytes),
    Receive(Bytes),
}

pub struct TestContext {
    pub main_remote_addr: SocketAddr,
    pub remote_addr_opt: Option<SocketAddr>,
    socket: UdpSocket,
}

impl TestContext {
    pub fn new(socket: UdpSocket, server_addr: &SocketAddr) -> TestContext {
        TestContext {
            main_remote_addr: server_addr.clone(),
            remote_addr_opt: None,
            socket: socket,
        }
    }
}

#[derive(Debug)]
pub struct TestError(String);

impl TestError {
    pub fn new(message: &str) -> TestError {
        TestError(message.to_owned())
    }
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for TestError {
    fn description(&self) -> &str {
        self.0.as_str()
    }
}

pub async fn do_test(server_addr: &SocketAddr, steps: Vec<Op>) -> Result<(), TestError> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = UdpSocket::bind(&addr).await.unwrap();

    let mut context = TestContext::new(socket, &server_addr);
    test_driver(&mut context, steps).await?;
    Ok(())
}

pub fn mk(packet: TftpPacket) -> Bytes {
    let mut buffer = BytesMut::with_capacity(packet.encoded_size());
    packet.encode(&mut buffer);
    buffer.freeze()
}

pub async fn test_driver(context: &mut TestContext, steps: Vec<Op>) -> Result<(), TestError> {
    for step in steps {
        match step {
            Op::Send(bytes) => {
                let remote_addr = context.remote_addr_opt.unwrap_or(context.main_remote_addr);
                let result = context.socket.send_to(&bytes, &remote_addr).await;
                match result {
                    Ok(_) => {}
                    Err(err) => {
                        return Err(TestError::new(err.description()));
                    }
                }
            }
            Op::Receive(expected_bytes) => {
                let mut buffer: Vec<u8> = vec![0; 65535];
                let recv_fut = context.socket.recv_from(&mut buffer);
                let timeout_fut = timeout(Duration::new(5, 0), recv_fut);
                match timeout_fut.await {
                    Ok(Ok((len, remote_addr))) => {
                        if context.remote_addr_opt.is_none() {
                            context.remote_addr_opt = Some(remote_addr);
                        }
                        match TftpPacket::from_bytes(&buffer[0..len]) {
                            Ok(packet) => {
                                println!("TEST: Received {}", &packet);
                            }
                            Err(err) => return Err(TestError::new(err.description())),
                        };
                        let bytes = Bytes::from(&buffer[0..len]);
                        if bytes != expected_bytes {
                            return Err(TestError::new("Packets differ"));
                        }
                    }
                    Ok(Err(err)) => {
                        return Err(TestError::new(err.description()));
                    }
                    Err(_) => return Err(TestError::new("timed out while waiting for packet")),
                }
            }
        }
    }

    Ok(())
}

pub async fn run_client_test<F>(
    test_name: &str,
    config: &TftpConfig,
    test: impl FnOnce(SocketAddr, TftpConfig) -> F,
    steps: Vec<Op>,
) where
    F: Future<Output = Result<(), TestError>>,
{
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("bind address");
    let socket = UdpSocket::bind(&addr).await.expect("bind socket");
    let server_addr = socket.local_addr().expect("server address");

    let mut context = TestContext::new(socket, &server_addr);
    let driver_fut = test_driver(&mut context, steps);

    let test_fut = test(server_addr, config.clone());

    println!("TEST '{}'", test_name);
    let result = try_join!(test_fut, driver_fut).map(|_| ());
    match result {
        Ok(_) => println!("TEST '{}': PASSED", test_name),
        Err(err) => panic!("Test '{}' failed: {}", test_name, err),
    }
}
