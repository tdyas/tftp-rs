use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use ascii::{AsciiString, IntoAsciiString};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::fs::File;
use tokio::prelude::*;

use super::conn::TftpConnState;
use super::proto::{DEFAULT_BLOCK_SIZE, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE, ERR_FILE_NOT_FOUND,
                   ERR_INVALID_OPTIONS, TftpPacket};
use crate::conn::Datagram;

pub struct TftpServer {
    socket: UdpSocket,
    reader_factory: Box<dyn ReaderFactory>,
    local_addr: SocketAddr,
}

#[async_trait]
pub trait ReaderFactory {
    async fn get_reader(&self, path: &str) -> io::Result<Bytes>;
}

struct FileReaderFactory {
    root: PathBuf,
}

#[async_trait]
impl ReaderFactory for FileReaderFactory {
    async fn get_reader(&self, path: &str) -> io::Result<Bytes> {
        let path = self.root.as_path().join(path).to_owned();
        let stat = tokio::fs::metadata(path.clone()).await?;
        if stat.is_file() {
            let mut file = File::open(path.clone()).await?;
            let mut contents = vec![];
            file.read_to_end(&mut contents).await?;
            Ok(Bytes::from(contents))
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
}

struct NullReaderFactory;

#[async_trait]
impl ReaderFactory for NullReaderFactory {
    async fn get_reader(&self, _path: &str) -> io::Result<Bytes> {
        Err(io::Error::from(io::ErrorKind::NotFound))
    }
}

pub struct Builder {
    addr: Option<SocketAddr>,
    reader_factory: Option<Box<dyn ReaderFactory>>,
}

impl Builder {
    pub fn addr(mut self, addr: SocketAddr) -> Builder {
        self.addr = Some(addr);
        self
    }

    pub fn port(self, port: u16) -> Builder {
        self.addr(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port).into())
    }

    pub fn reader_factory(mut self, reader_factory: Box<dyn ReaderFactory>) -> Builder {
        self.reader_factory = Some(reader_factory);
        self
    }

    pub fn read_root<P: AsRef<Path>>(self, path: P) -> Builder {
        let root = path.as_ref().to_path_buf();
        self.reader_factory(Box::new(FileReaderFactory { root: root }))
    }

    pub fn build(self) -> io::Result<TftpServer> {
        let addr = self.addr.ok_or(io::Error::new(io::ErrorKind::InvalidInput, "no address or port specified"))?;
        let reader_factory = self.reader_factory.unwrap_or(Box::new(NullReaderFactory));
        TftpServer::bind(&addr, reader_factory)
    }
}

impl TftpServer {
    pub fn builder() -> Builder {
        Builder {
            addr: None,
            reader_factory: None,
        }
    }

    async fn do_read_request(
        mut conn_state: TftpConnState,
        _mode: AsciiString,
        options: HashMap<String, String>,
        file_bytes: Bytes,
    ) -> io::Result<()> {
        let mut current_block_num: u16 = 1;
        let mut block_size: u16 = DEFAULT_BLOCK_SIZE;
        let mut option_err: Option<(u16, &'static [u8])> = None;
        let mut reply_options: HashMap<&'static [u8], Box<str>> = HashMap::new();
        let mut current_file_bytes_index: u16 = 0;

        if options.len() > 0 {
            if let Some(requested_block_size_str) = options.get("blksize") {
                let requested_block_size_str = requested_block_size_str.to_string();
                match requested_block_size_str.parse::<u16>() {
                    Ok(requested_block_size) => {
                        if requested_block_size < MIN_BLOCK_SIZE {
                            option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                        } else if requested_block_size > MAX_BLOCK_SIZE {
                            block_size = MAX_BLOCK_SIZE;
                            reply_options.insert(b"blksize", block_size.to_string().into_boxed_str());
                        } else {
                            block_size = requested_block_size;
                            reply_options.insert(b"blksize", block_size.to_string().into_boxed_str());
                        }
                    }
                    Err(_) => {
                        option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                    }
                }
            }
            if let Some(tsize_str) = options.get("tsize") {
                if tsize_str != "0" {
                    option_err = Some((ERR_INVALID_OPTIONS, b"Invalid tsize option"));
                } else {
                    reply_options.insert(b"tsize", file_bytes.len().to_string().into_boxed_str());
                }
            }
        }

        if let Some((code, message)) = option_err {
            let _ = conn_state.send_error(code, message).await;
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        if reply_options.len() > 0 {
            let bytes = {
                let reply_options_1 = reply_options.iter().map(|(&k, ref v)| (k, v.as_bytes())).collect();
                let packet = TftpPacket::OptionsAck(reply_options_1);
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);
                buffer.freeze()
            };
            let next_datagram = conn_state.send_and_receive_next(bytes.clone()).await?;
            let next_packet = TftpPacket::from_bytes(&next_datagram.data);
            println!("Received packet: {:?}", &next_packet);
            match next_packet {
                Ok(TftpPacket::Ack(0)) => {
                    // Client acknowledges options with ACK to block "0".
                },
                Ok(TftpPacket::Error { code, message }) => {
                    println!("Client sent error: code={}, message={}", code, message.into_ascii_string().unwrap());
                    return Ok(());
                },
                Ok(ref packet) => {
                    println!("Unexpected packet: {}", packet);
                    option_err = Some((1, b"Protocol violation"));
                },
                Err(err) => return Err(err),
            }
        }

        if let Some((code, message)) = option_err {
            let _ = conn_state.send_error(code, message).await;
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        loop {
            // Read in the next block of data.
            let mut n: u16 = file_bytes.len() as u16 - current_file_bytes_index;
            if n > block_size {
                n = block_size;
            }

            let data_packet_bytes = {
                let packet = TftpPacket::Data {
                    block: current_block_num,
                    data: &file_bytes[current_file_bytes_index as usize..n as usize],
                };
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);

                buffer.freeze()
            };

            current_file_bytes_index += n;

            loop {
                let next_datagram = conn_state.send_and_receive_next(data_packet_bytes.clone()).await?;
                let next_packet = TftpPacket::from_bytes(&next_datagram.data);
                match next_packet {
                    Ok(TftpPacket::Ack(block_num)) => {
                        if block_num == current_block_num {
                            current_block_num += 1;
                            break;
                        }
                    },
                    Ok(TftpPacket::Error { code, message }) => {
                        println!("Client has error: code={}, message={}", code, message.into_ascii_string().unwrap());
                        return Ok(());
                    },
                    Ok(ref packet) => {
                        println!("Unexpected packet: {}", packet);
                        return Ok(());
                    },
                    Err(err) => return Err(err),
                }
            }

            if n < block_size {
                break;
            }
        }

        println!("Transfer done.");
        Ok(())
    }

    async fn do_request(raw_request: Datagram, reader_factory: &Box<dyn ReaderFactory>) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr).unwrap();

        let mut conn_state = TftpConnState::new(reply_socket, raw_request.addr);

        let request = match TftpPacket::from_bytes(&raw_request.data[..]) {
            Ok(req) => {
                println!(">> {}: {}", &raw_request.addr, &req);
                req
            }
            Err(err) => {
                println!("-- {}: {}", &raw_request.addr, &err);
                return;
            }
        };

        match request {
            TftpPacket::ReadRequest { filename, mode, ref options } => {
                let filename = filename.into_ascii_string().unwrap();
                let mode = mode.into_ascii_string().unwrap();
                let options = options.iter().map(|(&k, &v)| {
                    let k = k.into_ascii_string().unwrap().to_string();
                    let v = v.into_ascii_string().unwrap().to_string();
                    (k.to_ascii_lowercase(), v.to_ascii_lowercase())
                }).collect::<HashMap<_, _>>();

                let filename_copy = filename.to_string();
                let file_bytes = match reader_factory.get_reader(&filename_copy).await {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        let _ = conn_state.send_error(ERR_FILE_NOT_FOUND, b"File not found").await;
                        return;
                    }
                };

                let read_fut = TftpServer::do_read_request(conn_state, mode, options, file_bytes.into());

                tokio::spawn(async move {
                    let _ = read_fut.await;
                    ()
                });
            }
            _ => {}
        }

        ()
    }

    pub async fn main_loop(&mut self) -> io::Result<()> {
        let mut buffer = Vec::with_capacity(65535);
        loop {
            let (packet_length, remote_addr) = self.socket.recv_from(&mut buffer).await?;
            let datagram = Datagram { data: Bytes::from(&buffer[0..packet_length]), addr: remote_addr};
            TftpServer::do_request(datagram, &self.reader_factory).await;
        }
    }

    pub fn bind(addr: &SocketAddr, reader_factory: Box<dyn ReaderFactory>) -> io::Result<TftpServer> {
        let socket = UdpSocket::bind(addr)?;
        let local_addr = socket.local_addr().unwrap();

        Ok(TftpServer {
            socket: socket,
            reader_factory: reader_factory,
            local_addr: local_addr,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::fmt;
    use std::io;
    use std::net::SocketAddr;

    use bytes::{BufMut, Bytes, BytesMut};
    use tokio::net::UdpSocket;

    use super::{ReaderFactory, TftpServer};
    use super::super::proto::{ERR_INVALID_OPTIONS, TftpPacket};

    use async_trait::async_trait;

    enum Op {
        Send(Bytes),
        Receive(Bytes),
    }

    struct TestContext {
        main_remote_addr: SocketAddr,
        remote_addr_opt: Option<SocketAddr>,
        socket: UdpSocket,
    }

    #[derive(Debug)]
    struct TestError(String);

    impl TestError {
        fn new(message: &str) -> TestError {
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

    struct TestReaderFactory {
        data: Bytes,
    }

    #[async_trait]
    impl ReaderFactory for TestReaderFactory {
        async fn get_reader(&self, path: &str) -> io::Result<Bytes> {
            let size = path.parse::<usize>().map_err(|_| io::ErrorKind::NotFound)?;
            Ok(self.data.slice(0, size))
        }
    }

    async fn test_driver(context: &mut TestContext, steps: Vec<Op>) -> Result<(), TestError> {
        for step in steps {
            match step {
                Op::Send(bytes) => {
                    let remote_addr = context.remote_addr_opt.unwrap_or(context.main_remote_addr);
                    let result = context.socket.send_to(&bytes, &remote_addr).await;
                    match result {
                        Ok(_) => {},
                        Err(err) => {
                            return Err(TestError::new(err.description()));
                        }
                    }
                }
                Op::Receive(expected_bytes) => {
                    let buffer = BytesMut::with_capacity(65535);
                    match context.socket.recv_from(&mut buffer).await {
                        Ok((len, remote_addr)) => {
                            if context.remote_addr_opt.is_none() {
                                context.remote_addr_opt = Some(remote_addr);
                            }
                            match TftpPacket::from_bytes(&buffer[0..len]) {
                                Ok(packet) => {
                                    println!("Received {}", &packet);
                                }
                                Err(err) => return Err(TestError::new(err.description())),
                            };
                            let bytes = Bytes::from(&buffer[0..len]);
                            if bytes != expected_bytes {
                                return Err(TestError::new("Packets differ"));
                            }
                        },
                        Err(err) => {
                            return Err(TestError::new(err.description()));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn do_test(server_addr: &SocketAddr, steps: Vec<Op>) -> Result<(), TestError>{
        let addr = "0.0.0.0:0".parse().unwrap();
        let socket = UdpSocket::bind(&addr).unwrap();

        let context = TestContext {
            main_remote_addr: server_addr.clone(),
            remote_addr_opt: None,
            socket: socket,
        };

        test_driver(&mut context, steps).await?;
        Ok(())
    }

    fn mk(packet: TftpPacket) -> Bytes {
        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
        packet.encode(&mut buffer);
        buffer.freeze()
    }

    async fn run_test(server_addr: &SocketAddr, test_name: &str, steps: Vec<Op>) {
        let result = do_test(server_addr, steps).await;
        if let Err(err) = result {
            panic!("Test failed ({}): {}", test_name, err);
        }
    }

    #[tokio::test]
    async fn test_read_requests() {
        let data = {
            let mut b = BytesMut::with_capacity(2048);
            for v in 0..b.capacity() {
                b.put((v & 0xFF) as u8);
            }
            b.freeze()
        };

        let reader_factory = Box::new(TestReaderFactory { data: data.clone() });

        let server_addr: SocketAddr = "0.0.0.0:5000".parse().unwrap();
        let server = TftpServer::bind(&server_addr, reader_factory).unwrap();
        let server_addr = server.local_addr().clone();

        tokio::spawn(async {
            let _ = server.main_loop().await;
            ()
        });

        use self::Op::*;
        use self::TftpPacket::*;

        run_test(&server_addr, "file not found", vec![
            Send(mk(ReadRequest { filename: b"missing", mode: b"octet", options: HashMap::default() })),
            Receive(mk(Error { code: 1, message: b"File not found" })),
        ]);

        run_test(&server_addr, "basic read request", vec![
            Send(mk(ReadRequest { filename: b"768", mode: b"octet", options: HashMap::default() })),
            Receive(mk(Data { block: 1, data: &data.slice(0, 512) })),
            Send(mk(Ack(1))),
            Receive(mk(Data { block: 2, data: &data.slice(512, 768) })),
            Send(mk(Ack(2))),
        ]);

        run_test(&server_addr, "block aligned read", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: HashMap::default() })),
            Receive(mk(Data { block: 1, data: &data.slice(0, 512) })),
            Send(mk(Ack(1))),
            Receive(mk(Data { block: 2, data: &data.slice(512, 1024) })),
            Send(mk(Ack(2))),
            Receive(mk(Data { block: 3, data: &[] })),
            Send(mk(Ack(3))),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"3");
        run_test(&server_addr, "blksize option rejected (too small)", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(Error { code: ERR_INVALID_OPTIONS, message: b"Invalid blksize option" })),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"xyzzy");
        run_test(&server_addr, "blksize option rejected (not a number)", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(Error { code: ERR_INVALID_OPTIONS, message: b"Invalid blksize option" })),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"65465");
        let mut options1: HashMap<&[u8], &[u8]> = HashMap::new();
        options1.insert(b"blksize", b"65464");
        run_test(&server_addr, "too large blksize option clamped", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(OptionsAck(options1))),
            Send(mk(Ack(0))),
            Receive(mk(Data { block: 1, data: &data.slice(0, 1024) })),
            Send(mk(Ack(1))),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"768");
        let options1 = options.clone();
        run_test(&server_addr, "larger blksize read", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(OptionsAck(options1))),
            Send(mk(Ack(0))),
            Receive(mk(Data { block: 1, data: &data.slice(0, 768) })),
            Send(mk(Ack(1))),
            Receive(mk(Data { block: 2, data: &data.slice(768, 1024) })),
            Send(mk(Ack(2))),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"tsize", b"0");
        let mut options1: HashMap<&[u8], &[u8]> = HashMap::new();
        options1.insert(b"tsize", b"768");
        run_test(&server_addr, "tsize option", vec![
            Send(mk(ReadRequest { filename: b"768", mode: b"octet", options: options })),
            Receive(mk(OptionsAck(options1))),
            Send(mk(Ack(0))),
            Receive(mk(Data { block: 1, data: &data.slice(0, 512) })),
            Send(mk(Ack(1))),
            Receive(mk(Data { block: 2, data: &data.slice(512, 768) })),
            Send(mk(Ack(2))),
        ]);
    }
}
