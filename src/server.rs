use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use ascii::{AsciiString, IntoAsciiString};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::Error;
use futures::task::{Context, Poll};
use tokio::fs::File;
use tokio::net::UdpSocket;
use tokio::prelude::*;

use crate::conn::{OwnedTftpPacket, PacketCheckResult, TftpConnState};
use crate::proto::{TftpPacket, DEFAULT_BLOCK_SIZE, ERR_ACCESS_VIOLATION, ERR_FILE_NOT_FOUND, ERR_INVALID_OPTIONS, ERR_NOT_DEFINED, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, ERR_ILLEGAL_OPERATION};

pub struct TftpServer {
    socket: UdpSocket,
    reader_factory: Box<dyn ReaderFactory + Send + Sync>,
    writer_factory: Box<dyn WriterFactory + Send + Sync>,
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

//
// Writer support
//

struct NullAsyncWrite;

impl AsyncWrite for NullAsyncWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
pub trait WriterFactory {
    async fn check_allow_write(&self, path: &str) -> bool;
    async fn get_writer(&self, path: &str) -> io::Result<Box<dyn AsyncWrite + Send + Sync>>;
}

struct NullWriterFactory;

#[async_trait]
impl WriterFactory for NullWriterFactory {
    async fn check_allow_write(&self, _path: &str) -> bool {
        true
    }

    async fn get_writer(&self, _path: &str) -> io::Result<Box<dyn AsyncWrite + Send + Sync>> {
        Ok(Box::new(NullAsyncWrite) as Box<dyn AsyncWrite + Send + Sync>)
    }
}

pub struct Builder {
    addr: SocketAddr,
    reader_factory: Option<Box<dyn ReaderFactory + Send + Sync>>,
    writer_factory: Option<Box<dyn WriterFactory + Send + Sync>>,
}

impl Builder {
    pub fn addr(mut self, addr: SocketAddr) -> Builder {
        self.addr = addr;
        self
    }

    pub fn port(mut self, port: u16) -> Builder {
        self.addr.set_port(port);
        self
    }

    pub fn reader_factory(
        mut self,
        reader_factory: Box<dyn ReaderFactory + Send + Sync>,
    ) -> Builder {
        self.reader_factory = Some(reader_factory);
        self
    }

    pub fn read_root<P: AsRef<Path>>(self, path: P) -> Builder {
        let root = path.as_ref().to_path_buf();
        self.reader_factory(Box::new(FileReaderFactory { root: root }))
    }

    pub fn writer_factory(
        mut self,
        writer_factory: Box<dyn WriterFactory + Send + Sync>,
    ) -> Builder {
        self.writer_factory = Some(writer_factory);
        self
    }

    pub async fn build(self) -> io::Result<TftpServer> {
        let reader_factory = self
            .reader_factory
            .unwrap_or(Box::new(NullReaderFactory) as Box<dyn ReaderFactory + Send + Sync>);

        let writer_factory = self.writer_factory.unwrap_or(Box::new(NullWriterFactory))
            as Box<dyn WriterFactory + Send + Sync>;

        TftpServer::bind(&self.addr, reader_factory, writer_factory).await
    }
}

impl TftpServer {
    pub fn builder() -> Builder {
        Builder {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            reader_factory: None,
            writer_factory: None,
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
                            reply_options
                                .insert(b"blksize", block_size.to_string().into_boxed_str());
                        } else {
                            block_size = requested_block_size;
                            reply_options
                                .insert(b"blksize", block_size.to_string().into_boxed_str());
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
                let reply_options_1 = reply_options
                    .iter()
                    .map(|(&k, ref v)| (k, v.as_bytes()))
                    .collect();
                let packet = TftpPacket::OptionsAck(reply_options_1);
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);
                buffer.freeze()
            };
            let _ = conn_state
                .send_and_receive_next(bytes.clone(), |packet| match packet {
                    TftpPacket::Ack(0) => PacketCheckResult::Accept,
                    _ => PacketCheckResult::Reject,
                })
                .await?;
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
                    data: &file_bytes[current_file_bytes_index as usize
                        ..(current_file_bytes_index + n) as usize],
                };
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);

                buffer.freeze()
            };

            current_file_bytes_index += n;

            let _ = conn_state
                .send_and_receive_next(data_packet_bytes.clone(), |packet| match packet {
                    TftpPacket::Ack(block_num) => {
                        if *block_num < current_block_num {
                            PacketCheckResult::Ignore
                        } else if *block_num == current_block_num {
                            PacketCheckResult::Accept
                        } else {
                            PacketCheckResult::Reject
                        }
                    }
                    _ => PacketCheckResult::Reject,
                })
                .await?;
            current_block_num += 1;

            if n < block_size {
                break;
            }
        }

        println!("Transfer done.");
        Ok(())
    }

    async fn do_write_request(
        mut conn_state: TftpConnState,
        _mode: AsciiString,
        options: HashMap<String, String>,
        _writer: Box<dyn AsyncWrite + Send + Sync>,
    ) -> io::Result<()> {
        let mut current_block_num: u16 = 0;
        let mut block_size: u16 = DEFAULT_BLOCK_SIZE;
        let mut option_err: Option<(u16, &'static [u8])> = None;
        let mut expected_transfer_size: Option<u32> = None;
        let mut actual_transfer_size: u32 = 0;
        let mut reply_options: HashMap<&'static [u8], Box<str>> = HashMap::new();

        if options.len() > 0 {
            if let Some(requested_block_size_str) = options.get("blksize") {
                let requested_block_size_str = requested_block_size_str.to_string();
                match requested_block_size_str.parse::<u16>() {
                    Ok(requested_block_size) => {
                        if requested_block_size < MIN_BLOCK_SIZE {
                            option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                        } else if requested_block_size > MAX_BLOCK_SIZE {
                            block_size = MAX_BLOCK_SIZE;
                            reply_options
                                .insert(b"blksize", block_size.to_string().into_boxed_str());
                        } else {
                            block_size = requested_block_size;
                            reply_options
                                .insert(b"blksize", block_size.to_string().into_boxed_str());
                        }
                    }
                    Err(_) => {
                        option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                    }
                }
            }
            if let Some(tsize_str) = options.get("tsize") {
                match tsize_str.parse::<u32>() {
                    Ok(tsize) => {
                        expected_transfer_size = Some(tsize);
                        reply_options.insert(b"tsize", tsize_str.to_string().into_boxed_str());
                    }
                    Err(_) => option_err = Some((ERR_INVALID_OPTIONS, b"Invalid tsize option")),
                }
            }
        }

        if let Some((code, message)) = option_err {
            let _ = conn_state.send_error(code, message).await;
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let initial_packet_bytes = if reply_options.len() > 0 {
            let reply_options_1 = reply_options
                .iter()
                .map(|(&k, ref v)| (k, v.as_bytes()))
                .collect();
            let packet = TftpPacket::OptionsAck(reply_options_1);
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        } else {
            let packet = TftpPacket::Ack(current_block_num);
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        };

        let mut next_data_packet = conn_state
            .send_and_receive_next(initial_packet_bytes, |packet: &TftpPacket| match packet {
                TftpPacket::Data { block, .. } => {
                    if *block == current_block_num + 1 {
                        PacketCheckResult::Accept
                    } else {
                        PacketCheckResult::Reject // reject any DATA blocks != 1 since this is start
                    }
                }
                _ => PacketCheckResult::Reject,
            })
            .await?;

        loop {
            current_block_num += 1;

            match next_data_packet.packet() {
                TftpPacket::Data { block, data } => {
                    // Enforce invariant that `next_data_packet` will always be the expected block number.
                    assert!(block == current_block_num, "block should always be the next block given invariants");

                    // Write the block to the writer.
                    //  writer.write_all(data).await?;
                    actual_transfer_size += data.len() as u32;
                    if let Some(size) = expected_transfer_size {
                        if actual_transfer_size > size {
                            let _ = conn_state.send_error(ERR_ILLEGAL_OPERATION, b"size exceeded tsize option").await;
                            return Err(io::Error::from(io::ErrorKind::InvalidInput));
                        }
                    }

                    // End loop if this packet has less than the full block size.
                    // Final ACK will be sent outside of the loop (since we do not need to
                    // wait for another DATA packet.
                    if (data.len() as u16) < block_size {
                        break;
                    }

                    // Send acknowledgement and wait for next DATA packet.
                    let bytes = {
                        let packet = TftpPacket::Ack(current_block_num);
                        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                        packet.encode(&mut buffer);
                        buffer.freeze()
                    };

                    next_data_packet = conn_state
                        .send_and_receive_next(bytes, |packet: &TftpPacket| match packet {
                            TftpPacket::Data { block, .. } => {
                                if *block == current_block_num + 1 {
                                    PacketCheckResult::Accept
                                } else {
                                    PacketCheckResult::Ignore // ignore prior DATA packets
                                }
                            }
                            _ => PacketCheckResult::Reject,
                        })
                        .await?;
                },
                _ => unreachable!()
            };
        }

        let bytes = {
            let packet = TftpPacket::Ack(current_block_num);
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        };
        let _ = conn_state.send(&bytes).await?;

        if conn_state.trace_packets {
            println!("Transfer ended.");
        }

        Ok(())
    }

    async fn do_request(
        request: OwnedTftpPacket,
        reader_factory: &Box<dyn ReaderFactory + Send + Sync>,
        writer_factory: &Box<dyn WriterFactory + Send + Sync>,
    ) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr).await.unwrap();

        let mut conn_state = TftpConnState::new(reply_socket, Some(request.addr), None);

        match request.packet() {
            TftpPacket::ReadRequest {
                filename,
                mode,
                ref options,
            } => {
                let filename = filename.into_ascii_string().unwrap();
                let mode = mode.into_ascii_string().unwrap();
                let options = options
                    .iter()
                    .map(|(&k, &v)| {
                        let k = k.into_ascii_string().unwrap().to_string();
                        let v = v.into_ascii_string().unwrap().to_string();
                        (k.to_ascii_lowercase(), v.to_ascii_lowercase())
                    })
                    .collect::<HashMap<_, _>>();

                let filename_copy = filename.to_string();
                let file_bytes = match reader_factory.get_reader(&filename_copy).await {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        let _ = conn_state
                            .send_error(ERR_FILE_NOT_FOUND, b"File not found")
                            .await;
                        return;
                    }
                };

                let read_fut =
                    TftpServer::do_read_request(conn_state, mode, options, file_bytes.into());

                tokio::spawn(async move {
                    let _ = read_fut.await;
                    ()
                });
            }

            TftpPacket::WriteRequest {
                filename,
                mode,
                ref options,
            } => {
                let filename = filename.into_ascii_string().unwrap();
                let mode = mode.into_ascii_string().unwrap();
                let options = options
                    .iter()
                    .map(|(&k, &v)| {
                        let k = k.into_ascii_string().unwrap().to_string();
                        let v = v.into_ascii_string().unwrap().to_string();
                        (k.to_ascii_lowercase(), v.to_ascii_lowercase())
                    })
                    .collect::<HashMap<_, _>>();

                if !writer_factory.check_allow_write(filename.as_str()).await {
                    let _ = conn_state
                        .send_error(ERR_ACCESS_VIOLATION, b"access violation")
                        .await;
                    return;
                }

                let writer = match writer_factory.get_writer(filename.as_str()).await {
                    Ok(w) => w as Box<dyn AsyncWrite + Send + Sync>,
                    Err(_) => {
                        let _ = conn_state
                            .send_error(ERR_NOT_DEFINED, b"server error")
                            .await;
                        return;
                    }
                };

                let write_fut = TftpServer::do_write_request(conn_state, mode, options, writer);

                tokio::spawn(async move {
                    let _ = write_fut.await;
                    ()
                });
            }

            _ => {}
        }

        ()
    }

    pub async fn main_loop(&mut self) -> io::Result<()> {
        let mut buffer: Vec<u8> = vec![0; 65535];
        loop {
            let (packet_length, remote_addr) = self.socket.recv_from(&mut buffer).await?;
            let packet_bytes = Bytes::from(&buffer[0..packet_length]);
            let owned_packet = OwnedTftpPacket {
                addr: remote_addr,
                bytes: packet_bytes,
            };
            TftpServer::do_request(owned_packet, &self.reader_factory, &self.writer_factory).await;
        }
    }

    pub async fn bind(
        addr: &SocketAddr,
        reader_factory: Box<dyn ReaderFactory + Send + Sync>,
        writer_factory: Box<dyn WriterFactory + Send + Sync>,
    ) -> io::Result<TftpServer> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr().unwrap();

        Ok(TftpServer {
            socket: socket,
            reader_factory: reader_factory,
            writer_factory: writer_factory,
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
    use std::io;
    use std::net::SocketAddr;

    use bytes::{BufMut, Bytes, BytesMut};

    use super::{ReaderFactory, TftpServer};
    use crate::proto::{TftpPacket, ERR_INVALID_OPTIONS};

    use crate::testing::*;

    use crate::server::{NullWriterFactory, WriterFactory};
    use async_trait::async_trait;

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

        let mut server = {
            let reader_factory = Box::new(TestReaderFactory { data: data.clone() })
                as Box<dyn ReaderFactory + std::marker::Send + Sync>;

            let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

            let writer_factory =
                Box::new(NullWriterFactory) as Box<dyn WriterFactory + std::marker::Send + Sync>;

            TftpServer::bind(&server_addr, reader_factory, writer_factory)
                .await
                .unwrap()
        };
        let server_addr = server.local_addr().clone();

        tokio::spawn(async move {
            let _ = server.main_loop().await;
            ()
        });

        use self::Op::*;
        use self::TftpPacket::*;

        run_test(
            &server_addr,
            "file not found",
            vec![
                Send(mk(ReadRequest {
                    filename: b"missing",
                    mode: b"octet",
                    options: HashMap::default(),
                })),
                Receive(mk(Error {
                    code: 1,
                    message: b"File not found",
                })),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "basic read request",
            vec![
                Send(mk(ReadRequest {
                    filename: b"768",
                    mode: b"octet",
                    options: HashMap::default(),
                })),
                Receive(mk(Data {
                    block: 1,
                    data: &data.slice(0, 512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data.slice(512, 768),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "block aligned read",
            vec![
                Send(mk(ReadRequest {
                    filename: b"1024",
                    mode: b"octet",
                    options: HashMap::default(),
                })),
                Receive(mk(Data {
                    block: 1,
                    data: &data.slice(0, 512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data.slice(512, 1024),
                })),
                Send(mk(Ack(2))),
                Receive(mk(Data {
                    block: 3,
                    data: &[],
                })),
                Send(mk(Ack(3))),
            ],
        )
        .await;

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"3");
        run_test(
            &server_addr,
            "blksize option rejected (too small)",
            vec![
                Send(mk(ReadRequest {
                    filename: b"1024",
                    mode: b"octet",
                    options: options,
                })),
                Receive(mk(Error {
                    code: ERR_INVALID_OPTIONS,
                    message: b"Invalid blksize option",
                })),
            ],
        )
        .await;

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"xyzzy");
        run_test(
            &server_addr,
            "blksize option rejected (not a number)",
            vec![
                Send(mk(ReadRequest {
                    filename: b"1024",
                    mode: b"octet",
                    options: options,
                })),
                Receive(mk(Error {
                    code: ERR_INVALID_OPTIONS,
                    message: b"Invalid blksize option",
                })),
            ],
        )
        .await;

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"65465");
        let mut options1: HashMap<&[u8], &[u8]> = HashMap::new();
        options1.insert(b"blksize", b"65464");
        run_test(
            &server_addr,
            "too large blksize option clamped",
            vec![
                Send(mk(ReadRequest {
                    filename: b"1024",
                    mode: b"octet",
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: &data.slice(0, 1024),
                })),
                Send(mk(Ack(1))),
            ],
        )
        .await;

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"768");
        let options1 = options.clone();
        run_test(
            &server_addr,
            "larger blksize read",
            vec![
                Send(mk(ReadRequest {
                    filename: b"1024",
                    mode: b"octet",
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: &data.slice(0, 768),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data.slice(768, 1024),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"tsize", b"0");
        let mut options1: HashMap<&[u8], &[u8]> = HashMap::new();
        options1.insert(b"tsize", b"768");
        run_test(
            &server_addr,
            "tsize option",
            vec![
                Send(mk(ReadRequest {
                    filename: b"768",
                    mode: b"octet",
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: &data.slice(0, 512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data.slice(512, 768),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;
    }
}
