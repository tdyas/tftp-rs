use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use ascii::{AsciiString, IntoAsciiString};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::Error;
use futures::task::{Context, Poll};
use tokio::fs::File;
use tokio::net::UdpSocket;
use tokio::prelude::*;

use crate::conn::{PacketCheckResult, TftpConnState};
use crate::proto::{
    TftpPacket, DEFAULT_BLOCK_SIZE, ERR_ACCESS_VIOLATION, ERR_FILE_EXISTS, ERR_FILE_NOT_FOUND,
    ERR_ILLEGAL_OPERATION, ERR_INVALID_OPTIONS, ERR_NOT_DEFINED, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE,
};

pub struct TftpServer {
    socket: UdpSocket,
    reader_factory: Box<dyn ReaderFactory + Send + Sync>,
    writer_factory: Box<dyn WriterFactory + Send + Sync>,
    local_addr: SocketAddr,
}

//
// Reader support
//

type AsyncReadWithSend = dyn AsyncRead + Send;

#[async_trait]
pub trait ReaderFactory {
    async fn get_reader(&self, path: &str) -> io::Result<(Box<AsyncReadWithSend>, Option<u64>)>;
}

pub struct FileReaderFactory {
    root: PathBuf,
}

impl FileReaderFactory {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        FileReaderFactory {
            root: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl ReaderFactory for FileReaderFactory {
    async fn get_reader(&self, path: &str) -> io::Result<(Box<AsyncReadWithSend>, Option<u64>)> {
        let path = self.root.as_path().join(path).to_owned();
        match tokio::fs::metadata(path.clone()).await {
            Ok(stat) if stat.is_file() => match File::open(path).await {
                Ok(f) => Ok((Box::new(f) as Box<AsyncReadWithSend>, Some(stat.len()))),
                _ => Err(io::Error::from(io::ErrorKind::NotFound)),
            },
            _ => Err(io::Error::from(io::ErrorKind::NotFound)),
        }
    }
}

pub struct NullReaderFactory;

#[async_trait]
impl ReaderFactory for NullReaderFactory {
    async fn get_reader(&self, _path: &str) -> io::Result<(Box<AsyncReadWithSend>, Option<u64>)> {
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
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        println!("null write for {} bytes", buf.len());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

type AsyncWriteWithSend = dyn AsyncWrite + Send;

#[async_trait]
pub trait WriterFactory {
    async fn get_writer(&self, path: &str) -> io::Result<Box<AsyncWriteWithSend>>;
}

pub struct NullWriterFactory;

#[async_trait]
impl WriterFactory for NullWriterFactory {
    async fn get_writer(&self, _path: &str) -> io::Result<Box<AsyncWriteWithSend>> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }
}

pub struct FileWriterFactory {
    root: PathBuf,
}

impl FileWriterFactory {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        FileWriterFactory {
            root: path.as_ref().to_path_buf(),
        }
    }
}

struct FileWriter(tokio::fs::File);

impl AsyncWrite for FileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

#[async_trait]
impl WriterFactory for FileWriterFactory {
    async fn get_writer(&self, path: &str) -> io::Result<Box<AsyncWriteWithSend>> {
        let path = Path::new(path);
        let components = path.components().take(2).collect::<Vec<_>>();
        match components.get(0) {
            // Allow single-component paths without reference to other directories or root.
            Some(Component::Normal(_)) if components.len() == 1 => (),

            // Deny everything else.
            _ => return Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        };

        let path = self.root.as_path().join(path).to_owned();
        match tokio::fs::metadata(path.clone()).await {
            Ok(_) => Err(io::Error::from(io::ErrorKind::AlreadyExists)),
            Err(_) => {
                let file = tokio::fs::File::create(path.clone()).await?;
                let writer: Box<AsyncWriteWithSend> = Box::new(FileWriter(file));
                Ok(writer)
            }
        }
    }
}

//
// Server builder support
//

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
        self.reader_factory(Box::new(FileReaderFactory::new(path)))
    }

    pub fn writer_factory(
        mut self,
        writer_factory: Box<dyn WriterFactory + Send + Sync>,
    ) -> Builder {
        self.writer_factory = Some(writer_factory);
        self
    }

    pub fn write_root<P: AsRef<Path>>(self, path: P) -> Builder {
        self.writer_factory(Box::new(FileWriterFactory::new(path)))
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
        reader: Box<AsyncReadWithSend>,
        reader_size_opt: Option<u64>,
    ) -> io::Result<()> {
        let mut reader: Pin<Box<AsyncReadWithSend>> = Pin::from(reader);

        let mut current_block_num: u16 = 1;
        let mut block_size: usize = DEFAULT_BLOCK_SIZE as usize;
        let mut option_err: Option<(u16, &'static [u8])> = None;
        let mut reply_options: HashMap<&'static [u8], Box<str>> = HashMap::new();

        if options.len() > 0 {
            if let Some(requested_block_size_str) = options.get("blksize") {
                let requested_block_size_str = requested_block_size_str.to_string();
                match requested_block_size_str.parse::<u16>() {
                    Ok(requested_block_size) => {
                        if requested_block_size < MIN_BLOCK_SIZE {
                            option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                        } else if requested_block_size > MAX_BLOCK_SIZE {
                            block_size = MAX_BLOCK_SIZE as usize;
                            reply_options
                                .insert(b"blksize", block_size.to_string().into_boxed_str());
                        } else {
                            block_size = requested_block_size as usize;
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
                } else if let Some(size) = reader_size_opt {
                    reply_options.insert(b"tsize", size.to_string().into_boxed_str());
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
                    .map(|(&k, ref v)| {
                        (
                            Bytes::copy_from_slice(k),
                            Bytes::copy_from_slice(v.as_bytes()),
                        )
                    })
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

        let mut buffer: Vec<u8> = vec![0; block_size as usize];

        loop {
            // Read in the next block of data.
            let mut data_len: usize = 0;
            while data_len < block_size {
                match reader.read(&mut buffer[data_len..]).await {
                    Ok(n) => {
                        data_len += n;
                        if data_len == block_size || n == 0 {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = conn_state.send_error(ERR_NOT_DEFINED, b"Read error").await;
                        return Err(err);
                    }
                }
            }

            let data_packet_bytes = {
                let packet = TftpPacket::Data {
                    block: current_block_num,
                    data: Bytes::copy_from_slice(&buffer[0..data_len]),
                };
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);

                buffer.freeze()
            };

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

            if data_len < block_size {
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
        writer: Box<AsyncWriteWithSend>,
    ) -> io::Result<()> {
        let mut writer: Pin<Box<AsyncWriteWithSend>> = Pin::from(writer);

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
                .map(|(&k, ref v)| {
                    (
                        Bytes::copy_from_slice(k),
                        Bytes::copy_from_slice(v.as_bytes()),
                    )
                })
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

            match next_data_packet {
                TftpPacket::Data { block, data } => {
                    // Enforce invariant that `next_data_packet` will always be the expected block number.
                    assert!(
                        block == current_block_num,
                        "block should always be the next block given invariants"
                    );

                    // Write the block to the writer.
                    writer.write_all(&data).await?;

                    actual_transfer_size += data.len() as u32;
                    if let Some(size) = expected_transfer_size {
                        if actual_transfer_size > size {
                            let _ = conn_state
                                .send_error(ERR_ILLEGAL_OPERATION, b"size exceeded tsize option")
                                .await;
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
                }
                _ => unreachable!(),
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

        let _ = writer.flush().await;

        Ok(())
    }

    async fn do_request(
        request: TftpPacket,
        addr: SocketAddr,
        reader_factory: &Box<dyn ReaderFactory + Send + Sync>,
        writer_factory: &Box<dyn WriterFactory + Send + Sync>,
    ) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr).await.unwrap();

        let mut conn_state = TftpConnState::new(reply_socket, Some(addr), None);

        match &request {
            TftpPacket::ReadRequest {
                filename,
                mode,
                ref options,
            } => {
                let filename = filename.into_ascii_string().unwrap();
                let mode = mode.into_ascii_string().unwrap();
                let options = options
                    .iter()
                    .map(|(k, v)| {
                        let k = k.into_ascii_string().unwrap().to_string();
                        let v = v.into_ascii_string().unwrap().to_string();
                        (k.to_ascii_lowercase(), v.to_ascii_lowercase())
                    })
                    .collect::<HashMap<_, _>>();

                let filename_copy = filename.to_string();
                let (reader, size_opt) = match reader_factory.get_reader(&filename_copy).await {
                    Ok((reader, size_opt)) => (reader, size_opt),
                    Err(_) => {
                        let _ = conn_state
                            .send_error(ERR_FILE_NOT_FOUND, b"File not found")
                            .await;
                        return;
                    }
                };

                let read_fut =
                    TftpServer::do_read_request(conn_state, mode, options, reader, size_opt);

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
                    .map(|(k, v)| {
                        let k = k.into_ascii_string().unwrap().to_string();
                        let v = v.into_ascii_string().unwrap().to_string();
                        (k.to_ascii_lowercase(), v.to_ascii_lowercase())
                    })
                    .collect::<HashMap<_, _>>();

                let writer: Box<AsyncWriteWithSend> =
                    match writer_factory.get_writer(filename.as_str()).await {
                        Ok(w) => w,
                        Err(err) => {
                            let (code, msg) = match err.kind() {
                                io::ErrorKind::AlreadyExists => {
                                    (ERR_FILE_EXISTS, b"File exists" as &[u8])
                                }
                                _ => (ERR_ACCESS_VIOLATION, b"Access violation" as &[u8]),
                            };
                            let _ = conn_state.send_error(code, msg).await;
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
            let packet_bytes = Bytes::copy_from_slice(&buffer[0..packet_length]);
            let request = match TftpPacket::from_bytes(&packet_bytes) {
                Ok(p) => p,
                Err(_) => continue,
            };
            TftpServer::do_request(
                request,
                remote_addr,
                &self.reader_factory,
                &self.writer_factory,
            )
            .await;
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
    use std::convert::TryInto;
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;

    use async_trait::async_trait;
    use bytes::{BufMut, Bytes, BytesMut};
    use futures::task::{Context, Poll};
    use tokio::io::AsyncWrite;
    use tokio::prelude::*;
    use tokio::sync::mpsc::Sender;

    use super::{ReaderFactory, TftpServer};
    use crate::proto::{
        TftpPacket, ERR_ACCESS_VIOLATION, ERR_FILE_EXISTS, ERR_FILE_NOT_FOUND, ERR_INVALID_OPTIONS,
    };
    use crate::server::{
        AsyncReadWithSend, AsyncWriteWithSend, FileReaderFactory, FileWriterFactory,
        NullReaderFactory, NullWriterFactory, WriterFactory,
    };
    use crate::testing::*;

    struct TestReaderFactory {
        data: Bytes,
    }

    #[async_trait]
    impl ReaderFactory for TestReaderFactory {
        async fn get_reader(
            &self,
            path: &str,
        ) -> io::Result<(Box<AsyncReadWithSend>, Option<u64>)> {
            let size = path.parse::<u64>().map_err(|_| io::ErrorKind::NotFound)?;
            let reader = io::Cursor::new(self.data.slice(0..size.try_into().unwrap()));
            let reader: Box<AsyncReadWithSend> = Box::new(reader);
            Ok((reader, Some(size)))
        }
    }

    async fn run_test(server_addr: &SocketAddr, test_name: &str, steps: Vec<Op>) {
        let result = do_test(server_addr, steps).await;
        if let Err(err) = result {
            panic!("Test failed ({}): {}", test_name, err);
        }
    }

    fn make_data_bytes() -> Bytes {
        let mut b = BytesMut::with_capacity(2048);
        for v in 0..b.capacity() {
            b.put_u8((v & 0xFF) as u8);
        }
        b.freeze()
    }

    #[tokio::test]
    async fn test_read_requests() {
        let data = make_data_bytes();

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
                    filename: Bytes::from_static(b"missing"),
                    mode: Bytes::from_static(b"octet"),
                    options: HashMap::default(),
                })),
                Receive(mk(Error {
                    code: 1,
                    message: Bytes::from_static(b"File not found"),
                })),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "basic read request",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"768"),
                    mode: Bytes::from_static(b"octet"),
                    options: HashMap::default(),
                })),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: data.slice(512..768),
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
                    filename: Bytes::from_static(b"1024"),
                    mode: Bytes::from_static(b"octet"),
                    options: HashMap::default(),
                })),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: data.slice(512..1024),
                })),
                Send(mk(Ack(2))),
                Receive(mk(Data {
                    block: 3,
                    data: Bytes::default(),
                })),
                Send(mk(Ack(3))),
            ],
        )
        .await;

        let mut options: HashMap<Bytes, Bytes> = HashMap::new();
        options.insert(Bytes::from_static(b"blksize"), Bytes::from_static(b"3"));
        run_test(
            &server_addr,
            "blksize option rejected (too small)",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"1024"),
                    mode: Bytes::from_static(b"octet"),
                    options: options,
                })),
                Receive(mk(Error {
                    code: ERR_INVALID_OPTIONS,
                    message: Bytes::from_static(b"Invalid blksize option"),
                })),
            ],
        )
        .await;

        let mut options: HashMap<Bytes, Bytes> = HashMap::new();
        options.insert(Bytes::from_static(b"blksize"), Bytes::from_static(b"xyzzy"));
        run_test(
            &server_addr,
            "blksize option rejected (not a number)",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"1024"),
                    mode: Bytes::from_static(b"octet"),
                    options: options,
                })),
                Receive(mk(Error {
                    code: ERR_INVALID_OPTIONS,
                    message: Bytes::from_static(b"Invalid blksize option"),
                })),
            ],
        )
        .await;

        let mut options: HashMap<Bytes, Bytes> = HashMap::new();
        options.insert(Bytes::from_static(b"blksize"), Bytes::from_static(b"65465"));
        let mut options1: HashMap<Bytes, Bytes> = HashMap::new();
        options1.insert(Bytes::from_static(b"blksize"), Bytes::from_static(b"65464"));
        run_test(
            &server_addr,
            "too large blksize option clamped",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"1024"),
                    mode: Bytes::from_static(b"octet"),
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..1024),
                })),
                Send(mk(Ack(1))),
            ],
        )
        .await;

        let mut options: HashMap<Bytes, Bytes> = HashMap::new();
        options.insert(Bytes::from_static(b"blksize"), Bytes::from_static(b"768"));
        let options1 = options.clone();
        run_test(
            &server_addr,
            "larger blksize read",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"1024"),
                    mode: Bytes::from_static(b"octet"),
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..768),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: data.slice(768..1024),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        let mut options: HashMap<Bytes, Bytes> = HashMap::new();
        options.insert(Bytes::from_static(b"tsize"), Bytes::from_static(b"0"));
        let mut options1: HashMap<Bytes, Bytes> = HashMap::new();
        options1.insert(Bytes::from_static(b"tsize"), Bytes::from_static(b"768"));
        run_test(
            &server_addr,
            "tsize option",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"768"),
                    mode: Bytes::from_static(b"octet"),
                    options: options,
                })),
                Receive(mk(OptionsAck(options1))),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: data.slice(512..768),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;
    }

    // Full write request observed by TestWriterFactory
    struct WritenFile {
        name: String,
        bytes: Vec<u8>,
    }

    struct TestWriterFactory {
        sender: Sender<WritenFile>,
    }

    struct TestWriter {
        sender: Sender<WritenFile>,
        name: String,
        buffer: Option<Vec<u8>>,
    }

    impl Drop for TestWriter {
        fn drop(&mut self) {
            if let Some(buffer) = self.buffer.take() {
                let message = WritenFile {
                    name: self.name.clone(),
                    bytes: buffer,
                };
                if let Err(_) = self.sender.try_send(message) {
                    panic!("unable to send result");
                }
            }
        }
    }

    impl AsyncWrite for TestWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            Pin::new(self.buffer.as_mut().as_mut().unwrap()).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            AsyncWrite::poll_flush(Pin::new(self.buffer.as_mut().as_mut().unwrap()), cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            AsyncWrite::poll_shutdown(Pin::new(self.buffer.as_mut().as_mut().unwrap()), cx)
        }
    }

    #[async_trait]
    impl WriterFactory for TestWriterFactory {
        async fn get_writer(&self, path: &str) -> io::Result<Box<AsyncWriteWithSend>> {
            match path {
                "already_exists" => Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                "denied" => Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                _ => {
                    let writer: Box<AsyncWriteWithSend> = Box::new(TestWriter {
                        sender: self.sender.clone(),
                        name: path.to_string(),
                        buffer: Some(Vec::new()),
                    });
                    Ok(writer)
                }
            }
        }
    }

    #[tokio::test]
    async fn test_write_requests() {
        let data = make_data_bytes();

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<WritenFile>(1);

        let mut server = {
            let reader_factory =
                Box::new(NullReaderFactory) as Box<dyn ReaderFactory + std::marker::Send + Sync>;

            let writer_factory = Box::new(TestWriterFactory { sender: sender });

            let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

            TftpServer::bind(&server_addr, reader_factory, writer_factory)
                .await
                .unwrap()
        };
        let server_addr = server.local_addr().clone();
        println!("server listening at {}", server_addr);

        tokio::spawn(async move {
            let _ = server.main_loop().await;
            ()
        });

        use self::Op::*;
        use self::TftpPacket::*;

        let empty_options: HashMap<Bytes, Bytes> = HashMap::new();

        run_test(
            &server_addr,
            "basic write",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"xyzzy"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: data.slice(512..768),
                })),
                Receive(mk(Ack(2))),
            ],
        )
        .await;
        let result = receiver.recv().await.expect("file was written");
        assert_eq!(result.name, "xyzzy");
        assert_eq!(result.bytes, data.slice(0..768).to_vec());

        run_test(
            &server_addr,
            "block-aligned write",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"xyzzy"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: data.slice(512..1024),
                })),
                Receive(mk(Ack(2))),
                Send(mk(Data {
                    block: 3,
                    data: Bytes::default(),
                })),
                Receive(mk(Ack(3))),
            ],
        )
        .await;
        let result = receiver.recv().await.expect("file was written");
        assert_eq!(result.name, "xyzzy");
        assert_eq!(result.bytes, data.slice(0..1024).to_vec());

        run_test(
            &server_addr,
            "file exists error",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"already_exists"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Error {
                    code: ERR_FILE_EXISTS,
                    message: Bytes::from_static(b"File exists"),
                })),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "file exists error",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"denied"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Error {
                    code: ERR_ACCESS_VIOLATION,
                    message: Bytes::from_static(b"Access violation"),
                })),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_file_factories() {
        let data = make_data_bytes();

        let root = tempfile::tempdir().expect("temp directory created");

        let mut server = {
            let reader_factory = Box::new(FileReaderFactory::new(&root))
                as Box<dyn ReaderFactory + std::marker::Send + Sync>;

            let writer_factory = Box::new(FileWriterFactory::new(&root))
                as Box<dyn WriterFactory + std::marker::Send + Sync>;

            let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

            TftpServer::bind(&server_addr, reader_factory, writer_factory)
                .await
                .unwrap()
        };
        let server_addr = server.local_addr().clone();
        println!("server listening at {}", server_addr);

        tokio::spawn(async move {
            let _ = server.main_loop().await;
            ()
        });

        use self::Op::*;
        use self::TftpPacket::*;

        let empty_options: HashMap<Bytes, Bytes> = HashMap::new();

        let mut input_file = tokio::fs::File::create(root.path().join("xyzzy"))
            .await
            .unwrap();
        input_file.write_all(&data[0..768]).await.unwrap();
        drop(input_file);

        run_test(
            &server_addr,
            "read request",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"xyzzy"),
                    mode: Bytes::from_static(b"octet"),
                    options: HashMap::default(),
                })),
                Receive(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: data.slice(512..768),
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "read request - file not found",
            vec![
                Send(mk(ReadRequest {
                    filename: Bytes::from_static(b"foobar"),
                    mode: Bytes::from_static(b"octet"),
                    options: HashMap::default(),
                })),
                Receive(mk(Error {
                    code: ERR_FILE_NOT_FOUND,
                    message: Bytes::from_static(b"File not found"),
                })),
            ],
        )
        .await;

        run_test(
            &server_addr,
            "write request",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"output"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: data.slice(0..512),
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: data.slice(512..768),
                })),
                Receive(mk(Ack(2))),
            ],
        )
        .await;

        let mut output_file = tokio::fs::File::open(root.path().join("output"))
            .await
            .unwrap();
        let mut buffer: Vec<u8> = Vec::new();
        output_file.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(768, buffer.len());
        assert_eq!(buffer, data.slice(0..768));

        run_test(
            &server_addr,
            "write request - file exists",
            vec![
                Send(mk(WriteRequest {
                    filename: Bytes::from_static(b"output"),
                    mode: Bytes::from_static(b"octet"),
                    options: empty_options.clone(),
                })),
                Receive(mk(Error {
                    code: ERR_FILE_EXISTS,
                    message: Bytes::from_static(b"File exists"),
                })),
            ],
        )
        .await;
    }
}
