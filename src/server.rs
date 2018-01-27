use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{metadata, File};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use ascii::{AsciiString, IntoAsciiString};
use bytes::{BufMut, BytesMut};
use futures::prelude::*;
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Handle;
use tokio_io::io::AllowStdIo;
use tokio_io::io::read as tokio_read;

use super::conn::TftpConnState;
use super::proto::{DEFAULT_BLOCK_SIZE, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE, ERR_FILE_NOT_FOUND,
                   ERR_INVALID_OPTIONS, TftpPacket};
use super::udp_stream::{Datagram, RawUdpStream};

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
    local_addr: SocketAddr,
}

pub trait ReaderFactory {
    fn get_reader(&self, path: &str) -> io::Result<(Box<io::Read>, Option<usize>)>;
}

struct FileReaderFactory {
    root: PathBuf,
}

impl ReaderFactory for FileReaderFactory {
    fn get_reader(&self, path: &str) -> io::Result<(Box<io::Read>, Option<usize>)> {
        let path = self.root.as_path().join(path);
        let stat = metadata(&path)?;
        if stat.is_file() {
            let file = File::open(&path)?;
            Ok((Box::new(io::BufReader::new(file)), Some(stat.len() as usize)))
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
}

struct NullReaderFactory;

impl ReaderFactory for NullReaderFactory {
    fn get_reader(&self, _: &str) -> io::Result<(Box<io::Read>, Option<usize>)> {
        Err(io::Error::from(io::ErrorKind::NotFound))
    }
}

struct ReadState {
    reader: RefCell<Option<Box<io::Read>>>,
}

pub struct Builder {
    addr: Option<SocketAddr>,
    handle: Option<Handle>,
    reader_factory: Option<Box<ReaderFactory>>,
}

impl Builder {
    pub fn handle(mut self, handle: &Handle) -> Builder {
        self.handle = Some(handle.clone());
        self
    }

    pub fn addr(mut self, addr: SocketAddr) -> Builder {
        self.addr = Some(addr);
        self
    }

    pub fn port(self, port: u16) -> Builder {
        self.addr(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port).into())
    }

    pub fn reader_factory(mut self, reader_factory: Box<ReaderFactory>) -> Builder {
        self.reader_factory = Some(reader_factory);
        self
    }

    pub fn read_root<P: AsRef<Path>>(self, path: P) -> Builder {
        let root = path.as_ref().to_path_buf();
        self.reader_factory(Box::new(FileReaderFactory { root: root }))
    }

    pub fn build(self) -> io::Result<TftpServer> {
        let handle = self.handle.ok_or(io::Error::new(io::ErrorKind::InvalidInput, "no handle specified"))?;
        let addr = self.addr.ok_or(io::Error::new(io::ErrorKind::InvalidInput, "no address or port specified"))?;
        let reader_factory = self.reader_factory.unwrap_or(Box::new(NullReaderFactory));
        TftpServer::bind(&addr, handle, reader_factory)
    }
}

impl TftpServer {
    pub fn builder() -> Builder {
        Builder {
            handle: None,
            addr: None,
            reader_factory: None,
        }
    }

    #[async(boxed)]
    fn do_read_request(
        read_state: Box<ReadState>,
        conn_state: Rc<TftpConnState>,
        filename: AsciiString,
        _mode: AsciiString,
        options: HashMap<String, String>,
        reader_factory: Rc<ReaderFactory>,
    ) -> io::Result<()> {
        let (reader, size_opt) = match reader_factory.get_reader(&filename.to_string()) {
            Ok((rdr, size_opt)) => (rdr, size_opt),
            Err(err) => {
                let _ = await!(conn_state.send_error(ERR_FILE_NOT_FOUND, b"File not found"));
                return Err(err);
            }
        };
        read_state.reader.replace(Some(reader));

        let mut current_block_num: u16 = 1;
        let mut block_size: u16 = DEFAULT_BLOCK_SIZE;
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
                } else if let Some(size) = size_opt {
                    reply_options.insert(b"tsize", size.to_string().into_boxed_str());
                }
            }
        }

        if let Some((code, message)) = option_err {
            let _ = await!(conn_state.send_error(code, message));
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
            let local_conn_state = conn_state.clone();
            let next_datagram = await!(local_conn_state.send_and_receive_next(bytes.clone()))?;
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
            let _ = await!(conn_state.send_error(code, message));
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        loop {
            // Read in the next block of data.
            let mut file_buffer = BytesMut::with_capacity(block_size as usize);
            for _ in 0..block_size { file_buffer.put_u8(0); }
            let reader = read_state.reader.borrow_mut().take().unwrap();
            let (reader, file_buffer, n) = await!(tokio_read(AllowStdIo::new(reader), file_buffer))?;
            read_state.reader.replace(Some(reader.into_inner()));

            let data_packet_bytes = {
                let packet = TftpPacket::Data {
                    block: current_block_num,
                    data: &file_buffer[0..n],
                };
                let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                packet.encode(&mut buffer);

                buffer.freeze()
            };

            loop {
                let local_conn_state = conn_state.clone();
                let next_datagram = await!(local_conn_state.send_and_receive_next(data_packet_bytes.clone()))?;
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

            if n < 512 {
                break;
            }
        }

        println!("Transfer done.");
        Ok(())
    }

    fn do_request(raw_request: Datagram, reader_factory: Rc<ReaderFactory>, handle: Handle) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr, &handle).unwrap();
        let reply_stream = RawUdpStream::new(reply_socket);

        let conn_state = Rc::new(TftpConnState::new(reply_stream, raw_request.addr, &handle));

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
                let read_state = Box::new(ReadState { reader: RefCell::new(None) });
                let read_request_future = TftpServer::do_read_request(read_state,
                                                                      conn_state, filename, mode, options, reader_factory.clone());

                handle.spawn(read_request_future.map_err(|err| {
                    println!("Error: {}", &err);
                    ()
                }));
            }
            _ => {}
        }
    }

    #[async(boxed)]
    fn main_loop(incoming: RawUdpStream, reader_factory: Rc<ReaderFactory>, handle: Handle) -> io::Result<()> {
        #[async]
            for packet in incoming {
            TftpServer::do_request(packet, reader_factory.clone(), handle.clone())
        }
        Ok(())
    }

    pub fn bind(addr: &SocketAddr, handle: Handle, reader_factory: Box<ReaderFactory>) -> io::Result<TftpServer> {
        let socket = UdpSocket::bind(addr, &handle)?;
        let stream = RawUdpStream::new(socket);
        let local_addr = stream.local_addr().unwrap();

        let reader_factory = Rc::from(reader_factory);

        let incoming = TftpServer::main_loop(stream, reader_factory, handle.clone());

        Ok(TftpServer {
            incoming: incoming,
            local_addr: local_addr,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

impl Future for TftpServer {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.incoming.poll()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::error::Error;
    use std::fmt;
    use std::io;
    use std::net::SocketAddr;

    use bytes::{BufMut, Bytes, BytesMut, IntoBuf};
    use futures::prelude::*;
    use tokio_core::net::UdpSocket;
    use tokio_core::reactor::{Core, Handle};

    use super::{ReaderFactory, TftpServer};
    use super::super::proto::{ERR_INVALID_OPTIONS, TftpPacket};
    use super::super::udp_stream::{Datagram, RawUdpStream};

    enum Op {
        Send(Bytes),
        Receive(Bytes),
    }

    struct TestContext {
        main_remote_addr: SocketAddr,
        remote_addr_opt: Option<SocketAddr>,
        sink: Option<Box<Sink<SinkItem=Datagram, SinkError=io::Error>>>,
        stream: Option<Box<Stream<Item=Datagram, Error=io::Error>>>,
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

    impl ReaderFactory for TestReaderFactory {
        fn get_reader(&self, path: &str) -> io::Result<(Box<io::Read>, Option<usize>)> {
            let size = path.parse::<usize>().map_err(|_| io::ErrorKind::NotFound)?;
            Ok((Box::new(self.data.slice(0, size).into_buf()), Some(size)))
        }
    }

    #[async]
    fn test_driver(context: Box<RefCell<TestContext>>, steps: Vec<Op>) -> Result<(), TestError> {
        for step in steps {
            match step {
                Op::Send(bytes) => {
                    let main_remote_addr = context.borrow().main_remote_addr;
                    let remote_addr_opt = context.borrow().remote_addr_opt;
                    let remote_addr = remote_addr_opt.unwrap_or(main_remote_addr);

                    let datagram = Datagram { addr: remote_addr, data: bytes };

                    let sink = context.borrow_mut().sink.take().unwrap();
                    let result = await!(sink.send(datagram));
                    match result {
                        Ok(sink) => {
                            context.borrow_mut().sink = Some(Box::new(sink));
                        }
                        Err(err) => {
                            return Err(TestError::new(err.description()));
                        }
                    }
                }
                Op::Receive(bytes) => {
                    let stream = context.borrow_mut().stream.take().unwrap();
                    let result = await!(stream.into_future());

                    let datagram_opt = match result {
                        Ok((datagram_opt, stream)) => {
                            context.borrow_mut().stream = Some(Box::new(stream));
                            datagram_opt
                        }
                        Err((err, stream)) => {
                            context.borrow_mut().stream = Some(Box::new(stream));
                            return Err(TestError::new(err.description()));
                        }
                    };

                    match datagram_opt {
                        Some(datagram) => {
                            if context.borrow().remote_addr_opt.is_none() {
                                context.borrow_mut().remote_addr_opt = Some(datagram.addr);
                            }
                            match TftpPacket::from_bytes(&datagram.data) {
                                Ok(packet) => {
                                    println!("Received {}", &packet);
                                }
                                Err(err) => return Err(TestError::new(err.description())),
                            };

                            if bytes != datagram.data {
                                return Err(TestError::new("Packets differ"));
                            }
                        }
                        None => {
                            return Err(TestError::new("Stream terminated early."));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn do_test(server_addr: &SocketAddr, handle: Handle, steps: Vec<Op>) -> Box<Future<Item=(), Error=TestError>> {
        let addr = "0.0.0.0:0".parse().unwrap();
        let socket = UdpSocket::bind(&addr, &handle).unwrap();
        let stream = RawUdpStream::new(socket);
        let (sink, stream) = stream.split();

        let context = Box::new(RefCell::new(TestContext {
            main_remote_addr: server_addr.clone(),
            remote_addr_opt: None,
            sink: Some(Box::new(sink)),
            stream: Some(Box::new(stream)),
        }));

        Box::new(test_driver(context, steps))
    }

    fn mk(packet: TftpPacket) -> Bytes {
        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
        packet.encode(&mut buffer);
        buffer.freeze()
    }

    fn run_test(core: &mut Core, server_addr: &SocketAddr, test_name: &str, steps: Vec<Op>) {
        let test_driver = do_test(server_addr, core.handle(), steps);
        let result = core.run(test_driver);
        if let Err(err) = result {
            panic!("Test failed ({}): {}", test_name, err);
        }
    }

    #[test]
    fn test_read_requests() {
        let data = {
            let mut b = BytesMut::with_capacity(2048);
            for v in 0..b.capacity() {
                b.put((v & 0xFF) as u8);
            }
            b.freeze()
        };

        let reader_factory = Box::new(TestReaderFactory { data: data.clone() });

        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let server_addr: SocketAddr = "0.0.0.0:5000".parse().unwrap();
        let server = TftpServer::bind(&server_addr, handle.clone(), reader_factory).unwrap();
        let server_addr = server.local_addr().clone();

        handle.spawn(server.map_err(|_| ()));

        use self::Op::*;
        use self::TftpPacket::*;

        run_test(&mut core, &server_addr, "file not found", vec![
            Send(mk(ReadRequest { filename: b"missing", mode: b"octet", options: HashMap::default() })),
            Receive(mk(Error { code: 1, message: b"File not found" })),
        ]);

        run_test(&mut core, &server_addr, "basic read request", vec![
            Send(mk(ReadRequest { filename: b"768", mode: b"octet", options: HashMap::default() })),
            Receive(mk(Data { block: 1, data: &data.slice(0, 512) })),
            Send(mk(Ack(1))),
            Receive(mk(Data { block: 2, data: &data.slice(512, 768) })),
            Send(mk(Ack(2))),
        ]);

        run_test(&mut core, &server_addr, "block aligned read", vec![
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
        run_test(&mut core, &server_addr, "blksize option rejected (too small)", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(Error { code: ERR_INVALID_OPTIONS, message: b"Invalid blksize option" })),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"xyzzy");
        run_test(&mut core, &server_addr, "blksize option rejected (not a number)", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(Error { code: ERR_INVALID_OPTIONS, message: b"Invalid blksize option" })),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"65465");
        let mut options1: HashMap<&[u8], &[u8]> = HashMap::new();
        options1.insert(b"blksize", b"65464");
        run_test(&mut core, &server_addr, "too large blksize option clamped", vec![
            Send(mk(ReadRequest { filename: b"1024", mode: b"octet", options: options })),
            Receive(mk(OptionsAck(options1))),
            Send(mk(Ack(0))),
            Receive(mk(Data { block: 1, data: &data.slice(0, 1024) })),
            Send(mk(Ack(1))),
        ]);

        let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
        options.insert(b"blksize", b"768");
        let options1 = options.clone();
        run_test(&mut core, &server_addr, "larger blksize read", vec![
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
        run_test(&mut core, &server_addr, "tsize option", vec![
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
