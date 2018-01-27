use std::cell::RefCell;
use std::fs::File;
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
use super::proto::TftpPacket;
use super::udp_stream::{Datagram, RawUdpStream};

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
    local_addr: SocketAddr,
}

pub trait ReaderFactory {
    fn get_reader(&self, path: &str) -> io::Result<Box<io::Read>>;
}

struct FileReaderFactory {
    root: PathBuf,
}

impl ReaderFactory for FileReaderFactory {
    fn get_reader(&self, path: &str) -> io::Result<Box<io::Read>> {
        let path = self.root.as_path().join(path);
        let file = File::open(path)?;
        Ok(Box::new(io::BufReader::new(file)))
    }
}

struct NullReaderFactory;

impl ReaderFactory for NullReaderFactory {
    fn get_reader(&self, _: &str) -> io::Result<Box<io::Read>> {
        Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }
}

struct ReadState {
    reader: RefCell<Option<Box<io::Read>>>,
}

pub struct Builder {
    addr: Option<SocketAddr>,
    handle: Option<Handle>,
    reader_factory: Option<Box<ReaderFactory>>
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
            reader_factory: None
        }
    }

    #[async(boxed)]
    fn do_read_request(
        read_state: Box<ReadState>,
        conn_state: Rc<TftpConnState>,
        filename: AsciiString,
        _mode: AsciiString,
        reader_factory: Rc<ReaderFactory>,
    ) -> io::Result<()> {
        let reader = match reader_factory.get_reader(&filename.to_string()) {
            Ok(rdr) => rdr,
            Err(err) => {
                let _ = await!(conn_state.send_error(1, b"File not found"));
                return Err(err);
            },
        };
        read_state.reader.replace(Some(reader));

        let mut current_block_num: u16 = 1;

        loop {
            // Read in the next block of data.
            let mut file_buffer = BytesMut::with_capacity(512);
            file_buffer.put_slice(&[0; 512]);
            let reader = read_state.reader.borrow_mut().take().unwrap();
            let (reader, file_buffer, n) = await!(tokio_read(AllowStdIo::new(reader), file_buffer))?;
            read_state.reader.replace(Some(reader.into_inner()));

            let data_packet_bytes = {
                let packet = TftpPacket::Data {
                    block: current_block_num,
                    data: &file_buffer[0..n],
                };
                let mut buffer = BytesMut::with_capacity(16384);
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
            },
            Err(err) => {
                println!("-- {}: {}", &raw_request.addr, &err);
                return;
            },
        };

        match request {
            TftpPacket::ReadRequest { filename, mode, .. } => {
                let filename = filename.into_ascii_string().unwrap();
                let mode = mode.into_ascii_string().unwrap();
                let read_state = Box::new(ReadState { reader: RefCell::new(None) });
                let read_request_future = TftpServer::do_read_request(read_state, conn_state, filename, mode, reader_factory.clone());

                handle.spawn(read_request_future.map_err(|err| {
                    println!("Error: {}", &err);
                    ()
                }));
            },
            _ => {
            },
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
    use std::error::Error;
    use std::fmt;
    use std::io;
    use std::net::SocketAddr;

    use bytes::{BufMut, Bytes, BytesMut, IntoBuf};
    use futures::prelude::*;
    use tokio_core::net::UdpSocket;
    use tokio_core::reactor::{Core, Handle};

    use super::{ReaderFactory, TftpServer};
    use super::super::proto::TftpPacket;
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
        fn get_reader(&self, path: &str) -> io::Result<Box<io::Read>> {
            let size = path.parse::<usize>().map_err(|_| io::ErrorKind::NotFound)?;
            Ok(Box::new(self.data.slice(0, size).into_buf()))
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
                        },
                        Err(err) => {
                            return Err(TestError::new(err.description()));
                        },
                    }
                },
                Op::Receive(bytes) => {
                    let stream = context.borrow_mut().stream.take().unwrap();
                    let result = await!(stream.into_future());

                    let datagram_opt = match result {
                        Ok((datagram_opt, stream)) => {
                            context.borrow_mut().stream = Some(Box::new(stream));
                            datagram_opt
                        },
                        Err((err, stream)) => {
                            context.borrow_mut().stream = Some(Box::new(stream));
                            return Err(TestError::new(err.description()));
                        },
                    };

                    match datagram_opt {
                        Some(datagram) => {
                            if context.borrow().remote_addr_opt.is_none() {
                                context.borrow_mut().remote_addr_opt = Some(datagram.addr);
                            }
                            match TftpPacket::from_bytes(&datagram.data) {
                                Ok(packet) => {
                                    println!("Received {}", &packet);
                                },
                                Err(err) => return Err(TestError::new(err.description())),
                            };

                            if bytes != datagram.data {
                                return Err(TestError::new("Packets differ"));
                            }
                        },
                        None => {
                            return Err(TestError::new("Stream terminated early."))
                        },
                    }
                },
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
        let mut buffer = BytesMut::with_capacity(1024);
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

        let reader_factory = Box::new(TestReaderFactory { data: data });

        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let server_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let server = TftpServer::bind(&server_addr, handle.clone(), reader_factory).unwrap();
        let server_addr = server.local_addr().clone();

        handle.spawn(server.map_err(|_| ()));

        run_test(&mut core, &server_addr, "file not found", vec![
            Op::Send(mk(TftpPacket::ReadRequest { filename: b"missing", mode: b"octet" })),
            Op::Receive(mk(TftpPacket::Error { code: 1, message: b"File not found"})),
        ]);
    }
}
