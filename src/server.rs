use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::net::SocketAddr;
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

struct ReadState {
    reader: RefCell<Option<io::BufReader<File>>>,
}

impl TftpServer {
    #[async(boxed)]
    fn do_read_request(
        req_state: Box<ReadState>,
        conn_state: Rc<TftpConnState>,
        filename: AsciiString,
        _mode: AsciiString,
        root: Rc<PathBuf>,
    ) -> io::Result<()> {
        let path = root.as_path().join(Path::new(&filename.to_string()));
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => {
                let _ = await!(conn_state.send_error(1, b"File not found"));
                return Err(err);
            },
        };
        let reader = io::BufReader::new(file);
        req_state.reader.replace(Some(reader));

        let mut current_block_num: u16 = 1;

        loop {
            // Read in the next block of data.
            let mut file_buffer = BytesMut::with_capacity(512);
            file_buffer.put_slice(&[0; 512]);
            let reader = req_state.reader.borrow_mut().take().unwrap();
            let (reader, file_buffer, n) = await!(tokio_read(AllowStdIo::new(reader), file_buffer))?;
            req_state.reader.replace(Some(reader.into_inner()));

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

    fn do_request(raw_request: Datagram, root: Rc<PathBuf>, handle: Handle) {
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
                let read_request_future = TftpServer::do_read_request(read_state, conn_state, filename, mode, root.clone());

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
    fn main_loop(incoming: RawUdpStream, root: Rc<PathBuf>, handle: Handle) -> io::Result<()> {
        #[async]
        for packet in incoming {
            TftpServer::do_request(packet, root.clone(), handle.clone())
        }
        Ok(())
    }

    pub fn bind<P>(addr: &SocketAddr, handle: Handle, root: P) -> io::Result<TftpServer>
        where P: AsRef<Path> {

        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not directory or does not exist"));
        }

        let socket = UdpSocket::bind(addr, &handle)?;
        let stream = RawUdpStream::new(socket);
        let local_addr = stream.local_addr().unwrap();

        let incoming = TftpServer::main_loop(stream, Rc::new(root), handle.clone());

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
    use std::path::Path;

    use bytes::{Bytes, BytesMut};
    use futures::prelude::*;
    use tokio_core::net::UdpSocket;
    use tokio_core::reactor::{Core, Handle};

    use super::TftpServer;
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
        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let server_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let server = TftpServer::bind(&server_addr, handle.clone(), Path::new(".")).unwrap();
        let server_addr = server.local_addr().clone();

        handle.spawn(server.map_err(|_| ()));

        run_test(&mut core, &server_addr, "file not found", vec![
            Op::Send(mk(TftpPacket::ReadRequest { filename: b"missing", mode: b"octet" })),
            Op::Receive(mk(TftpPacket::Error { code: 1, message: b"File not found"})),
        ]);
    }
}
