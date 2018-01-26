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
                await!(conn_state.send_error(1, b"File not found"));
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

        let incoming = TftpServer::main_loop(stream, Rc::new(root), handle.clone());

        Ok(TftpServer {
            incoming: incoming,
        })
    }

//    fn send_final(stream: RawUdpStream, addr: &SocketAddr, msg: TftpPacket) -> Box<Future<Item=(), Error=()>> {
//        let mut buffer = BytesMut::with_capacity(8 * 1024);
//        msg.encode(&mut buffer);
//        let packet = Packet { addr: addr.to_owned(), data: buffer.freeze() };
//
//        let result = stream
//            .send(packet)
//            .and_then(|mut s| s.close())
//            .map(|_| ())
//            .map_err(|_| ());
//
//        Box::new(result)
//    }
//
//    fn handle_rrq_packet(request: Option<TftpPacket>, timedout: bool) -> Option<(TftpPacket, i32)> {
//        None
//    }
//
//    fn rrq_future(state: TftpConnState,
//                  stream: RawUdpStream,
//                  message: Option<TftpPacket>, timeout: i32) -> Box<Future<Item=(), Error=io::Error>> {
//        let send_f: Box<Future<Item=(Option<Packet>, RawUdpStream), Error=io::Error>> = match message {
//            Some(msg) => {
//                let mut buffer = BytesMut::with_capacity(8 * 1024);
//                msg.encode(&mut buffer);
//                let packet = Packet { addr: state.remote_addr, data: buffer.freeze() };
//
//                Box::new(stream.send(packet).and_then(|s| {
//                    s.into_future()
//                }))
//            },
//            None => Box::new(stream.into_future()),
//        };
//
//        send_f.map(|(raw_packet_opt, remainder)| {
//
//        })
//    }
//
//    fn setup_read_request<P>(stream: RawUdpStream, request: TftpPacket, handle: Handle, root: P)
//        where P: AsRef<Path> {
//
//        let TftpPacket::ReadRequest { ref filename, ref mode } = request;
//        let filename = AsciiStr::from_ascii(filename).unwrap();
//        let mode = AsciiStr::from_ascii(mode).unwrap();
//
//        let file = root.as_ref().join(filename);
//        if !file.exists() {
//            handle.spawn(TftpServer::send_final(stream, request.addr, TftpPacket::Error {
//                code: 1,
//                message: &b"File not found."[..],
//            }));
//            return;
//        }
//
//        let config = TftpConfig::default();
//
//        let state = TftpConnState {
//            remote_addr: request.addr,
//            config: config,
//            file: file,
//            current_block_num: 0,
//            current_block_data: config.block_size,
//        };
//
//    }
//
}

impl Future for TftpServer {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.incoming.poll()
    }
}
