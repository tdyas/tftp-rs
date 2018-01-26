use std::io;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use ascii::{AsciiString, IntoAsciiString};
use bytes::{BytesMut, BufMut};
use futures::prelude::*;
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Handle;

use super::conn::TftpConnState;
use super::proto::TftpPacket;
use super::udp_stream::{Datagram, RawUdpStream};

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
    root: PathBuf,
}

impl TftpServer {
    #[async(boxed)]
    fn do_read_request(conn_state: Box<TftpConnState>, filename: AsciiString) -> io::Result<()> {
        if filename != "foo.dat" {
            await!(conn_state.send_error(1, b"File not found"));
            return Ok(());
        }

        let data = {
            let mut data = BytesMut::with_capacity(500);
            for _ in 0..data.capacity() {
                data.put_u8(0);
            }

            let packet = TftpPacket::Data { block: 1, data: &(data.freeze()) };
            let mut buffer = BytesMut::with_capacity(16384);
            packet.encode(&mut buffer);

            buffer.freeze()
        };

        let next_datagram = await!(conn_state.send_and_receive_next(data))?;

        let next_packet = TftpPacket::from_bytes(&next_datagram.data);
        match next_packet {
            Ok(TftpPacket::Ack(1)) => {
                println!("Received proper ACK");
            },
            Err(err) => return Err(err),
            _ => {
                println!("unexpected state")
            },
        }

        Ok(())
    }

    fn do_request(raw_request: Datagram, handle: Handle) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr, &handle).unwrap();
        let reply_stream = RawUdpStream::new(reply_socket);

        let conn_state = Box::new(TftpConnState::new(reply_stream, raw_request.addr, &handle));

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
            TftpPacket::ReadRequest { filename, .. } => {
                let read_request_future = TftpServer::do_read_request(conn_state, filename.into_ascii_string().unwrap());

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
    fn main_loop(incoming: RawUdpStream, handle: Handle) -> io::Result<()> {
        #[async]
        for packet in incoming {
            TftpServer::do_request(packet, handle.clone())
        }
        Ok(())
    }

    pub fn bind<P>(addr: &SocketAddr, handle: Handle, root: P) -> io::Result<TftpServer>
        where P: AsRef<Path> {

        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(Error::new(ErrorKind::InvalidInput, "not directory or does not exist"));
        }

        let socket = UdpSocket::bind(addr, &handle)?;
        let stream = RawUdpStream::new(socket);

        let incoming = TftpServer::main_loop(stream, handle.clone());

        Ok(TftpServer {
            incoming: incoming,
            root: root.to_path_buf(),
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
