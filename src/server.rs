use std::fmt;
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
use std::path::{Path, PathBuf};

//use ascii::AsciiStr;
use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Handle;

use super::config::TftpConfig;
use super::proto::TftpPacket;


#[derive(Debug)]
struct RawUdpStream {
    socket: UdpSocket,
    read_buffer: Vec<u8>,
    write_buffer: Bytes,
    out_addr: SocketAddr,
}

impl RawUdpStream {
    pub fn new(socket: UdpSocket) -> RawUdpStream {
        RawUdpStream {
            socket: socket,
            read_buffer: vec![0; 64 * 1024],
            write_buffer: Bytes::default(),
            out_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

#[derive(Debug)]
struct Packet {
    addr: SocketAddr,
    data: Bytes,
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Packet(addr: {}, data: ? bytes)", self.addr)
    }
}

impl Stream for RawUdpStream  {
    type Item = Packet;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (n, addr) = try_nb!(self.socket.recv_from(&mut self.read_buffer));
        let data = Bytes::from(&self.read_buffer[..n]);
        Ok(Async::Ready(Some(Packet { addr,  data })))
    }
}

impl Sink for RawUdpStream {
    type SinkItem = Packet;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.write_buffer.len() > 0 {
            self.poll_complete()?;
            if self.write_buffer.len() > 0 {
                return Ok(AsyncSink::NotReady(item));
            }
        }

        self.write_buffer = item.data;
        self.out_addr = item.addr;
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if self.write_buffer.is_empty() {
            return Ok(Async::Ready(()))
        }

        let n = try_nb!(self.socket.send_to(&self.write_buffer, &self.out_addr));
        let wrote_all = n == self.write_buffer.len();
        self.write_buffer.clear();
        if wrote_all {
            Ok(Async::Ready(()))
        } else {
            Err(io::Error::new(io::ErrorKind::Other,
                               "failed to write entire datagram to socket"))
        }
    }

    fn close(&mut self) -> Poll<(), io::Error> {
        try_ready!(self.poll_complete());
        Ok(().into())
    }
}

struct TftpConnState {
    remote_addr: SocketAddr,
    config: TftpConfig,
    file: File,
    current_block_num: u16,
    current_block_data: BytesMut,
}

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
    handle: Handle,
    root: PathBuf,
}

impl TftpServer {
    #[async(boxed)]
    fn do_read_request(stream: RawUdpStream, remote_addr: SocketAddr) -> io::Result<()> {
        let error = TftpPacket::Error { code: 1, message: b"File not found"};
        let mut buffer = BytesMut::with_capacity(16384);
        error.encode(&mut buffer);
        let packet = Packet { addr: remote_addr, data: buffer.freeze() };
        let stream = await!(stream.send(packet));
        Ok(())
    }

    fn do_request(raw_request: Packet, handle: Handle) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr, &handle).unwrap();
        let reply_stream = RawUdpStream::new(reply_socket);

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
            TftpPacket::ReadRequest { .. } => {
                handle.spawn(TftpServer::do_read_request(reply_stream, raw_request.addr).map_err(|_| ()))
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
            handle: handle.clone(),
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
