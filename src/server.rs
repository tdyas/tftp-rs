use std::cell::RefCell;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use ascii::{AsciiString, IntoAsciiString};
use bytes::{Bytes, BytesMut, BufMut};
use futures::future::Either;
use futures::prelude::*;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use tokio_core::net::UdpSocket;
use tokio_core::reactor::{Handle, Timeout};

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
struct Datagram {
    addr: SocketAddr,
    data: Bytes,
}

impl fmt::Display for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Packet(addr: {}, data: ? bytes)", self.addr)
    }
}

impl Stream for RawUdpStream  {
    type Item = Datagram;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let (n, addr) = try_nb!(self.socket.recv_from(&mut self.read_buffer));
        let data = Bytes::from(&self.read_buffer[..n]);
        Ok(Async::Ready(Some(Datagram { addr,  data })))
    }
}

impl Sink for RawUdpStream {
    type SinkItem = Datagram;
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
    main_remote_addr: SocketAddr,
    remote_addr: RefCell<Option<SocketAddr>>,
//    config: TftpConfig,
//    file: File,
//    current_block_num: u16,
//    current_block_data: BytesMut,
    trace_packets: bool,
    max_retries: u16,
    stream: RefCell<Option<RawUdpStream>>,
}

impl TftpConnState {
    #[async(boxed)]
    fn send_error(self: Box<Self>, code: u16, message: &'static [u8]) -> io::Result<()> {
        let stream = self.stream.borrow_mut().take().unwrap();
        let remote_addr_opt = self.remote_addr.borrow_mut().take();
        if let Some(remote_addr) = remote_addr_opt {
            let error_packet = TftpPacket::Error { code: code, message: message };
            let mut buffer = BytesMut::with_capacity(200);
            error_packet.encode(&mut buffer);

            let datagram = Datagram { addr: remote_addr, data: buffer.freeze() };
            let stream = await!(stream.send(datagram))?;
            self.stream.replace(Some(stream));
        }
        Ok(())
    }

    #[async(boxed)]
    fn send_and_receive_next(
        self: Box<Self>,
        bytes_to_send: Bytes,
    ) -> io::Result<Rc<Datagram>> {

        if self.trace_packets {
            let packet = TftpPacket::from_bytes(&bytes_to_send).unwrap();
            println!("sending {}", &packet);
        }

        let mut retries = 0;

        while retries < self.max_retries {
            // Construct the datagram to be sent to the remote.
            let remote_addr = self.remote_addr.borrow().unwrap_or(self.main_remote_addr);
            let datagram = Datagram { addr: remote_addr, data: bytes_to_send.clone() };

            // Send the packet to the remote.
            let stream = self.stream.borrow_mut().take().unwrap();
            let stream = await!(stream.send(datagram)).unwrap();
            self.stream.replace(Some(stream));

            // Now wait for the remote to send us a response (with a timeout).
//            let timeout = Timeout::new(Duration::new(2, 0), &self.handle);
            let stream = self.stream.borrow_mut().take().unwrap();
            let next_datagram_future = stream.into_future();
            let next_datagram = match await!(next_datagram_future) {
                Ok((next_datagram_opt, stream)) => {
                    self.stream.replace(Some(stream));
                    next_datagram_opt.expect("TODO: handle closure")
                },
                Err((err, stream)) => {
                    self.stream.replace(Some(stream));
                    return Err(err)
                },
            };

            if self.remote_addr.borrow().is_none() {
                self.remote_addr.replace(Some(next_datagram.addr));
            }

            return Ok(Rc::new(next_datagram));

            retries += 1;
        }

        // If we timed out while trying to receive a response, terminate the connection.
        let stream = self.stream.borrow_mut().take().unwrap();
        let remote_addr_opt = self.remote_addr.borrow_mut().take();
        await!(self.send_error(0, b"timed out")).ok();
        Err(io::Error::new(ErrorKind::TimedOut, "timed out"))
    }
}

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
    handle: Handle,
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

        let conn_state = Box::new(TftpConnState {
            remote_addr: RefCell::new(Some(raw_request.addr.clone())),
            main_remote_addr: raw_request.addr.clone(),
            trace_packets: true,
            max_retries: 5,
            stream: RefCell::new(Some(reply_stream)),
        });

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
                handle.spawn(TftpServer::do_read_request(conn_state, filename.into_ascii_string().unwrap()).map_err(|_| ()))
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
