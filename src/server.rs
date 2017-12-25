use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};

use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use futures::{Async, AsyncSink, Poll, Sink, StartSend, Stream};
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Handle;

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

pub struct TftpServer {
    incoming: Box<Future<Item=(), Error=io::Error>>,
}

impl TftpServer {
    pub fn bind(addr: &SocketAddr, handle: Handle) -> io::Result<TftpServer> {
        let socket= UdpSocket::bind(addr, &handle)?;
        let stream= RawUdpStream::new(socket);

        let incoming = stream
            .for_each(move |raw_request| {
                TftpServer::handle_request(raw_request, handle.clone());
                Ok(())
            });

        Ok(TftpServer {
            incoming: Box::new(incoming),
        })
    }

    fn handle_request(raw_request: Packet, handle: Handle) {
        // Bind a socket for this connection.
        let reply_bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let reply_socket = UdpSocket::bind(&reply_bind_addr, &handle).unwrap();
        let reply_stream = RawUdpStream::new(reply_socket);

        let _request = match TftpPacket::from_bytes(&raw_request.data[..]) {
            Ok(req) => {
                println!(">> {}: {}", &raw_request.addr, &req);
                req
            },
            Err(err) => {
                println!("-- {}: {}", &raw_request.addr, &err);
                return;
            },
        };

        let reply = TftpPacket::Error {
            code: 1,
            message: &b"File not found."[..],
        };

        let mut buffer = BytesMut::with_capacity(8 * 1024);
        reply.encode(&mut buffer);
        let reply_packet = Packet { addr: raw_request.addr, data: buffer.freeze() };

        let send_future = reply_stream
            .send(reply_packet)
            .and_then(|mut sink| sink.close())
            .map(|_| ())
            .map_err(|_| ());

        handle.spawn(send_future);
    }
}

impl Future for TftpServer {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.incoming.poll()
    }
}
