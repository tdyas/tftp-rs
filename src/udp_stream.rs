use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};

use bytes::Bytes;
use futures::prelude::*;
use tokio_core::net::UdpSocket;

#[derive(Debug)]
pub(crate) struct Datagram {
    pub addr: SocketAddr,
    pub data: Bytes,
}

#[derive(Debug)]
pub(crate) struct RawUdpStream {
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

    #[allow(dead_code)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
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
