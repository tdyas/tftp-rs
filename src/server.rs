use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use tokio_core::net::{UdpCodec, UdpFramed};
use tokio_core::reactor::Handle;

use super::proto::TftpPacket;

struct RawUdpStream;

impl UdpCodec for RawUdpStream {
    type In = (SocketAddr, Vec<u8>);
    type Out = (SocketAddr, Vec<u8>);

    fn decode(&mut self, src: &SocketAddr, buf: &[u8]) -> io::Result<Self::In> {
        Ok((src.clone(), buf.into()))
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> SocketAddr {
        buf.extend(msg.1);
        msg.0
    }
}

struct Inner {
    handle: Handle,
}

pub struct TftpServer {
    inner: Inner,
}

impl TftpServer {
    pub fn bind(addr: &SocketAddr, handle: &Handle) -> io::Result<TftpServer> {
        Ok(TftpServer {
            inner: Inner {
                handle: handle.clone(),
            }
        })
    }
}