use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use futures::future::Either;
use tokio_core::reactor::{Handle, Timeout};

use super::proto::TftpPacket;
use super::udp_stream::{Datagram, RawUdpStream};

type OutgoingSink = Box<Sink<SinkItem=Datagram, SinkError=io::Error>>;
type IncomingStream = Box<Stream<Item=Datagram, Error=io::Error>>;
type IncomingFuture = Box<Future<Item=(Option<Datagram>, IncomingStream), Error=(io::Error, IncomingStream)>>;

struct Inner {
    outgoing: Option<OutgoingSink>,
    incoming_future: Option<IncomingFuture>,
    remaining_stream: Option<IncomingStream>,
}

pub(crate) struct TftpConnState {
    main_remote_addr: SocketAddr,
    remote_addr: RefCell<Option<SocketAddr>>,
    handle: Handle,
    trace_packets: bool,
    max_retries: u16,
    inner: RefCell<Inner>,
}

impl TftpConnState {
    pub fn new(stream: RawUdpStream, remote_addr: SocketAddr, handle: &Handle) -> TftpConnState {
        let (outgoing, incoming) = stream.split();

        TftpConnState {
            remote_addr: RefCell::new(Some(remote_addr.clone())),
            main_remote_addr: remote_addr.clone(),
            handle: handle.clone(),
            trace_packets: true,
            max_retries: 5,
            inner: RefCell::new(Inner {
                outgoing: Some(Box::new(outgoing)),
                incoming_future: None,
                remaining_stream: Some(Box::new(incoming)),
            }),
        }
    }

    #[async(boxed)]
    pub fn send_error(self: Rc<Self>, code: u16, message: &'static [u8]) -> io::Result<()> {
        let remote_addr_opt = self.remote_addr.borrow_mut().take();
        if let Some(remote_addr) = remote_addr_opt {
            let error_packet = TftpPacket::Error { code: code, message: message };
            let mut buffer = BytesMut::with_capacity(200);
            error_packet.encode(&mut buffer);

            let datagram = Datagram { addr: remote_addr, data: buffer.freeze() };
            let sink = self.get_sink();
            let sink = await!(sink.send(datagram))?;
            self.put_sink(sink);
        }
        Ok(())
    }

    fn get_sink(&self) -> OutgoingSink {
        let sink = self.inner.borrow_mut().outgoing.take().expect("multiple sends in flight");
        sink
    }

    fn put_sink(&self, sink: OutgoingSink) {
        self.inner.borrow_mut().outgoing = Some(sink);
    }

    fn get_incoming(&self) -> IncomingFuture {
        let mut inner = self.inner.borrow_mut();
        let incoming_future_opt = inner.incoming_future.take();
        if let Some(incoming_future) = incoming_future_opt {
            assert!(inner.remaining_stream.is_none());
            return incoming_future;
        } else {
            let remaining_stream_opt = inner.remaining_stream.take();
            if let Some(stream) = remaining_stream_opt {
                let incoming_future = stream.into_future();
                return Box::new(incoming_future);
            } else {
                panic!("no stream available");
            }
        }
    }

    fn put_incoming_future(&self, future: IncomingFuture) {
        let mut inner = self.inner.borrow_mut();
        assert!(inner.incoming_future.is_none() && inner.remaining_stream.is_none());
        inner.incoming_future = Some(future);
    }

    fn put_incoming_stream(&self, stream: IncomingStream) {
        let mut inner = self.inner.borrow_mut();
        assert!(inner.incoming_future.is_none() && inner.remaining_stream.is_none());
        inner.remaining_stream = Some(stream);
    }

    #[async(boxed)]
    pub fn send_and_receive_next(
        self: Rc<Self>,
        bytes_to_send: Bytes,
    ) -> io::Result<Rc<Datagram>> {

        if self.trace_packets {
            let packet = TftpPacket::from_bytes(&bytes_to_send).unwrap();
            println!("sending {}", &packet);
        }

        let mut tries = 0;

        while tries < self.max_retries {
            tries += 1;

            // Construct the datagram to be sent to the remote.
            let remote_addr = self.remote_addr.borrow().unwrap_or(self.main_remote_addr);
            let datagram = Datagram { addr: remote_addr, data: bytes_to_send.clone() };

            // Send the packet to the remote.
            let sink = self.get_sink();
            let sink = await!(sink.send(datagram))?;
            self.put_sink(sink);

            // Now wait for the remote to send us a response (with a timeout).
            let timeout_future = Timeout::new(Duration::new(2, 0), &self.handle).unwrap();
            let next_datagram_future = self.get_incoming();
            let event_future = next_datagram_future.select2(timeout_future);

            let event = await!(event_future);
            let next_datagram = match event {
                Ok(Either::A( ((next_datagram_opt, stream), _)  )) => {
                    // Received datagram.
                    self.put_incoming_stream(stream);
                    next_datagram_opt.expect("TODO: handle stream closure")
                },
                Ok(Either::B( (_, future) )) => {
                    // Timed out.
                    self.put_incoming_future(future);
                    continue;
                },
                Err(Either::A( ((err, stream), _) )) => {
                    self.put_incoming_stream(stream);
                    return Err(err);
                },
                Err(Either::B( (err, future) )) => {
                    self.put_incoming_future(future);
                    return Err(err);
                },
            };

            if self.remote_addr.borrow().is_none() {
                self.remote_addr.replace(Some(next_datagram.addr));
            }

            return Ok(Rc::new(next_datagram));
        }

        // If we timed out while trying to receive a response, terminate the connection.
        await!(self.send_error(0, b"timed out"))?;

        Err(io::Error::new(io::ErrorKind::TimedOut, "timed out"))
    }
}
