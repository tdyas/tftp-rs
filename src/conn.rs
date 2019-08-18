use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::timer::Timeout;

use super::proto::TftpPacket;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub(crate) struct Datagram {
    pub addr: SocketAddr,
    pub data: Bytes,
}

pub(crate) struct TftpConnState {
    socket: UdpSocket,
    main_remote_addr: SocketAddr,
    remote_addr: Option<SocketAddr>,
    trace_packets: bool,
    max_retries: u16,
}

impl TftpConnState {
    pub fn new(socket: UdpSocket, remote_addr: SocketAddr) -> TftpConnState {
        TftpConnState {
            socket: socket,
            remote_addr: Some(remote_addr.clone()),
            main_remote_addr: remote_addr.clone(),
            trace_packets: true,
            max_retries: 5,
        }
    }

    pub async fn send_error(&mut self, code: u16, message: &'static [u8]) -> io::Result<()> {
        if let Some(remote_addr) = self.remote_addr {
            let bytes_to_send = {
                let error_packet = TftpPacket::Error { code: code, message: message };
                let mut buffer = BytesMut::with_capacity(65535);
                error_packet.encode(&mut buffer);
                buffer.freeze()
            };

            self.socket.send_to(&bytes_to_send, &remote_addr).await?;
        }
        Ok(())
    }

    pub async fn send_and_receive_next(&mut self, bytes_to_send: Bytes) -> io::Result<Rc<Datagram>> {
        if self.trace_packets {
            let packet = TftpPacket::from_bytes(&bytes_to_send).unwrap();
            println!("sending {}", &packet);
        }

        let mut tries = 0;

        while tries < self.max_retries {
            tries += 1;

            // Construct the datagram to be sent to the remote.
            let remote_addr = self.remote_addr.unwrap_or(self.main_remote_addr);

            // Send the packet to the remote.
            self.socket.send_to(&bytes_to_send, &remote_addr).await?;

            // Now wait for the remote to send us a response (with a timeout).
            let mut buffer: Vec<u8> = vec![0; 65535];
            let packet_future = self.socket.recv_from(&mut buffer);
            let timeout_future = Timeout::new(packet_future, Duration::new(2, 0));

            let next_datagram = match timeout_future.await {
                Ok(Ok((packet_length, packet_addr))) => {
                    Datagram {
                        addr: packet_addr,
                        data: Bytes::from(&buffer[0..packet_length]),
                    }
                },
                Ok(Err(err)) => {
                    return Err(err);
                },
                Err(_) => {
                    continue;
                }
            };

            return Ok(Rc::new(next_datagram));
        }

        // If we timed out while trying to receive a response, terminate the connection.
        self.send_error(0, b"timed out").await?;

        Err(io::Error::new(io::ErrorKind::TimedOut, "timed out"))
    }
}
