use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use ascii::AsciiStr;
use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::timer::Timeout;

use crate::proto::{TftpPacket, ERR_ILLEGAL_OPERATION};
use std::ops::Add;

#[derive(Debug)]
pub(crate) struct OwnedTftpPacket {
    pub addr: SocketAddr,
    pub bytes: Bytes,
}

impl OwnedTftpPacket {
    pub fn packet(&self) -> TftpPacket {
        TftpPacket::from_bytes(&self.bytes).unwrap()
    }
}

// Possible return values from check_packet closure supplied to send_receive_next.
pub(crate) enum PacketCheckResult {
    Accept, // Accept the packet and return it from send_and_receive_next.
    Ignore, // Ignore the packet and continue receive loop.
    Reject, // Reject the packet and end the connection.
}

pub(crate) struct TftpConnState {
    socket: UdpSocket,
    main_remote_addr: Option<SocketAddr>,
    remote_addr: Option<SocketAddr>,
    trace_packets: bool,
    max_retries: u16,
}

impl TftpConnState {
    pub fn new(
        socket: UdpSocket,
        remote_addr_opt: Option<SocketAddr>,
        main_remote_addr_opt: Option<SocketAddr>,
    ) -> TftpConnState {
        TftpConnState {
            socket: socket,
            remote_addr: remote_addr_opt,
            main_remote_addr: main_remote_addr_opt,
            trace_packets: true,
            max_retries: 5,
        }
    }

    fn remote_addr(&self) -> io::Result<SocketAddr> {
        match self.remote_addr.or(self.main_remote_addr) {
            Some(addr) => Ok(addr),
            None => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "No address set in state",
            )),
        }
    }

    pub async fn send(&mut self, bytes_to_send: &Bytes) -> io::Result<()> {
        let remote_addr = self.remote_addr()?;
        self.socket.send_to(bytes_to_send, &remote_addr).await?;
        Ok(())
    }

    pub async fn send_error(&mut self, code: u16, message: &'static [u8]) -> io::Result<()> {
        let remote_addr = self.remote_addr()?;

        let bytes_to_send = {
            let packet = TftpPacket::Error {
                code: code,
                message: message,
            };
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        };

        self.socket.send_to(&bytes_to_send, &remote_addr).await?;

        Ok(())
    }

    pub async fn send_and_receive_next<F>(
        &mut self,
        bytes_to_send: Bytes,
        check_packet: F,
    ) -> io::Result<OwnedTftpPacket>
    where
        F: Fn(&TftpPacket) -> PacketCheckResult,
    {
        let mut buffer: Vec<u8> = vec![0; 65535];

        if self.trace_packets {
            let packet = TftpPacket::from_bytes(&bytes_to_send).unwrap();
            println!("Sending {}", &packet);
        }

        let mut tries = 0;

        'send_loop: while tries < self.max_retries {
            tries += 1;

            // Determine the remote address to use.
            let remote_addr = match self.remote_addr.or(self.main_remote_addr) {
                Some(addr) => addr,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        "No address set in state",
                    ));
                }
            };

            // Send the packet to the remote.
            self.socket.send_to(&bytes_to_send, &remote_addr).await?;

            // Compute the deadline for receiving a response to this packet. This is used to recompute
            // the timeout future on each loop iteration.
            let deadline = Instant::now().add(Duration::new(2, 0));

            // Now wait for the remote to send us a response (with a timeout).
            'receive_loop: loop {
                let recv_fut = self.socket.recv_from(&mut buffer);
                let now = Instant::now();
                let timeout_duration = deadline.saturating_duration_since(now);
                let timeout_fut = Timeout::new(recv_fut, timeout_duration);

                match timeout_fut.await {
                    Ok(Ok((recv_len, recv_addr))) => {
                        // Case: Received a packet.

                        // Update the remote address state as necessary.
                        match self.remote_addr {
                            Some(addr) => {
                                if addr != recv_addr {
                                    // RFC 1350: "If a source TID does not match, the packet should be discarded as erroneously sent from
                                    // somewhere else.  An error packet should be sent to the source of the incorrect packet, while not
                                    // disturbing the transfer."
                                    // TODO: Send error packet.
                                    continue 'receive_loop;
                                }
                            }
                            None => {
                                // Record the peer's address as the expected address if and only if there is a separate
                                // "main" remote address in use and this packet's address differs from that main address.
                                if let Some(addr) = self.main_remote_addr {
                                    if addr != recv_addr {
                                        self.main_remote_addr = Some(recv_addr);
                                    }
                                }
                            }
                        }

                        // Parse the packet. Ignore malformed packets by just receiving again.
                        let recv_bytes = Bytes::from(&buffer[0..recv_len]);
                        let recv_packet = match TftpPacket::from_bytes(&recv_bytes) {
                            Ok(packet) => {
                                if self.trace_packets {
                                    println!("Received {} from {}", &packet, &recv_addr);
                                }
                                match packet {
                                    TftpPacket::Error { code, message } => {
                                        let message_as_str = AsciiStr::from_ascii(&message)
                                            .map(|s| s.as_str())
                                            .unwrap_or("Malformed message");
                                        return Err(io::Error::new(
                                            io::ErrorKind::Other,
                                            format!("TFTP Error: {} ({})", code, message_as_str),
                                        ));
                                    }
                                    packet => packet,
                                }
                            }
                            Err(_) => {
                                // TODO: Send error to source for malformed packets. Ignore for now.
                                continue 'receive_loop;
                            }
                        };

                        // Ask the supplied closure whether this packet is expected.
                        // If it isn't, then just continue waiting.
                        match check_packet(&recv_packet) {
                            PacketCheckResult::Accept => {
                                let owned_packet = OwnedTftpPacket {
                                    addr: recv_addr,
                                    bytes: recv_bytes,
                                };

                                return Ok(owned_packet);
                            }
                            PacketCheckResult::Ignore => continue 'receive_loop,
                            PacketCheckResult::Reject => {
                                let _ = self
                                    .send_error(ERR_ILLEGAL_OPERATION, b"Illegal operation")
                                    .await;
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "Unexpected packet",
                                ));
                            }
                        };
                    }
                    Ok(Err(err)) => {
                        return Err(err);
                    }
                    Err(_) => {
                        // We timed out without receiving a response. Resend the next packet.
                        continue 'send_loop;
                    }
                };
            }
        }

        // If we timed out while trying to receive a response, terminate the connection.
        let _ = self.send_error(0, b"timed out").await;
        Err(io::Error::new(io::ErrorKind::TimedOut, "timed out"))
    }
}
