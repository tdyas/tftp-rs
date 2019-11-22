use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;

use ascii::AsAsciiStr;
use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;

use crate::config::TftpConfig;
use crate::conn::{PacketCheckResult, TftpConnState};
use crate::proto::{
    TftpPacket, DEFAULT_BLOCK_SIZE, ERR_ILLEGAL_OPERATION, ERR_INVALID_OPTIONS, MAX_BLOCK_SIZE,
    MIN_BLOCK_SIZE,
};

fn parse_number<N: FromStr>(m: &HashMap<&[u8], &[u8]>, key: &[u8]) -> ::std::option::Option<N> {
    m.get(&key).and_then(move |x| match (*x).as_ascii_str() {
        Ok(y) => match (*y).as_str().parse::<N>() {
            Ok(v) => Some(v),
            Err(_) => None,
        },
        Err(_) => None,
    })
}

pub async fn tftp_write(
    address: &SocketAddr,
    filename: &[u8],
    mode: &[u8],
    config: &TftpConfig,
    file_bytes: Bytes,
) -> io::Result<()> {
    // Bind a random but specific local socket for this request.
    let socket_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let socket = UdpSocket::bind(&socket_addr).await.unwrap();

    let mut conn_state = TftpConnState::new(socket, None, Some(address.clone()));

    let mut block_size: u16 = config.max_block_size;

    let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
    let blksize_value = format!("{}", &block_size);
    let tsize_value = format!("{}", file_bytes.len());
    if config.enable_tsize_option {
        options.insert(b"tsize", tsize_value.as_bytes());
    }
    if config.enable_blksize_option {
        options.insert(b"blksize", blksize_value.as_bytes());
    }

    let write_request_bytes = {
        let packet = TftpPacket::WriteRequest {
            filename: filename,
            mode: mode,
            options: options,
        };
        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
        packet.encode(&mut buffer);
        buffer.freeze()
    };

    let mut option_err: Option<(u16, &'static [u8])> = None;

    let response = conn_state
        .send_and_receive_next(write_request_bytes, |packet| match packet {
            TftpPacket::Ack(_) => PacketCheckResult::Accept,
            TftpPacket::OptionsAck(_) => PacketCheckResult::Accept,
            _ => PacketCheckResult::Reject,
        })
        .await?;
    match response.packet() {
        TftpPacket::Ack(block) => {
            block_size = DEFAULT_BLOCK_SIZE;
            if block != 0 {
                println!("TFTP server sent unexpected block number.");
                let _ = conn_state
                    .send_error(ERR_ILLEGAL_OPERATION, b"Illegal operation.")
                    .await?;
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }
        }
        TftpPacket::OptionsAck(ref options) => {
            block_size = DEFAULT_BLOCK_SIZE;
            if options.contains_key(b"blksize" as &[u8]) {
                if let Some(accepted_block_size) = parse_number::<u16>(options, b"blksize") {
                    if accepted_block_size < MIN_BLOCK_SIZE || accepted_block_size > MAX_BLOCK_SIZE
                    {
                        option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                    } else {
                        block_size = accepted_block_size as u16;
                    }
                } else {
                    option_err = Some((ERR_INVALID_OPTIONS, b"Invalid blksize option"));
                }
            }
        }
        _ => unreachable!(),
    };

    if let Some((code, message)) = option_err {
        let _ = conn_state.send_error(code, message).await;
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    let mut current_block_num: u16 = 1;
    let mut current_offset: usize = 0;

    loop {
        // Send the next DATA packet.
        let data_len = (file_bytes.len() - current_offset).min(block_size as usize);
        let last_packet = data_len < block_size as usize;
        let bytes_to_send = {
            let packet = TftpPacket::Data {
                block: current_block_num,
                data: &file_bytes[current_offset..current_offset + data_len],
            };
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        };
        let _ = conn_state
            .send_and_receive_next(bytes_to_send, |packet| match packet {
                TftpPacket::Ack(block) => {
                    if *block < current_block_num {
                        PacketCheckResult::Ignore
                    } else if *block == current_block_num {
                        PacketCheckResult::Accept
                    } else {
                        PacketCheckResult::Reject
                    }
                }
                _ => PacketCheckResult::Reject,
            })
            .await?;

        current_offset += data_len;
        current_block_num += 1;
        if last_packet {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::{BufMut, BytesMut};

    use super::tftp_write;
    use crate::config::TftpConfig;
    use crate::testing::*;

    #[tokio::test]
    async fn test_write() {
        let data = {
            let mut b = BytesMut::with_capacity(2048);
            for v in 0..b.capacity() {
                b.put((v & 0xFF) as u8);
            }
            b.freeze()
        };

        use crate::proto::TftpPacket::*;
        use Op::*;

        let mut config = TftpConfig::default();
        config.enable_blksize_option = false;
        config.enable_tsize_option = false;

        let expected_bytes = data.slice(0, 768);
        run_client_test(
            "simple write",
            &config,
            |server_addr, config| {
                async move {
                    let result =
                        tftp_write(&server_addr, b"xyzzy", b"octet", &config, expected_bytes).await;
                    assert!(result.is_ok());
                    Ok(())
                }
            },
            vec![
                Receive(mk(WriteRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: HashMap::new(),
                })),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data[512..768],
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        let expected_bytes = data.slice(0, 1024);
        run_client_test(
            "write with block-aligned file size",
            &config,
            |server_addr, config| {
                async move {
                    let result =
                        tftp_write(&server_addr, b"xyzzy", b"octet", &config, expected_bytes).await;
                    assert!(result.is_ok());
                    Ok(())
                }
            },
            vec![
                Receive(mk(WriteRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: HashMap::new(),
                })),
                Send(mk(Ack(0))),
                Receive(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data[512..1024],
                })),
                Send(mk(Ack(2))),
                Receive(mk(Data {
                    block: 3,
                    data: &[],
                })),
                Send(mk(Ack(3))),
            ],
        )
        .await;

        // read with tsize option
        let expected_bytes = data.slice(0, 768);
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"tsize", b"768");
        config.enable_tsize_option = true;
        config.enable_blksize_option = false;
        run_client_test(
            "write with tsize option",
            &config,
            |server_addr, config| {
                async move {
                    let result =
                        tftp_write(&server_addr, b"xyzzy", b"octet", &config, expected_bytes).await;
                    assert!(result.is_ok());
                    Ok(())
                }
            },
            vec![
                Receive(mk(WriteRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: recv_options.clone(),
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data[512..768],
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        // write with blksize option - non-block-size number of bytes
        let expected_bytes = data.slice(0, 1024);
        let mut send_options: HashMap<&[u8], &[u8]> = HashMap::new();
        send_options.insert(b"blksize", b"768");
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"blksize", b"768");
        config.enable_tsize_option = false;
        config.enable_blksize_option = true;
        config.max_block_size = 768;
        run_client_test(
            "write with blksize option",
            &config,
            |server_addr, config| {
                async move {
                    let result =
                        tftp_write(&server_addr, b"xyzzy", b"octet", &config, expected_bytes).await;
                    assert!(result.is_ok());
                    Ok(())
                }
            },
            vec![
                Receive(mk(WriteRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: send_options,
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Data {
                    block: 1,
                    data: &data[0..768],
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data[768..1024],
                })),
                Send(mk(Ack(2))),
            ],
        )
        .await;

        // write with blksize option - server modified blksize option
        let expected_bytes = data.slice(0, 1024);
        let mut send_options: HashMap<&[u8], &[u8]> = HashMap::new();
        send_options.insert(b"blksize", b"768");
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"blksize", b"384");
        config.enable_tsize_option = false;
        config.enable_blksize_option = true;
        config.max_block_size = 768;
        run_client_test(
            "write with blksize option modified by server",
            &config,
            |server_addr, config| {
                async move {
                    let result = tftp_write(&server_addr, b"xyzzy", b"octet", &config, expected_bytes).await;
                    assert!(result.is_ok());
                    Ok(())
                }
            },
            vec![
                Receive(mk(WriteRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: send_options,
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Data {
                    block: 1,
                    data: &data[0..384],
                })),
                Send(mk(Ack(1))),
                Receive(mk(Data {
                    block: 2,
                    data: &data[384..768],
                })),
                Send(mk(Ack(2))),
                Receive(mk(Data {
                    block: 3,
                    data: &data[768..1024],
                })),
                Send(mk(Ack(3))),
            ],
        )
        .await;
    }
}
