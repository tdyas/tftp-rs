use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::UdpSocket;

use super::util::parse_number;
use crate::config::TftpConfig;
use crate::conn::{PacketCheckResult, TftpConnState};
use crate::proto::{
    TftpPacket, DEFAULT_BLOCK_SIZE, ERR_ILLEGAL_OPERATION, ERR_INVALID_OPTIONS, ERR_NOT_DEFINED,
    MAX_BLOCK_SIZE, MIN_BLOCK_SIZE,
};

pub async fn tftp_read<W: AsyncWrite + Unpin>(
    address: &SocketAddr,
    filename: &[u8],
    mode: &[u8],
    config: &TftpConfig,
    writer: &mut W,
) -> io::Result<()> {
    // Bind a random but specific local socket for this request.
    let socket_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let socket = UdpSocket::bind(&socket_addr).await.unwrap();

    let mut conn_state = TftpConnState::new(socket, None, Some(address.clone()));

    let mut enable_transfer_size_option;
    let mut block_size: u16 = config.max_block_size;

    let mut options: HashMap<&[u8], &[u8]> = HashMap::new();
    let blksize_value = format!("{}", &block_size);
    if config.enable_tsize_option {
        options.insert(b"tsize", b"0");
    }
    if config.enable_blksize_option {
        options.insert(b"blksize", blksize_value.as_bytes());
    }

    let read_request_bytes = {
        let packet = TftpPacket::ReadRequest {
            filename: filename,
            mode: mode,
            options: options,
        };
        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
        packet.encode(&mut buffer);
        buffer.freeze()
    };

    let mut current_block_num: u16 = 1;
    let mut actual_transfer_size: usize = 0;
    let mut expected_transfer_size: usize = 0;

    let mut option_err: Option<(u16, &'static [u8])> = None;

    let response = conn_state
        .send_and_receive_next(read_request_bytes, |packet| match packet {
            TftpPacket::Data { .. } => PacketCheckResult::Accept,
            TftpPacket::OptionsAck(_) => PacketCheckResult::Accept,
            _ => PacketCheckResult::Reject,
        })
        .await?;
    match response.packet() {
        TftpPacket::Data { block, data } => {
            block_size = DEFAULT_BLOCK_SIZE;
            enable_transfer_size_option = false;
            if block != current_block_num {
                let _ = conn_state
                    .send_error(ERR_ILLEGAL_OPERATION, b"Illegal operation.")
                    .await?;
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }
            if let Err(err) = writer.write_all(data).await {
                let _ = conn_state
                    .send_error(ERR_NOT_DEFINED, b"Write error")
                    .await?;
                return Err(err);
            }
            actual_transfer_size += data.len();
        }
        TftpPacket::OptionsAck(ref options) => {
            // Reset the block number so that the data receive loop will send the correct acknowledgement.
            current_block_num = 0;

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

            enable_transfer_size_option = false;
            if options.contains_key(b"tsize" as &[u8]) {
                if let Some(transfer_size) = parse_number::<usize>(options, b"tsize") {
                    enable_transfer_size_option = true;
                    expected_transfer_size = transfer_size;
                } else {
                    option_err = Some((ERR_INVALID_OPTIONS, b"Invalid tsize option"));
                }
            }
        }
        _ => unreachable!(),
    };

    if let Some((code, message)) = option_err {
        let _ = conn_state.send_error(code, message).await;
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }

    'data_loop: loop {
        // Send the acknowledgement for the data packet or OACK.
        let ack_bytes_to_send = {
            let packet = TftpPacket::Ack(current_block_num);
            let mut buffer = BytesMut::with_capacity(packet.encoded_size());
            packet.encode(&mut buffer);
            buffer.freeze()
        };
        let response = conn_state
            .send_and_receive_next(ack_bytes_to_send, |packet| match packet {
                TftpPacket::Data { block, .. } => {
                    if *block <= current_block_num {
                        PacketCheckResult::Ignore
                    } else if *block == current_block_num + 1 {
                        PacketCheckResult::Accept
                    } else {
                        PacketCheckResult::Reject
                    }
                }
                _ => PacketCheckResult::Reject,
            })
            .await?;
        match response.packet() {
            TftpPacket::Data { block, data } => {
                if block != current_block_num + 1 {
                    let _ = conn_state
                        .send_error(ERR_ILLEGAL_OPERATION, b"Illegal operation.")
                        .await?;
                    return Err(io::Error::from(io::ErrorKind::InvalidInput));
                }
                current_block_num += 1;

                if let Err(err) = writer.write_all(data).await {
                    let _ = conn_state
                        .send_error(ERR_NOT_DEFINED, b"Write error")
                        .await?;
                    return Err(err);
                }
                actual_transfer_size += data.len();

                if (data.len() as u16) < block_size {
                    let ack_bytes_to_send = {
                        let packet = TftpPacket::Ack(current_block_num);
                        let mut buffer = BytesMut::with_capacity(packet.encoded_size());
                        packet.encode(&mut buffer);
                        buffer.freeze()
                    };
                    conn_state.send(&ack_bytes_to_send).await?;
                    break 'data_loop;
                }
            }
            _ => unreachable!(),
        }
    }

    if enable_transfer_size_option && actual_transfer_size != expected_transfer_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transfer size mismatch",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;

    use bytes::{BufMut, BytesMut};

    use super::tftp_read;
    use crate::config::TftpConfig;
    use crate::testing::*;

    #[tokio::test]
    async fn test_read() {
        let data = {
            let mut b = BytesMut::with_capacity(2048);
            for v in 0..b.capacity() {
                b.put_u8((v & 0xFF) as u8);
            }
            b.freeze()
        };

        use crate::proto::TftpPacket::*;
        use Op::*;

        let mut config = TftpConfig::default();
        config.enable_blksize_option = false;
        config.enable_tsize_option = false;

        run_client_test(
            "file not found",
            &config,
            |server_addr, config| {
                async move {
                    let mut bytes: Vec<u8> = Vec::new();
                    let result =
                        tftp_read(&server_addr, b"missing", b"octet", &config, &mut bytes).await;
                    let error = result.expect_err("error expected");
                    assert!(error.description().contains("File not found"));
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"missing",
                    mode: b"octet",
                    options: HashMap::new(),
                })),
                Send(mk(Error {
                    code: 1,
                    message: b"File not found",
                })),
            ],
        )
        .await;

        let expected_bytes = data.slice(0..768);
        run_client_test(
            "simple read",
            &config,
            |server_addr, config| {
                async move {
                    let mut actual_bytes: Vec<u8> = Vec::new();
                    tftp_read(&server_addr, b"xyzzy", b"octet", &config, &mut actual_bytes)
                        .await
                        .unwrap();
                    assert_eq!(&expected_bytes, &actual_bytes);
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: HashMap::new(),
                })),
                Send(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: &data[512..768],
                })),
                Receive(mk(Ack(2))),
            ],
        )
        .await;

        let expected_bytes = data.slice(0..1024);
        run_client_test(
            "read with block-aligned file size",
            &config,
            |server_addr, config| {
                async move {
                    let mut actual_bytes: Vec<u8> = Vec::new();
                    tftp_read(&server_addr, b"xyzzy", b"octet", &config, &mut actual_bytes)
                        .await
                        .unwrap();
                    assert_eq!(&expected_bytes, &actual_bytes);
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: HashMap::new(),
                })),
                Send(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: &data[512..1024],
                })),
                Receive(mk(Ack(2))),
                Send(mk(Data {
                    block: 3,
                    data: &[],
                })),
                Receive(mk(Ack(3))),
            ],
        )
        .await;

        // read with tsize option
        let expected_bytes = data.slice(0..768);
        let mut send_options: HashMap<&[u8], &[u8]> = HashMap::new();
        send_options.insert(b"tsize", b"0");
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"tsize", b"768");
        config.enable_tsize_option = true;
        config.enable_blksize_option = false;
        run_client_test(
            "read with tsize option",
            &config,
            |server_addr, config| {
                async move {
                    let mut actual_bytes: Vec<u8> = Vec::new();
                    tftp_read(&server_addr, b"xyzzy", b"octet", &config, &mut actual_bytes)
                        .await
                        .unwrap();
                    assert_eq!(&expected_bytes, &actual_bytes);
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: send_options,
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: &data[0..512],
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: &data[512..768],
                })),
                Receive(mk(Ack(2))),
            ],
        )
        .await;

        // read with blksize option - non-block-size number of bytes
        let expected_bytes = data.slice(0..1024);
        let mut send_options: HashMap<&[u8], &[u8]> = HashMap::new();
        send_options.insert(b"blksize", b"768");
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"blksize", b"768");
        config.enable_tsize_option = false;
        config.enable_blksize_option = true;
        config.max_block_size = 768;
        run_client_test(
            "read with blksize option",
            &config,
            |server_addr, config| {
                async move {
                    let mut actual_bytes: Vec<u8> = Vec::new();
                    tftp_read(&server_addr, b"xyzzy", b"octet", &config, &mut actual_bytes)
                        .await
                        .unwrap();
                    assert_eq!(&expected_bytes, &actual_bytes);
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: send_options,
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: &data[0..768],
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: &data[768..1024],
                })),
                Receive(mk(Ack(2))),
            ],
        )
        .await;

        // read with blksize option - server modified blksize option
        let expected_bytes = data.slice(0..1024);
        let mut send_options: HashMap<&[u8], &[u8]> = HashMap::new();
        send_options.insert(b"blksize", b"768");
        let mut recv_options: HashMap<&[u8], &[u8]> = HashMap::new();
        recv_options.insert(b"blksize", b"384");
        config.enable_tsize_option = false;
        config.enable_blksize_option = true;
        config.max_block_size = 768;
        run_client_test(
            "read with blksize option modified by server",
            &config,
            |server_addr, config| {
                async move {
                    let mut actual_bytes: Vec<u8> = Vec::new();
                    tftp_read(&server_addr, b"xyzzy", b"octet", &config, &mut actual_bytes)
                        .await
                        .unwrap();
                    assert_eq!(&expected_bytes, &actual_bytes);
                    Ok(())
                }
            },
            vec![
                Receive(mk(ReadRequest {
                    filename: b"xyzzy",
                    mode: b"octet",
                    options: send_options,
                })),
                Send(mk(OptionsAck(recv_options))),
                Receive(mk(Ack(0))),
                Send(mk(Data {
                    block: 1,
                    data: &data[0..384],
                })),
                Receive(mk(Ack(1))),
                Send(mk(Data {
                    block: 2,
                    data: &data[384..768],
                })),
                Receive(mk(Ack(2))),
                Send(mk(Data {
                    block: 3,
                    data: &data[768..1024],
                })),
                Receive(mk(Ack(3))),
            ],
        )
        .await;
    }
}
