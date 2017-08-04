use std::io::{self, Cursor, Error, ErrorKind};

use bytes::{BigEndian, Buf, BufMut, IntoBuf, BytesMut};

pub enum TftpPacket<'req> {
    ReadRequest {
        filename: &'req [u8],
        mode: &'req [u8],
    },
    WriteRequest {
        filename: &'req [u8],
        mode: &'req [u8],
    },
    Data {
        block: u16,
        data: &'req [u8],
    },
    Ack(u16),
    Error {
        code: u16,
        message: &'req [u8],
    }
}

impl<'req> TftpPacket<'req> {
    pub fn from_bytes(bytes: &'req [u8]) -> io::Result<TftpPacket<'req>> {
        let mut buf = bytes.into_buf();
        if buf.remaining() < 2 {
            return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet: No type code."))
        }

        let code = buf.get_u16::<BigEndian>();
        match code {
            1|2 => {
                let strs = &bytes[2..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if strs.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"))
                }
                if code == 1 {
                    Ok(TftpPacket::ReadRequest { filename: strs[0], mode: strs[1] })
                } else {
                    Ok(TftpPacket::WriteRequest { filename: strs[0], mode: strs[1] })
                }
            },
            3 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"))
                }
                let block = buf.get_u16::<BigEndian>();
                let data = &bytes[4..];
                Ok(TftpPacket::Data { block: block, data: data })
            },
            4 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"))
                }
                Ok(TftpPacket::Ack(buf.get_u16::<BigEndian>()))
            },
            5 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"))
                }
                let code = buf.get_u16::<BigEndian>();
                let strs = &bytes[4..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if strs.len() < 1 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"))
                }
                let message = strs[0];
                Ok(TftpPacket::Error { code: code, message: message })
            },
            x => {
                Err(Error::new(ErrorKind::InvalidInput, format!("Unknown packet type: {}", x)))
            },
        }
    }

    pub fn encode(&self, out: &mut BytesMut) {
        match *self {
            TftpPacket::ReadRequest { filename, mode } => {
                out.put_u16::<BigEndian>(1);
                out.extend_from_slice(filename);
                out.put_u8(0);
                out.extend_from_slice(mode);
                out.put_u8(0);
            },
            TftpPacket::WriteRequest { filename, mode } => {
                out.put_u16::<BigEndian>(2);
                out.extend_from_slice(filename);
                out.put_u8(0);
                out.extend_from_slice(mode);
                out.put_u8(0);
            },
            TftpPacket::Data { block, data } => {
                out.put_u16::<BigEndian>(3);
                out.put_u16::<BigEndian>(block);
                out.extend_from_slice(data);

            }
            TftpPacket::Ack(block) => {
                out.put_u16::<BigEndian>(4);
                out.put_u16::<BigEndian>(block);
            },
            TftpPacket::Error { code, message } => {
                out.put_u16::<BigEndian>(5);
                out.put_u16::<BigEndian>(code);
                out.extend_from_slice(message);
                out.put_u8(0);
            },
        }
    }
}
