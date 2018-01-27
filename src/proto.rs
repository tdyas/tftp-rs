use ascii::AsciiStr;
use std::fmt;
use std::io::{self, Error, ErrorKind};

use bytes::{BigEndian, Buf, BufMut, IntoBuf};

#[derive(Debug)]
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
                    Ok(TftpPacket::ReadRequest {
                        filename: strs[0],
                        mode: strs[1],
                    })
                } else {
                    Ok(TftpPacket::WriteRequest {
                        filename: strs[0],
                        mode: strs[1]
                    })
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

    pub fn encode<B: BufMut>(&self, out: &mut B) {
        match *self {
            TftpPacket::ReadRequest { filename, mode } => {
                out.put_u16::<BigEndian>(1);
                out.put(filename);
                out.put_u8(0);
                out.put(mode);
                out.put_u8(0);
            },
            TftpPacket::WriteRequest { filename, mode } => {
                out.put_u16::<BigEndian>(2);
                out.put(filename);
                out.put_u8(0);
                out.put(mode);
                out.put_u8(0);
            },
            TftpPacket::Data { block, data } => {
                out.put_u16::<BigEndian>(3);
                out.put_u16::<BigEndian>(block);
                out.put(data);

            }
            TftpPacket::Ack(block) => {
                out.put_u16::<BigEndian>(4);
                out.put_u16::<BigEndian>(block);
            },
            TftpPacket::Error { code, message } => {
                out.put_u16::<BigEndian>(5);
                out.put_u16::<BigEndian>(code);
                out.put(message);
                out.put_u8(0);
            },
        }
    }

    #[allow(dead_code)]
    pub fn encoded_size(&self) -> usize {
        match *self {
            TftpPacket::ReadRequest { filename, mode } => {
                2 + filename.len() + 1 + mode.len() + 1
            },
            TftpPacket::WriteRequest { filename, mode } => {
                2 + filename.len() + 1 + mode.len() + 1
            },
            TftpPacket::Data { data, .. } => {
                2 + 2 + data.len()
            }
            TftpPacket::Ack(_) => {
                2 + 2
            },
            TftpPacket::Error { message, ..} => {
                2 + 2 + message.len() + 1
            },
        }
    }
}

impl<'a> fmt::Display for TftpPacket<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::TftpPacket::*;
        match *self {
            ReadRequest { ref filename, ref mode } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                write!(f, "RRQ[file={}, mode={}]", filename, mode)
            },
            WriteRequest { ref filename, ref mode } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                write!(f, "WRQ[file={}, mode={}]", filename, mode)
            },
            Data { block, data } => {
                write!(f, "DATA[block={}, {} bytes]", block, data.len())
            },
            Ack(block) => {
                write!(f, "ACK[block={}]", block)
            }
            Error { code, ref message } => {
                let message = AsciiStr::from_ascii(message).unwrap();
                write!(f, "ERROR[code={}, msg='{}']", code, message)
            },
        }
    }
}
