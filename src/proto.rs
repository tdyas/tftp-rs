#![allow(unused)]

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Error, ErrorKind};

use ascii::AsciiStr;
use bytes::{BigEndian, Buf, BufMut, IntoBuf};

pub const DEFAULT_BLOCK_SIZE: u16 = 512;
pub const MIN_BLOCK_SIZE: u16 = 4;
pub const MAX_BLOCK_SIZE: u16 = 65464;

pub const ERR_NOT_DEFINED: u16         = 0; // Not defined, see error message (if any).
pub const ERR_FILE_NOT_FOUND: u16      = 1; // File not found.
pub const ERR_ACCESS_VIOLATION: u16    = 2; // Access violation.
pub const ERR_DISK_FULL: u16           = 3; // Disk full or allocation exceeded.
pub const ERR_ILLEGAL_OPERATION: u16   = 4; // Illegal TFTP operation.
pub const ERR_UNKNOWN_TRANSFER_ID: u16 = 5; // Unknown transfer ID.
pub const ERR_FILE_EXISTS: u16         = 6; // File already exists.
pub const ERR_NO_USER: u16             = 7; // No such user.
pub const ERR_INVALID_OPTIONS: u16     = 8; // Invalid options

#[derive(Debug)]
pub enum TftpPacket<'req> {
    ReadRequest {
        filename: &'req [u8],
        mode: &'req [u8],
        options: HashMap<&'req [u8], &'req [u8]>,
    },
    WriteRequest {
        filename: &'req [u8],
        mode: &'req [u8],
        options: HashMap<&'req [u8], &'req [u8]>,
    },
    Data {
        block: u16,
        data: &'req [u8],
    },
    Ack(u16),
    Error {
        code: u16,
        message: &'req [u8],
    },
    OptionsAck(HashMap<&'req [u8], &'req [u8]>),
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
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                if (strs.len() % 2) == 0 || strs[strs.len()-1].len() > 0 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }

                let mut options: HashMap<&'req [u8], &'req [u8]> = HashMap::new();
                let mut i = 2;
                while i < strs.len() - 1 {
                    options.insert(strs[i], strs[i + 1]);
                    i += 2;
                }

                if code == 1 {
                    Ok(TftpPacket::ReadRequest {
                        filename: strs[0],
                        mode: strs[1],
                        options: options,
                    })
                } else {
                    Ok(TftpPacket::WriteRequest {
                        filename: strs[0],
                        mode: strs[1],
                        options: options,
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
            6 => {
                let strs = &bytes[2..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if (strs.len() % 2) == 0 || strs[strs.len()-1].len() > 0 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }

                let mut options: HashMap<&'req [u8], &'req [u8]> = HashMap::new();
                let mut i = 0;
                while i < strs.len() - 1 {
                    options.insert(strs[i], strs[i + 1]);
                    i += 2;
                }

                Ok(TftpPacket::OptionsAck(options))
            },
            code => {
                Err(Error::new(ErrorKind::InvalidInput, format!("Unknown packet type: {}", code)))
            },
        }
    }

    pub fn encode<B: BufMut>(&self, out: &mut B) {
        match *self {
            TftpPacket::ReadRequest { filename, mode, ref options } => {
                out.put_u16::<BigEndian>(1);
                out.put(filename);
                out.put_u8(0);
                out.put(mode);
                out.put_u8(0);
                for (&key, &value) in options.iter() {
                    out.put(key);
                    out.put_u8(0);
                    out.put(value);
                    out.put_u8(0);
                }
            },
            TftpPacket::WriteRequest { filename, mode, ref options } => {
                out.put_u16::<BigEndian>(2);
                out.put(filename);
                out.put_u8(0);
                out.put(mode);
                out.put_u8(0);
                for (&key, &value) in options.iter() {
                    out.put(key);
                    out.put_u8(0);
                    out.put(value);
                    out.put_u8(0);
                }
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
            TftpPacket::OptionsAck(ref options) => {
                out.put_u16::<BigEndian>(6);
                for (&key, &value) in options.iter() {
                    out.put(key);
                    out.put_u8(0);
                    out.put(value);
                    out.put_u8(0);
                }
            },
        }
    }

    #[allow(dead_code)]
    pub fn encoded_size(&self) -> usize {
        match *self {
            TftpPacket::ReadRequest { filename, mode, ref options } => {
                let opts_len: usize = options.iter().map(|(&k, &v)| k.len() + 1 + v.len() + 1).sum();
                2 + filename.len() + 1 + mode.len() + 1 + opts_len
            },
            TftpPacket::WriteRequest { filename, mode, ref options } => {
                let opts_len: usize = options.iter().map(|(&k, &v)| k.len() + 1 + v.len() + 1).sum();
                2 + filename.len() + 1 + mode.len() + 1 + opts_len
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
            TftpPacket::OptionsAck(ref options) => {
                2 + options.iter().map(|(&k, &v)| k.len() + 1 + v.len() + 1).sum::<usize>()
            },
        }
    }
}

impl<'a> fmt::Display for TftpPacket<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::TftpPacket::*;
        match *self {
            ReadRequest { ref filename, ref mode, ref options } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                let options_as_str = options.iter().map(|(&key, &value)| {
                    let key = AsciiStr::from_ascii(key).unwrap();
                    let value = AsciiStr::from_ascii(value).unwrap();
                    format!("{}={}", key, value)
                }).collect::<Vec<_>>().join(", ");
                write!(f, "RRQ[file={}, mode={}, {}]", filename, mode, options_as_str)
            },
            WriteRequest { ref filename, ref mode, ref options } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                let options_as_str = options.iter().map(|(&key, &value)| {
                    let key = AsciiStr::from_ascii(key).unwrap();
                    let value = AsciiStr::from_ascii(value).unwrap();
                    format!("{}={}", key, value)
                }).collect::<Vec<_>>().join(", ");
                write!(f, "WRQ[file={}, mode={}, {}]", filename, mode, options_as_str)
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
            OptionsAck(ref options) => {
                let options_as_str = options.iter().map(|(&key, &value)| {
                    let key = AsciiStr::from_ascii(key).unwrap();
                    let value = AsciiStr::from_ascii(value).unwrap();
                    format!("{}={}", key, value)
                }).collect::<Vec<_>>().join(", ");
                write!(f, "OACK[{}]", options_as_str)
            },
        }
    }
}
