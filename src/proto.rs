#![allow(unused)]

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Error, ErrorKind};

use ascii::AsciiStr;
use bytes::{Buf, BufMut, Bytes};

pub const DEFAULT_BLOCK_SIZE: u16 = 512;
pub const MIN_BLOCK_SIZE: u16 = 4;
pub const MAX_BLOCK_SIZE: u16 = 65464;

pub const ERR_NOT_DEFINED: u16 = 0; // Not defined, see error message (if any).
pub const ERR_FILE_NOT_FOUND: u16 = 1; // File not found.
pub const ERR_ACCESS_VIOLATION: u16 = 2; // Access violation.
pub const ERR_DISK_FULL: u16 = 3; // Disk full or allocation exceeded.
pub const ERR_ILLEGAL_OPERATION: u16 = 4; // Illegal TFTP operation.
pub const ERR_UNKNOWN_TRANSFER_ID: u16 = 5; // Unknown transfer ID.
pub const ERR_FILE_EXISTS: u16 = 6; // File already exists.
pub const ERR_NO_USER: u16 = 7; // No such user.
pub const ERR_INVALID_OPTIONS: u16 = 8; // Invalid options

#[derive(Debug)]
pub enum TftpPacket {
    ReadRequest {
        filename: Bytes,
        mode: Bytes,
        options: HashMap<Bytes, Bytes>,
    },
    WriteRequest {
        filename: Bytes,
        mode: Bytes,
        options: HashMap<Bytes, Bytes>,
    },
    Data {
        block: u16,
        data: Bytes,
    },
    Ack(u16),
    Error {
        code: u16,
        message: Bytes,
    },
    OptionsAck(HashMap<Bytes, Bytes>),
}

impl<'req> TftpPacket {
    pub fn from_bytes(bytes: &Bytes) -> io::Result<TftpPacket> {
        let mut buf = io::Cursor::new(bytes);

        if buf.remaining() < 2 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Malformed packet: No type code.",
            ));
        }

        let code = buf.get_u16();
        match code {
            1 | 2 => {
                let strs = bytes[2..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if strs.len() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                if (strs.len() % 2) == 0 || strs[strs.len() - 1].len() > 0 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }

                let mut options: HashMap<Bytes, Bytes> = HashMap::new();
                let mut i = 2;
                while i < strs.len() - 1 {
                    options.insert(bytes.slice_ref(strs[i]), bytes.slice_ref(strs[i + 1]));
                    i += 2;
                }

                if code == 1 {
                    Ok(TftpPacket::ReadRequest {
                        filename: bytes.slice_ref(strs[0]),
                        mode: bytes.slice_ref(strs[1]),
                        options: options,
                    })
                } else {
                    Ok(TftpPacket::WriteRequest {
                        filename: bytes.slice_ref(strs[0]),
                        mode: bytes.slice_ref(strs[1]),
                        options: options,
                    })
                }
            }
            3 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                let block = buf.get_u16();
                let data = bytes.slice(4..);
                Ok(TftpPacket::Data {
                    block: block,
                    data: data,
                })
            }
            4 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                Ok(TftpPacket::Ack(buf.get_u16()))
            }
            5 => {
                if buf.remaining() < 2 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                let code = buf.get_u16();
                let strs = bytes[4..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if strs.len() < 1 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }
                let message = bytes.slice_ref(strs[0]);
                Ok(TftpPacket::Error {
                    code: code,
                    message: message,
                })
            }
            6 => {
                let strs = bytes[2..].split(|&b| b == 0).collect::<Vec<&[u8]>>();
                if (strs.len() % 2) == 0 || strs[strs.len() - 1].len() > 0 {
                    return Err(Error::new(ErrorKind::InvalidInput, "Malformed packet"));
                }

                let mut options: HashMap<Bytes, Bytes> = HashMap::new();
                let mut i = 0;
                while i < strs.len() - 1 {
                    options.insert(bytes.slice_ref(strs[i]), bytes.slice_ref(strs[i + 1]));
                    i += 2;
                }

                Ok(TftpPacket::OptionsAck(options))
            }
            code => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Unknown packet type: {}", code),
            )),
        }
    }

    pub fn encode<B: BufMut>(&self, out: &mut B) {
        fn encode_options<B: BufMut, T: AsRef<[u8]>>(options: &HashMap<T, T>, out: &mut B) {
            let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = options
                .iter()
                .map(|(k, v)| ((*k).as_ref().to_vec(), (*v).as_ref().to_vec()))
                .collect();
            pairs.sort();
            for (key, value) in &pairs {
                out.put_slice(key.as_ref());
                out.put_u8(0);
                out.put_slice(value.as_ref());
                out.put_u8(0);
            }
        }

        match &self {
            TftpPacket::ReadRequest {
                filename,
                mode,
                ref options,
            } => {
                out.put_u16(1);
                out.put_slice(filename);
                out.put_u8(0);
                out.put_slice(mode);
                out.put_u8(0);
                encode_options(options, out);
            }
            TftpPacket::WriteRequest {
                filename,
                mode,
                ref options,
            } => {
                out.put_u16(2);
                out.put_slice(filename);
                out.put_u8(0);
                out.put_slice(mode);
                out.put_u8(0);
                encode_options(options, out);
            }
            TftpPacket::Data { block, data } => {
                out.put_u16(3);
                out.put_u16(*block);
                out.put_slice(data);
            }
            TftpPacket::Ack(block) => {
                out.put_u16(4);
                out.put_u16(*block);
            }
            TftpPacket::Error { code, message } => {
                out.put_u16(5);
                out.put_u16(*code);
                out.put_slice(message);
                out.put_u8(0);
            }
            TftpPacket::OptionsAck(ref options) => {
                out.put_u16(6);
                for (key, value) in options.iter() {
                    out.put_slice(key.as_ref());
                    out.put_u8(0);
                    out.put_slice(value.as_ref());
                    out.put_u8(0);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn encoded_size(&self) -> usize {
        match self {
            TftpPacket::ReadRequest {
                filename,
                mode,
                ref options,
            } => {
                let opts_len: usize = options.iter().map(|(k, v)| k.len() + 1 + v.len() + 1).sum();
                2 + filename.len() + 1 + mode.len() + 1 + opts_len
            }
            TftpPacket::WriteRequest {
                filename,
                mode,
                ref options,
            } => {
                let opts_len: usize = options.iter().map(|(k, v)| k.len() + 1 + v.len() + 1).sum();
                2 + filename.len() + 1 + mode.len() + 1 + opts_len
            }
            TftpPacket::Data { data, .. } => 2 + 2 + data.len(),
            TftpPacket::Ack(_) => 2 + 2,
            TftpPacket::Error { message, .. } => 2 + 2 + message.len() + 1,
            TftpPacket::OptionsAck(ref options) => {
                2 + options
                    .iter()
                    .map(|(k, v)| k.len() + 1 + v.len() + 1)
                    .sum::<usize>()
            }
        }
    }
}

impl<'a> fmt::Display for TftpPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::TftpPacket::*;
        match self {
            ReadRequest {
                ref filename,
                ref mode,
                ref options,
            } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                let options_as_str = options
                    .iter()
                    .map(|(key, value)| {
                        let key = AsciiStr::from_ascii(&key).unwrap();
                        let value = AsciiStr::from_ascii(&value).unwrap();
                        format!("{}={}", key, value)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "RRQ[file={}, mode={}, {}]",
                    filename, mode, options_as_str
                )
            }
            WriteRequest {
                ref filename,
                ref mode,
                ref options,
            } => {
                let filename = AsciiStr::from_ascii(filename).unwrap();
                let mode = AsciiStr::from_ascii(mode).unwrap();
                let options_as_str = options
                    .iter()
                    .map(|(key, value)| {
                        let key = AsciiStr::from_ascii(&key).unwrap();
                        let value = AsciiStr::from_ascii(&value).unwrap();
                        format!("{}={}", key, value)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "WRQ[file={}, mode={}, {}]",
                    filename, mode, options_as_str
                )
            }
            Data { block, data } => write!(f, "DATA[block={}, {} bytes]", block, data.len()),
            Ack(block) => write!(f, "ACK[block={}]", block),
            Error { code, ref message } => {
                let message = AsciiStr::from_ascii(message).unwrap();
                write!(f, "ERROR[code={}, msg='{}']", code, message)
            }
            OptionsAck(ref options) => {
                let options_as_str = options
                    .iter()
                    .map(|(key, value)| {
                        let key = AsciiStr::from_ascii(&key).unwrap();
                        let value = AsciiStr::from_ascii(&value).unwrap();
                        format!("{}={}", key, value)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "OACK[{}]", options_as_str)
            }
        }
    }
}
