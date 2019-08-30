#![feature(async_closure)]

mod client;
mod config;
mod conn;
mod proto;
mod server;

#[cfg(test)]
mod testing;

pub use client::tftp_get;
pub use server::TftpServer;
