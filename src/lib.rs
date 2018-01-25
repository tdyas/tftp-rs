#![feature(proc_macro, conservative_impl_trait, generators, arbitrary_self_types)]

extern crate ascii;
extern crate bytes;
#[macro_use]
extern crate futures_await as futures;
#[macro_use]
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

mod config;
mod proto;
mod server;

pub use server::TftpServer;
