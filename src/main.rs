#![allow(warnings)]
#[allow(unused_imports)]
use memtuco::page::*;
#[allow(unused_imports)]
use memtuco::btree::*;
#[allow(unused_imports)]
use memtuco::tokenizer::*;
#[allow(unused_imports)]
use memtuco::dbengine::*;
#[allow(unused_imports)]
use memtuco::transaction::*;
use std::env;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::sync::{Arc, Mutex};


fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <db_path> <port> [address]", args[0]);
        std::process::exit(1);
    }
    let db_path = &args[1];
    let port: u16 = args[2].parse().expect("Port must be a number");
    let address = if args.len() > 3 {
        args[3].clone()
    } else {
        "127.0.0.1".to_string()
    };

    TransactionEngine::start(db_path, port, &address);
}
