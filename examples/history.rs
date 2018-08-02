#![feature(nll)]

extern crate futures;
extern crate resp_async;
extern crate tokio;
extern crate tokio_codec;

use std::net::SocketAddr;
use std::{env, io};

use futures::future::FutureResult;
use tokio::prelude::*;

use resp_async::*;

fn process(
    peer: &mut PeerContext<i32>,
    request: Value,
) -> impl IntoFuture<Future = FutureResult<Value, io::Error>, Item = Value, Error = io::Error> {
    let value = vec![peer.user_data.into(), request].into();
    if let Some(Value::Array(history)) = peer.get_mut("history") {
        history.push(value);
    } else {
        peer.set("history", vec![value].into());
    }
    peer.user_data += 1;
    Ok(Clone::clone(peer.get("history").unwrap()))
}

fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>().unwrap();
    tokio::run(listen_and_serve(&addr, process).map_err(|_| ()));
}
