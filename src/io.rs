use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;

use bytes::BytesMut;
use futures::{Future, Sink, Stream};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_codec::{Decoder, Encoder};

use error::*;
use resp::*;

#[derive(Default)]
struct ValueCodec {
    decoder: Option<ValueDecoder>,
}

impl Decoder for ValueCodec {
    type Item = Value;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Value>> {
        if src.is_empty() {
            return Ok(None);
        }
        if self.decoder.is_none() {
            self.decoder = Some(ValueDecoder::new(src)?);
        }
        let mut decoder = mem::replace(&mut self.decoder, None);
        let value = decoder.as_mut().unwrap().try_decode(src)?;
        if value.is_none() {
            mem::replace(&mut self.decoder, decoder);
        }
        Ok(value)
    }
}

impl Encoder for ValueCodec {
    type Item = Value;
    type Error = Error;

    fn encode(&mut self, value: Value, buffer: &mut BytesMut) -> Result<()> {
        ValueEncoder::encode(buffer, &value);
        Ok(())
    }
}

macro_rules! try_result {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return ::futures::future::Either::A(::futures::future::err(err)),
        }
    };
    ($expr:expr,) => {
        $expr?
    };
}

macro_rules! or {
    ($expr:expr) => {
        ::futures::future::Either::B($expr)
    };
}

#[derive(Debug)]
pub struct PeerContext<T>
where
    T: Default,
{
    peer: SocketAddr,
    local: SocketAddr,
    ctx: HashMap<String, Value>,
    pub user_data: T,
}

impl<T> PeerContext<T>
where
    T: Default,
{
    pub fn set<K>(&mut self, key: K, value: Value) -> Option<Value>
    where
        K: Into<String>,
    {
        self.ctx.insert(key.into(), value)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.ctx.contains_key(key)
    }

    pub fn get(&mut self, key: &str) -> Option<&Value> {
        self.ctx.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.ctx.get_mut(key)
    }

    pub fn peer_addr(&self) -> &SocketAddr {
        &self.peer
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local
    }
}

fn process<F, U, T>(socket: TcpStream, f: F) -> impl Future<Item = (), Error = Error>
where
    F: Fn(&mut PeerContext<T>, Value) -> U,
    U: IntoFuture<Item = Value, Error = Error> + Send + Sync,
    T: Default,
{
    let mut client = PeerContext {
        peer: try_result!(socket.peer_addr()),
        local: try_result!(socket.local_addr()),
        ctx: HashMap::new(),
        user_data: T::default(),
    };
    let (tx, rx) = ValueCodec::default().framed(socket).split();
    // Map all requests into responses and send them back to the client.
    or!(tx
        .send_all(rx.and_then(move |v| f(&mut client, v)))
        .and_then(|_res| Ok(())))
}

pub fn listen_and_serve<F, U, T>(addr: &SocketAddr, f: F) -> impl Future<Item = (), Error = Error>
where
    F: Fn(&mut PeerContext<T>, Value) -> U + Clone,
    U: IntoFuture<Item = Value, Error = Error> + Send + Sync,
    T: Default,
{
    or!(try_result!(TcpListener::bind(&addr))
        .incoming()
        .for_each(move |socket| process(socket, f.clone())))
}
