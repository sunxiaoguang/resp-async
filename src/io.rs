use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use futures::{future, Future, Sink, Stream};
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio_codec::{Decoder, Encoder};

use crate::error::*;
use crate::resp::*;

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

macro_rules! either {
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

pub trait EventHandler {
    type ClientUserData: Default + Send + Sync;

    fn on_request(
        &self,
        peer: &mut PeerContext<Self::ClientUserData>,
        request: Value,
    ) -> Box<dyn Future<Item = Value, Error = Error> + Send + Sync>;
    fn on_connect(&self, _id: u64) -> Result<Self::ClientUserData> {
        Ok(Self::ClientUserData::default())
    }
    fn on_disconnect(&self, _id: u64) {}
}

pub struct Server<H>
where
    H: EventHandler + Send + Sync + 'static,
{
    handler: Arc<H>,
    addresses: Vec<TcpListener>,
    client_id: Arc<AtomicU64>,
}

impl<H> Server<H>
where
    H: EventHandler + Send + Sync + 'static,
{
    pub fn new(handler: H) -> Self {
        Server {
            handler: Arc::new(handler),
            addresses: Vec::with_capacity(1),
            client_id: Arc::new(AtomicU64::default()),
        }
    }

    pub fn listen(&mut self, addr: &SocketAddr) -> Result<&mut Self> {
        self.addresses.push(TcpListener::bind(addr)?);
        Ok(self)
    }

    fn try_process(
        id: u64,
        handler: Arc<H>,
        socket: TcpStream,
    ) -> impl Future<Item = (), Error = Error> {
        let user_data = either!(handler.on_connect(id));
        let mut client = PeerContext {
            peer: either!(socket.peer_addr()),
            local: either!(socket.local_addr()),
            ctx: HashMap::new(),
            user_data,
        };
        let (tx, rx) = ValueCodec::default().framed(socket).split();
        let disconnect_handler = Arc::clone(&handler);
        or!(tx
            .send_all(rx.and_then(move |v| handler.on_request(&mut client, v)))
            .then(move |r| {
                disconnect_handler.on_disconnect(id);
                r.map(|_| ())
            }))
    }

    fn process(id: u64, handler: Arc<H>, socket: TcpStream) -> impl Future<Item = (), Error = ()> {
        Self::try_process(id, handler, socket)
            .map_err(move |e| warn!("Failed to handle connection: {}", e))
    }

    pub fn serve(&mut self) -> Result<()> {
        let addresses: Vec<TcpListener> = self.addresses.drain(..).collect();
        let handler = Arc::clone(&self.handler);
        let client_id = Arc::clone(&self.client_id);
        tokio::run(
            future::join_all(addresses.into_iter().map(move |l| {
                let handler = Arc::clone(&handler);
                let client_id = Arc::clone(&client_id);
                l.incoming()
                    .map_err(|e| warn!("Failed to accept connection: {}", e))
                    .for_each(move |socket| {
                        tokio::spawn(Self::process(
                            client_id.fetch_add(1, Ordering::AcqRel),
                            Arc::clone(&handler),
                            socket,
                        ))
                    })
            }))
            .map(|_| ()),
        );
        Ok(())
    }
}
