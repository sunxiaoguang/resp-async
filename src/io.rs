use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use log::{error, info};
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

use crate::error::*;
use crate::resp::*;

const DEFAULT_ADDRESS: &str = "127.0.0.1:6379";

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
    ) -> impl Future<Output = Result<Value>> + Send;
    fn on_connect(&self, _id: u64) -> impl Future<Output = Result<Self::ClientUserData>> + Send {
        async { Ok(Self::ClientUserData::default()) }
    }
    fn on_disconnect(&self, _id: u64) -> impl Future<Output = ()> + Send {
        async {}
    }
}

struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub(crate) async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }
        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }
}

pub struct Server<H>
where
    H: EventHandler + Send + Sync + 'static,
{
    handler: Arc<H>,
    address: String,
    client_id: Arc<AtomicU64>,
}

impl<H> Server<H>
where
    H: EventHandler + Send + Sync + 'static,
{
    pub fn new(handler: H) -> Self {
        Server {
            handler: Arc::new(handler),
            address: DEFAULT_ADDRESS.into(),
            client_id: Arc::new(AtomicU64::default()),
        }
    }

    pub fn listen(&mut self, addr: impl Into<String>) -> Result<&mut Self> {
        self.address = addr.into();
        Ok(self)
    }

    async fn run_client_loop(
        user_data: H::ClientUserData,
        handler: Arc<H>,
        mut socket: TcpStream,
    ) -> Result<()> {
        let mut client = PeerContext {
            peer: socket.peer_addr()?,
            local: socket.local_addr()?,
            ctx: HashMap::new(),
            user_data,
        };
        let mut rd = BytesMut::new();
        let mut wr = BytesMut::new();
        let mut decoder = ValueDecoder::default();
        loop {
            if let Some(value) = decoder.try_decode(&mut rd)? {
                handler
                    .on_request(&mut client, value)
                    .await?
                    .encode(&mut wr);
                socket.write_all(&wr).await?;
                wr.clear();
                socket.flush().await?;
            }

            if 0 == socket.read_buf(&mut rd).await? && rd.is_empty() {
                return Ok(());
            }
        }
    }

    async fn run_client_hooks(id: u64, handler: Arc<H>, socket: TcpStream) -> Result<()> {
        let user_data = handler.on_connect(id).await?;
        let result = Self::run_client_loop(user_data, Arc::clone(&handler), socket).await;
        handler.on_disconnect(id).await;
        result
    }

    async fn run_client(
        id: u64,
        handler: Arc<H>,
        socket: TcpStream,
        notify_shutdown: broadcast::Sender<()>,
        _shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<()> {
        let mut shutdown = Shutdown::new(notify_shutdown.subscribe());
        tokio::select! {
            res = Self::run_client_hooks(id, handler, socket) => { res }
            _ = shutdown.recv() => { Ok(()) }
        }
    }

    async fn run_accept_loop(
        &mut self,
        listener: TcpListener,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<()> {
        let mut shutdown = Shutdown::new(notify_shutdown.subscribe());
        while !shutdown.is_shutdown() {
            tokio::select! {
                res = listener.accept() => {
                    if let Ok((socket, _)) = res {
                        let handler = Arc::clone(&self.handler);
                        let client_id = Arc::clone(&self.client_id);
                        let notify_shutdown = Clone::clone(&notify_shutdown);
                        let shutdown_complete_tx = Clone::clone(&shutdown_complete_tx);
                        tokio::spawn(async move {
                            Self::run_client(client_id.fetch_add(1, Ordering::AcqRel), handler, socket, notify_shutdown, shutdown_complete_tx).await
                        });
                    }
                },
                _ = shutdown.recv() => {}
            }
        }
        Ok(())
    }

    pub async fn serve(&mut self, shutdown: impl Future) -> Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        tokio::select! {
            res = self.run_accept_loop(listener, Clone::clone(&notify_shutdown), Clone::clone(&shutdown_complete_tx)) => {
                if let Err(err) = res {
                    error!("Failed to accept {:?}", err);
                }
            }
            _ = shutdown => {
                info!("shutting down server");
            }
        }

        drop(notify_shutdown);
        drop(shutdown_complete_tx);

        let _ = shutdown_complete_rx.recv().await;
        Ok(())
    }
}
