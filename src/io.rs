use std::collections::VecDeque;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use log::error;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast, mpsc, oneshot};
use tokio::task::JoinSet;

use crate::context::{Command, Extensions, PubSubHandle, PushHandle, RequestContext};
use crate::error::Result;
use crate::resp::{DecodeLimits, Value, ValueDecoder};
use crate::response::{IntoResponse, RespError};
use crate::router::Router;

/// Per-connection metadata passed to hooks and extension factories.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionInfo {
    pub id: u64,
    pub peer_addr: SocketAddr,
    pub local_addr: SocketAddr,
}

/// Observability hooks for the server runtime.
pub trait ServerHooks: Send + Sync + 'static {
    fn on_accept_error(&self, _err: &std::io::Error) {}
    fn on_connection_open(&self, _info: ConnectionInfo) {}
    fn on_connection_close(&self, _info: ConnectionInfo) {}
    fn on_command(&self, _id: u64, _command: &Command) {}
    fn on_protocol_error(&self, _err: &RespError) {}
    fn on_io_error(&self, _err: &std::io::Error) {}
}

/// Default no-op hooks implementation.
#[derive(Debug, Default, Clone)]
pub struct NoopServerHooks;

impl ServerHooks for NoopServerHooks {}

struct ConnectionGuard {
    hooks: Arc<dyn ServerHooks>,
    info: ConnectionInfo,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.hooks.on_connection_close(self.info);
    }
}

type ExtensionsFactory = Arc<dyn Fn(ConnectionInfo) -> Extensions + Send + Sync + 'static>;

fn default_extensions(_info: ConnectionInfo) -> Extensions {
    Extensions::default()
}

/// Server configuration for limits and runtime behavior.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub max_frame_size: usize,
    pub max_bulk_len: usize,
    pub max_array_len: usize,
    pub max_depth: usize,
    pub max_inflight_requests: usize,
    pub max_connections: usize,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub push_queue_len: usize,
    pub response_queue_len: usize,
    pub write_batch_bytes: usize,
    pub tcp_nodelay: bool,
    pub backlog: Option<u32>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 1 << 20,
            max_bulk_len: 1 << 20,
            max_array_len: 1024,
            max_depth: 16,
            max_inflight_requests: 128,
            max_connections: 1024,
            read_timeout: None,
            write_timeout: None,
            idle_timeout: None,
            push_queue_len: 1024,
            response_queue_len: 1024,
            write_batch_bytes: 8 * 1024,
            tcp_nodelay: true,
            backlog: None,
        }
    }
}

/// Builder for [`ServerConfig`].
#[derive(Clone, Debug)]
pub struct ServerConfigBuilder {
    cfg: ServerConfig,
}

impl ServerConfig {
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder {
            cfg: ServerConfig::default(),
        }
    }
}

impl ServerConfigBuilder {
    /// Set the maximum total frame size (bytes).
    pub fn max_frame_size(mut self, value: usize) -> Self {
        self.cfg.max_frame_size = value.max(1);
        self
    }

    /// Set the maximum bulk string length (bytes).
    pub fn max_bulk_len(mut self, value: usize) -> Self {
        self.cfg.max_bulk_len = value.max(1);
        self
    }

    /// Set the maximum array length (elements).
    pub fn max_array_len(mut self, value: usize) -> Self {
        self.cfg.max_array_len = value.max(1);
        self
    }

    /// Set the maximum nesting depth.
    pub fn max_depth(mut self, value: usize) -> Self {
        self.cfg.max_depth = value.max(1);
        self
    }

    /// Set the maximum number of buffered in-flight requests.
    pub fn max_inflight_requests(mut self, value: usize) -> Self {
        self.cfg.max_inflight_requests = value.max(1);
        self
    }

    /// Set the maximum number of concurrent connections.
    pub fn max_connections(mut self, value: usize) -> Self {
        self.cfg.max_connections = value.max(1);
        self
    }

    /// Set the read timeout for socket reads.
    pub fn read_timeout(mut self, value: Option<Duration>) -> Self {
        self.cfg.read_timeout = value;
        self
    }

    /// Set the write timeout for socket writes.
    pub fn write_timeout(mut self, value: Option<Duration>) -> Self {
        self.cfg.write_timeout = value;
        self
    }

    /// Set the idle timeout for connections.
    pub fn idle_timeout(mut self, value: Option<Duration>) -> Self {
        self.cfg.idle_timeout = value;
        self
    }

    /// Set the push queue length.
    pub fn push_queue_len(mut self, value: usize) -> Self {
        self.cfg.push_queue_len = value.max(1);
        self
    }

    /// Set the response queue length.
    pub fn response_queue_len(mut self, value: usize) -> Self {
        self.cfg.response_queue_len = value.max(1);
        self
    }

    /// Set the write batch size (bytes).
    pub fn write_batch_bytes(mut self, value: usize) -> Self {
        self.cfg.write_batch_bytes = value.max(1);
        self
    }

    /// Enable or disable TCP_NODELAY.
    pub fn tcp_nodelay(mut self, value: bool) -> Self {
        self.cfg.tcp_nodelay = value;
        self
    }

    /// Set the listener backlog (may be ignored depending on platform).
    pub fn backlog(mut self, value: Option<u32>) -> Self {
        self.cfg.backlog = value;
        self
    }

    /// Finalize the configuration.
    pub fn build(self) -> ServerConfig {
        self.cfg
    }
}

/// Server builder for configuring the runtime.
pub struct ServerBuilder {
    addr: String,
    cfg: ServerConfig,
    shutdown: Option<BoxFuture>,
    hooks: Arc<dyn ServerHooks>,
    extensions_factory: ExtensionsFactory,
}

type BoxFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

impl ServerBuilder {
    /// Apply a custom server configuration.
    pub fn with_config(mut self, cfg: ServerConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Provide a future to trigger graceful shutdown.
    pub fn with_graceful_shutdown<F>(mut self, fut: F) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.shutdown = Some(Box::pin(fut));
        self
    }

    /// Provide observability hooks for the server runtime.
    pub fn with_hooks<H>(mut self, hooks: H) -> Self
    where
        H: ServerHooks,
    {
        self.hooks = Arc::new(hooks);
        self
    }

    /// Provide per-connection extensions for request contexts.
    ///
    /// The factory is invoked once per connection and its `Extensions` are
    /// cloned into each `RequestContext` for that connection.
    pub fn with_connection_extensions<F>(mut self, factory: F) -> Self
    where
        F: Fn(ConnectionInfo) -> Extensions + Send + Sync + 'static,
    {
        self.extensions_factory = Arc::new(factory);
        self
    }

    pub async fn serve<State>(self, app: Router<State>) -> Result<()>
    where
        State: Send + Sync + 'static,
    {
        let listener = if let Some(backlog) = self.cfg.backlog {
            bind_with_backlog(&self.addr, backlog)?
        } else {
            TcpListener::bind(&self.addr).await?
        };
        self.serve_with_listener(listener, app).await
    }

    /// Serve using a pre-bound listener (useful for tests).
    pub async fn serve_with_listener<State>(
        self,
        listener: TcpListener,
        app: Router<State>,
    ) -> Result<()>
    where
        State: Send + Sync + 'static,
    {
        run_server(
            listener,
            app,
            self.cfg,
            self.shutdown,
            self.hooks,
            self.extensions_factory,
        )
        .await
    }
}

/// A RESP server.
pub struct Server;

impl Server {
    /// Bind a server to the provided address.
    pub fn bind(addr: impl Into<String>) -> ServerBuilder {
        ServerBuilder {
            addr: addr.into(),
            cfg: ServerConfig::default(),
            shutdown: None,
            hooks: Arc::new(NoopServerHooks),
            extensions_factory: Arc::new(default_extensions),
        }
    }
}

fn bind_with_backlog(addr: &str, backlog: u32) -> Result<TcpListener> {
    let mut addrs = addr.to_socket_addrs()?;
    let addr = addrs.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "empty bind address")
    })?;
    let domain = Domain::for_address(addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;
    let backlog = backlog.max(1).min(i32::MAX as u32) as i32;
    socket.listen(backlog)?;
    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener)
}

async fn run_server<State>(
    listener: TcpListener,
    app: Router<State>,
    cfg: ServerConfig,
    shutdown: Option<BoxFuture>,
    hooks: Arc<dyn ServerHooks>,
    extensions_factory: ExtensionsFactory,
) -> Result<()>
where
    State: Send + Sync + 'static,
{
    let (shutdown_tx, _) = broadcast::channel(1);
    let mut shutdown_rx = shutdown_tx.subscribe();
    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(cfg.max_connections));
    let client_id = Arc::new(AtomicU64::new(1));

    let shutdown_fut = async move {
        if let Some(fut) = shutdown {
            fut.await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let mut shutdown_fut = Box::pin(shutdown_fut);

    loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break;
            }
            _ = shutdown_rx.recv() => {
                break;
            }
            accept = listener.accept() => {
                let (socket, _) = match accept {
                    Ok(value) => value,
                    Err(err) => {
                        error!("accept error: {:?}", err);
                        hooks.on_accept_error(&err);
                        continue;
                    }
                };

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        continue;
                    }
                };

                if cfg.tcp_nodelay {
                    let _ = socket.set_nodelay(true);
                }

                let handler = app.clone();
                let cfg = cfg.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                let id = client_id.fetch_add(1, Ordering::AcqRel);
                let hooks = Arc::clone(&hooks);
                let extensions_factory = Arc::clone(&extensions_factory);
                join_set.spawn(async move {
                    let _permit = permit;
                    if let Err(err) =
                        run_connection(
                            id,
                            socket,
                            handler,
                            cfg,
                            shutdown_rx,
                            hooks.clone(),
                            extensions_factory,
                        )
                        .await
                    {
                        error!("connection error: {:?}", err);
                        hooks.on_io_error(&err);
                    }
                });
            }
        }
    }

    let _ = shutdown_tx.send(());

    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res {
            error!("connection task error: {:?}", err);
        }
    }

    Ok(())
}

async fn run_connection<State>(
    id: u64,
    socket: TcpStream,
    app: Router<State>,
    cfg: ServerConfig,
    mut shutdown: broadcast::Receiver<()>,
    hooks: Arc<dyn ServerHooks>,
    extensions_factory: ExtensionsFactory,
) -> Result<()>
where
    State: Send + Sync + 'static,
{
    let peer_addr = socket.peer_addr()?;
    let local_addr = socket.local_addr()?;
    let info = ConnectionInfo {
        id,
        peer_addr,
        local_addr,
    };
    hooks.on_connection_open(info);
    let _guard = ConnectionGuard {
        hooks: Arc::clone(&hooks),
        info,
    };
    let (mut reader, writer) = socket.into_split();

    let (resp_tx, resp_rx) = mpsc::channel(cfg.response_queue_len);
    let (push_tx, push_rx) = mpsc::channel(cfg.push_queue_len);
    let (close_tx, mut close_rx) = mpsc::channel(1);
    let (writer_close_tx, writer_close_rx) = oneshot::channel();
    let mut writer_close_tx = Some(writer_close_tx);

    let push_handle = PushHandle::new(push_tx, close_tx);
    let pubsub_count = Arc::new(AtomicUsize::new(0));
    let pubsub_handle = PubSubHandle::new(pubsub_count.clone());
    let extensions = (extensions_factory)(info);

    let writer_cfg = cfg.clone();
    let writer_task = tokio::spawn(async move {
        writer_loop(writer, resp_rx, push_rx, writer_close_rx, writer_cfg).await
    });

    let mut rd = BytesMut::with_capacity(4096);
    let mut decoder = ValueDecoder::new(DecodeLimits {
        max_bulk_len: cfg.max_bulk_len,
        max_array_len: cfg.max_array_len,
        max_depth: cfg.max_depth,
    });
    let mut inflight = VecDeque::new();

    loop {
        while inflight.len() < cfg.max_inflight_requests {
            match decoder.try_decode(&mut rd) {
                Ok(Some(value)) => {
                    let command = match Command::from_value(value) {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            hooks.on_protocol_error(&err);
                            let _ = resp_tx.send(err.into_response()).await;
                            signal_writer_close(&mut writer_close_tx);
                            return Ok(());
                        }
                    };

                    if pubsub_count.load(Ordering::Acquire) > 0
                        && !is_pubsub_allowed(&command.name_upper)
                    {
                        let err = RespError::invalid_data(
                            "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context",
                        );
                        hooks.on_protocol_error(&err);
                        let _ = resp_tx.send(err.into_response()).await;
                        continue;
                    }

                    hooks.on_command(id, &command);
                    inflight.push_back(command);
                }
                Ok(None) => break,
                Err(err) => {
                    hooks.on_protocol_error(&err);
                    let _ = resp_tx.send(err.into_response()).await;
                    signal_writer_close(&mut writer_close_tx);
                    return Ok(());
                }
            }
        }

        if let Some(command) = inflight.pop_front() {
            let close_after = command.name_upper.as_ref() == b"QUIT";
            let ctx = RequestContext {
                command,
                peer_addr,
                local_addr,
                client_id: id,
                extensions: extensions.clone(),
                push: push_handle.clone(),
                pubsub: pubsub_handle.clone(),
            };
            let response = app.call(ctx).await;
            if resp_tx.send(response).await.is_err() {
                break;
            }
            if close_after {
                signal_writer_close(&mut writer_close_tx);
                break;
            }
            continue;
        }

        tokio::select! {
            _ = shutdown.recv() => {
                signal_writer_close(&mut writer_close_tx);
                break;
            }
            _ = close_rx.recv() => {
                let err = RespError::invalid_data("ERR client output buffer limit reached");
                hooks.on_protocol_error(&err);
                let _ = resp_tx.try_send(err.into_response());
                signal_writer_close(&mut writer_close_tx);
                break;
            }
            read = read_more(&mut reader, &mut rd, cfg.read_timeout, cfg.idle_timeout) => {
                let read = read?;
                if read == 0 {
                    if rd.is_empty() {
                        signal_writer_close(&mut writer_close_tx);
                        break;
                    }
                    let err = RespError::invalid_data("ERR unexpected EOF");
                    hooks.on_protocol_error(&err);
                    let _ = resp_tx.send(err.into_response()).await;
                    signal_writer_close(&mut writer_close_tx);
                    break;
                }
                if rd.len() > cfg.max_frame_size {
                    let err = RespError::invalid_data("ERR max frame size exceeded");
                    hooks.on_protocol_error(&err);
                    let _ = resp_tx.send(err.into_response()).await;
                    signal_writer_close(&mut writer_close_tx);
                    break;
                }
            }
        }
    }

    signal_writer_close(&mut writer_close_tx);
    drop(resp_tx);
    drop(push_handle);

    let _ = writer_task.await;
    Ok(())
}

async fn read_more(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    buf: &mut BytesMut,
    read_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
) -> Result<usize> {
    let fut = reader.read_buf(buf);
    let timeout = idle_timeout.or(read_timeout);
    if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
        }
    } else {
        fut.await
    }
}

async fn writer_loop(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut resp_rx: mpsc::Receiver<Value>,
    mut push_rx: mpsc::Receiver<Value>,
    mut close_rx: oneshot::Receiver<()>,
    cfg: ServerConfig,
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(cfg.write_batch_bytes);
    let mut resp_closed = false;
    let mut push_closed = false;
    let mut closing = false;

    loop {
        let mut drained_response = false;
        while let Ok(value) = resp_rx.try_recv() {
            drained_response = true;
            value.encode(&mut buf);
            if buf.len() >= cfg.write_batch_bytes {
                flush_buffer(&mut writer, &mut buf, cfg.write_timeout).await?;
            }
        }
        if !drained_response && !closing {
            while let Ok(value) = push_rx.try_recv() {
                value.encode(&mut buf);
                if buf.len() >= cfg.write_batch_bytes {
                    break;
                }
            }
        }
        if !buf.is_empty() {
            flush_buffer(&mut writer, &mut buf, cfg.write_timeout).await?;
        }

        if closing {
            break;
        }

        if resp_closed && push_closed {
            break;
        }

        tokio::select! {
            biased;
            _ = &mut close_rx => {
                closing = true;
            }
            res = resp_rx.recv() => {
                match res {
                    Some(value) => value.encode(&mut buf),
                    None => resp_closed = true,
                }
            }
            res = push_rx.recv(), if !closing => {
                match res {
                    Some(value) => value.encode(&mut buf),
                    None => push_closed = true,
                }
            }
        }

        if !buf.is_empty() {
            flush_buffer(&mut writer, &mut buf, cfg.write_timeout).await?;
        }
    }

    Ok(())
}

fn signal_writer_close(tx: &mut Option<oneshot::Sender<()>>) {
    if let Some(tx) = tx.take() {
        let _ = tx.send(());
    }
}

async fn flush_buffer(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    buf: &mut BytesMut,
    timeout: Option<Duration>,
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let write = writer.write_all(buf);
    if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, write).await {
            Ok(res) => res?,
            Err(_) => return Err(std::io::ErrorKind::TimedOut.into()),
        }
    } else {
        write.await?;
    }
    buf.clear();
    Ok(())
}

fn is_pubsub_allowed(cmd: &bytes::Bytes) -> bool {
    matches!(
        cmd.as_ref(),
        b"SUBSCRIBE" | b"PSUBSCRIBE" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PING" | b"QUIT"
    )
}
