use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

use crate::context::{
    ClientId, Cmd, Command, Extensions, LocalAddr, PeerAddr, PubSubHandle, PushHandle,
    RequestContext, State as AppState,
};
use crate::response::{IntoResponse, RespError, Response};

/// Extract a typed value from a request context.
pub trait FromRequest<State>: Sized {
    type Rejection: IntoResponse;

    fn from_request(
        ctx: &mut RequestContext,
        state: &Arc<State>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Handler for a RESP command.
pub trait Handler<State>: Send + Sync + 'static {
    fn call(&self, ctx: RequestContext, state: Arc<State>) -> BoxFuture<Response>;
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub trait IntoHandler<State, Args>: Send + Sync + 'static {
    fn into_handler(self) -> Arc<dyn Handler<State>>;
}

struct HandlerFn0<F> {
    f: F,
}

struct HandlerFn1<F, T1> {
    f: F,
    _t1: PhantomData<fn(T1)>,
}

struct HandlerFn2<F, T1, T2> {
    f: F,
    _t: PhantomData<fn(T1, T2)>,
}

struct HandlerFn3<F, T1, T2, T3> {
    f: F,
    _t: PhantomData<fn(T1, T2, T3)>,
}

struct HandlerFn4<F, T1, T2, T3, T4> {
    f: F,
    _t: PhantomData<fn(T1, T2, T3, T4)>,
}

struct HandlerFn5<F, T1, T2, T3, T4, T5> {
    f: F,
    _t: PhantomData<fn(T1, T2, T3, T4, T5)>,
}

struct HandlerFn6<F, T1, T2, T3, T4, T5, T6> {
    f: F,
    _t: PhantomData<fn(T1, T2, T3, T4, T5, T6)>,
}

macro_rules! impl_handler {
    ($name:ident, $( $ty:ident ),* ) => {
        #[allow(non_snake_case)]
        impl<State, F, Fut, R, $( $ty ),*> Handler<State> for $name<F, $( $ty ),*>
        where
            F: Copy + Send + Sync + 'static + Fn($( $ty ),*) -> Fut,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoResponse,
            $( $ty: FromRequest<State> + Send + 'static, )*
            State: Send + Sync + 'static,
        {
            fn call(&self, mut ctx: RequestContext, state: Arc<State>) -> BoxFuture<Response> {
                let f = self.f;
                Box::pin(async move {
                    $(
                        let $ty = match $ty::from_request(&mut ctx, &state).await {
                            Ok(value) => value,
                            Err(rejection) => return rejection.into_response(),
                        };
                    )*

                    f($( $ty ),*).await.into_response()
                })
            }
        }
    };
}

impl<State, F, Fut, R> Handler<State> for HandlerFn0<F>
where
    F: Copy + Send + Sync + 'static + Fn() -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    State: Send + Sync + 'static,
{
    fn call(&self, _ctx: RequestContext, _state: Arc<State>) -> BoxFuture<Response> {
        let f = self.f;
        Box::pin(async move { f().await.into_response() })
    }
}

impl_handler!(HandlerFn1, T1);
impl_handler!(HandlerFn2, T1, T2);
impl_handler!(HandlerFn3, T1, T2, T3);
impl_handler!(HandlerFn4, T1, T2, T3, T4);
impl_handler!(HandlerFn5, T1, T2, T3, T4, T5);
impl_handler!(HandlerFn6, T1, T2, T3, T4, T5, T6);

impl<State, F, Fut, R> IntoHandler<State, ()> for F
where
    F: Copy + Send + Sync + 'static + Fn() -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn0 { f: self })
    }
}

impl<State, F, Fut, R, T1> IntoHandler<State, (T1,)> for F
where
    F: Copy + Send + Sync + 'static + Fn(T1) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn1 {
            f: self,
            _t1: PhantomData,
        })
    }
}

impl<State, F, Fut, R, T1, T2> IntoHandler<State, (T1, T2)> for F
where
    F: Copy + Send + Sync + 'static + Fn(T1, T2) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    T2: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn2 {
            f: self,
            _t: PhantomData,
        })
    }
}

impl<State, F, Fut, R, T1, T2, T3> IntoHandler<State, (T1, T2, T3)> for F
where
    F: Copy + Send + Sync + 'static + Fn(T1, T2, T3) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    T2: FromRequest<State> + Send + 'static,
    T3: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn3 {
            f: self,
            _t: PhantomData,
        })
    }
}

impl<State, F, Fut, R, T1, T2, T3, T4> IntoHandler<State, (T1, T2, T3, T4)> for F
where
    F: Copy + Send + Sync + 'static + Fn(T1, T2, T3, T4) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    T2: FromRequest<State> + Send + 'static,
    T3: FromRequest<State> + Send + 'static,
    T4: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn4 {
            f: self,
            _t: PhantomData,
        })
    }
}

impl<State, F, Fut, R, T1, T2, T3, T4, T5> IntoHandler<State, (T1, T2, T3, T4, T5)> for F
where
    F: Copy + Send + Sync + 'static + Fn(T1, T2, T3, T4, T5) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    T2: FromRequest<State> + Send + 'static,
    T3: FromRequest<State> + Send + 'static,
    T4: FromRequest<State> + Send + 'static,
    T5: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn5 {
            f: self,
            _t: PhantomData,
        })
    }
}

impl<State, F, Fut, R, T1, T2, T3, T4, T5, T6> IntoHandler<State, (T1, T2, T3, T4, T5, T6)>
    for F
where
    F: Copy + Send + Sync + 'static + Fn(T1, T2, T3, T4, T5, T6) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    T1: FromRequest<State> + Send + 'static,
    T2: FromRequest<State> + Send + 'static,
    T3: FromRequest<State> + Send + 'static,
    T4: FromRequest<State> + Send + 'static,
    T5: FromRequest<State> + Send + 'static,
    T6: FromRequest<State> + Send + 'static,
    State: Send + Sync + 'static,
{
    fn into_handler(self) -> Arc<dyn Handler<State>> {
        Arc::new(HandlerFn6 {
            f: self,
            _t: PhantomData,
        })
    }
}

/// Router mapping command names to handlers.
pub struct Router<State = ()> {
    inner: Arc<RouterInner<State>>,
}

impl<State> Clone for Router<State> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct RouterInner<State> {
    state: Arc<State>,
    routes: HashMap<Bytes, Arc<dyn Handler<State>>>,
}

impl<State> Router<State>
where
    State: Default + Send + Sync + 'static,
{
    /// Create a new router using `State::default()`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RouterInner {
                state: Arc::new(State::default()),
                routes: HashMap::new(),
            }),
        }
    }
}

impl<State> Router<State>
where
    State: Send + Sync + 'static,
{
    /// Create a new router with the provided state.
    pub fn from_state(state: State) -> Self {
        Self {
            inner: Arc::new(RouterInner {
                state: Arc::new(state),
                routes: HashMap::new(),
            }),
        }
    }

    /// Replace the state while keeping existing routes.
    pub fn with_state(self, state: State) -> Self {
        let mut inner = self.into_inner();
        inner.state = Arc::new(state);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Register a command handler.
    ///
    /// The handler must be an async function item (or other `fn` pointer) so it
    /// can be converted into a `fn(...) -> Future` type.
    pub fn route<H, Args>(self, command: &'static str, handler: H) -> Self
    where
        H: IntoHandler<State, Args>,
    {
        let mut inner = self.into_inner();
        inner
            .routes
            .insert(normalize_command_key(command), handler.into_handler());
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn state(&self) -> Arc<State> {
        Arc::clone(&self.inner.state)
    }

    pub(crate) fn call(&self, ctx: RequestContext) -> BoxFuture<Response> {
        let Some(handler) = self.inner.routes.get(&ctx.command.name_upper).cloned() else {
            return Box::pin(async move {
                RespError::invalid_data(format!(
                    "ERR unknown command '{}'",
                    display_command_name(&ctx.command.name)
                ))
                .into_response()
            });
        };
        handler.call(ctx, self.state())
    }

    fn into_inner(self) -> RouterInner<State> {
        Arc::try_unwrap(self.inner).unwrap_or_else(|arc| RouterInner {
            state: Arc::clone(&arc.state),
            routes: arc.routes.clone(),
        })
    }
}

fn normalize_command_key(command: &str) -> Bytes {
    let bytes = command.as_bytes();
    let mut needs = false;
    for &b in bytes {
        if b.is_ascii_lowercase() {
            needs = true;
            break;
        }
    }
    if !needs {
        return Bytes::copy_from_slice(command.as_bytes());
    }
    let mut buf = BytesMut::with_capacity(bytes.len());
    for &b in bytes {
        buf.put_u8(b.to_ascii_uppercase());
    }
    buf.freeze()
}

fn display_command_name(bytes: &Bytes) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => format!("0x{}", hex_bytes(bytes)),
    }
}

fn hex_bytes(bytes: &Bytes) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes.iter() {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

impl<State> FromRequest<State> for Cmd
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(Cmd(ctx.command.clone()))
    }
}

impl<T> FromRequest<T> for AppState<T>
where
    T: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        _ctx: &mut RequestContext,
        state: &Arc<T>,
    ) -> Result<Self, Self::Rejection> {
        Ok(AppState(Arc::clone(state)))
    }
}

impl<State> FromRequest<State> for PeerAddr
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(PeerAddr(ctx.peer_addr))
    }
}

impl<State> FromRequest<State> for LocalAddr
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(LocalAddr(ctx.local_addr))
    }
}

impl<State> FromRequest<State> for ClientId
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ClientId(ctx.client_id))
    }
}

impl<State> FromRequest<State> for Extensions
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ctx.extensions.clone())
    }
}

impl<State> FromRequest<State> for PushHandle
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ctx.push.clone())
    }
}

impl<State> FromRequest<State> for PubSubHandle
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ctx.pubsub.clone())
    }
}

impl<State> FromRequest<State> for Command
where
    State: Send + Sync + 'static,
{
    type Rejection = Infallible;

    async fn from_request(
        ctx: &mut RequestContext,
        _state: &Arc<State>,
    ) -> Result<Self, Self::Rejection> {
        Ok(ctx.command.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use crate::Value;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    fn make_ctx(cmd: Command) -> RequestContext {
        let (push_tx, _push_rx) = mpsc::channel(1);
        let (close_tx, _close_rx) = mpsc::channel(1);
        RequestContext {
            command: cmd,
            peer_addr: "127.0.0.1:1".parse().unwrap(),
            local_addr: "127.0.0.1:2".parse().unwrap(),
            client_id: 1,
            extensions: Extensions::default(),
            push: PushHandle::new(push_tx, close_tx),
            pubsub: PubSubHandle::new(Arc::new(AtomicUsize::new(0))),
        }
    }

    async fn ping() -> Value {
        Value::Simple(Bytes::from_static(b"PONG"))
    }

    #[tokio::test]
    async fn route_dispatches() {
        let app: Router<()> = Router::new().route("PING", ping);
        let cmd = Command::new(Bytes::from_static(b"PING"), Vec::new());
        let resp = app.call(make_ctx(cmd)).await;
        assert_eq!(resp, Value::Simple(Bytes::from_static(b"PONG")));
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let app: Router<()> = Router::new();
        let cmd = Command::new(Bytes::from_static(b"NOPE"), Vec::new());
        let resp = app.call(make_ctx(cmd)).await;
        assert!(matches!(resp, Value::Error(_)));
    }

    #[tokio::test]
    async fn state_extractor_works() {
        async fn handler(AppState(state): AppState<u64>) -> Value {
            Value::Integer(*state as i64)
        }

        let app = Router::from_state(5u64).route("GET", handler);
        let cmd = Command::new(Bytes::from_static(b"GET"), Vec::new());
        let resp = app.call(make_ctx(cmd)).await;
        assert_eq!(resp, Value::Integer(5));
    }
}
