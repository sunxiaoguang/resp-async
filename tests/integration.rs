use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use resp_async::{Cmd, Extensions, PushHandle, Router, Server, ServerBuilder, Value, ValueDecoder};

#[derive(Debug)]
struct Marker(u8);

async fn start_server_with<State, F>(
    app: Router<State>,
    build: F,
) -> (SocketAddr, oneshot::Sender<()>)
where
    State: Send + Sync + 'static,
    F: FnOnce(ServerBuilder) -> ServerBuilder + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = build(Server::bind(addr.to_string()))
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .serve_with_listener(listener, app)
            .await;
    });

    (addr, shutdown_tx)
}

async fn start_server<State>(app: Router<State>) -> (SocketAddr, oneshot::Sender<()>)
where
    State: Send + Sync + 'static,
{
    start_server_with(app, |builder| builder).await
}

fn command(name: &str, args: &[&str]) -> Value {
    let mut items = Vec::with_capacity(1 + args.len());
    items.push(Value::Bulk(Bytes::from(name.as_bytes().to_vec())));
    for arg in args {
        items.push(Value::Bulk(Bytes::from(arg.as_bytes().to_vec())));
    }
    Value::Array(items)
}

async fn read_value(
    stream: &mut TcpStream,
    rd: &mut BytesMut,
    decoder: &mut ValueDecoder,
) -> Value {
    loop {
        if let Some(value) = decoder.try_decode(rd).unwrap() {
            return value;
        }
        let n = stream.read_buf(rd).await.unwrap();
        if n == 0 {
            panic!("unexpected EOF");
        }
    }
}

#[tokio::test]
async fn responses_are_ordered_for_pipelining() {
    async fn ping() -> Value {
        Value::Simple(Bytes::from_static(b"PONG"))
    }

    let app: Router<()> = Router::new().route("PING", ping);
    let (addr, shutdown) = start_server(app).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut rd = BytesMut::with_capacity(1024);
    let mut decoder = ValueDecoder::default();
    let mut buf = BytesMut::new();
    command("PING", &[]).encode(&mut buf);
    command("PING", &[]).encode(&mut buf);
    stream.write_all(&buf).await.unwrap();

    let first = read_value(&mut stream, &mut rd, &mut decoder).await;
    let second = read_value(&mut stream, &mut rd, &mut decoder).await;

    assert_eq!(first, Value::Simple(Bytes::from_static(b"PONG")));
    assert_eq!(second, Value::Simple(Bytes::from_static(b"PONG")));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn push_messages_follow_responses() {
    async fn subscribe(_cmd: Cmd, push: PushHandle) -> Value {
        let _ = push
            .send(Value::Array(vec![
                Value::Bulk(Bytes::from_static(b"message")),
                Value::Bulk(Bytes::from_static(b"chan")),
                Value::Bulk(Bytes::from_static(b"payload")),
            ]))
            .await;
        Value::Simple(Bytes::from_static(b"OK"))
    }

    let app: Router<()> = Router::new().route("SUB", subscribe);
    let (addr, shutdown) = start_server(app).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut rd = BytesMut::with_capacity(1024);
    let mut decoder = ValueDecoder::default();
    let mut buf = BytesMut::new();
    command("SUB", &[]).encode(&mut buf);
    stream.write_all(&buf).await.unwrap();

    let first = read_value(&mut stream, &mut rd, &mut decoder).await;
    let second = read_value(&mut stream, &mut rd, &mut decoder).await;

    assert_eq!(first, Value::Simple(Bytes::from_static(b"OK")));
    assert!(matches!(second, Value::Array(_)));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn connection_extensions_are_visible() {
    async fn check(ext: Extensions) -> Value {
        match ext.get::<Marker>() {
            Some(marker) => Value::Integer(i64::from(marker.0)),
            None => Value::Error(Bytes::from_static(b"ERR missing extension")),
        }
    }

    let app: Router<()> = Router::new().route("EXT", check);
    let (addr, shutdown) = start_server_with(app, |builder| {
        builder.with_connection_extensions(|_info| {
            let mut ext = Extensions::default();
            ext.insert(Marker(7));
            ext
        })
    })
    .await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut rd = BytesMut::with_capacity(1024);
    let mut decoder = ValueDecoder::default();
    let mut buf = BytesMut::new();
    command("EXT", &[]).encode(&mut buf);
    stream.write_all(&buf).await.unwrap();

    let response = read_value(&mut stream, &mut rd, &mut decoder).await;
    assert_eq!(response, Value::Integer(7));

    let _ = shutdown.send(());
}

#[tokio::test]
async fn quit_closes_connection() {
    async fn quit() -> Value {
        Value::Simple(Bytes::from_static(b"OK"))
    }

    let app: Router<()> = Router::new().route("QUIT", quit);
    let (addr, shutdown) = start_server(app).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut rd = BytesMut::with_capacity(1024);
    let mut decoder = ValueDecoder::default();
    let mut buf = BytesMut::new();
    command("QUIT", &[]).encode(&mut buf);
    stream.write_all(&buf).await.unwrap();

    let response = read_value(&mut stream, &mut rd, &mut decoder).await;
    assert_eq!(response, Value::Simple(Bytes::from_static(b"OK")));

    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .expect("read timeout")
        .unwrap();
    assert_eq!(read, 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn graceful_shutdown_stops_server() {
    let app: Router<()> = Router::new();
    let (addr, shutdown) = start_server(app).await;

    let _ = TcpStream::connect(addr).await.unwrap();
    let _ = shutdown.send(());
}
