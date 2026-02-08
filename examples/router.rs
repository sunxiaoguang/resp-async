extern crate resp_async;
extern crate tokio;

use bytes::Bytes;
use resp_async::error::Result;
use resp_async::{ConnectionInfo, Router, Server, ServerHooks, Value};

struct Hooks;

impl ServerHooks for Hooks {
    fn on_connection_open(&self, info: ConnectionInfo) {
        println!("client {} connected from {}", info.id, info.peer_addr);
    }

    fn on_connection_close(&self, info: ConnectionInfo) {
        println!("client {} disconnected", info.id);
    }
}

async fn ping() -> Value {
    Value::Simple(Bytes::from_static(b"PONG"))
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let app: Router<()> = Router::new().route("PING", ping);
    Server::bind("0.0.0.0:6379")
        .with_hooks(Hooks)
        .serve(app)
        .await
}
