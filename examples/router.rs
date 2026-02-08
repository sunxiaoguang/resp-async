extern crate resp_async;
extern crate tokio;

use bytes::Bytes;
use resp_async::error::Result;
use resp_async::{Router, Server, Value};

async fn ping() -> Value {
    Value::Simple(Bytes::from_static(b"PONG"))
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let app: Router<()> = Router::new().route("PING", ping);
    Server::bind("0.0.0.0:6379").serve(app).await
}
