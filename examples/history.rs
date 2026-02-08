extern crate resp_async;
extern crate tokio;

use std::env;
use std::sync::Mutex;

use bytes::Bytes;
use tokio::signal;

use resp_async::error::Result;
use resp_async::{Cmd, Router, Server, State, Value};

#[derive(Default)]
struct AppState {
    history: Mutex<Vec<Value>>,
}

async fn history(State(state): State<AppState>, Cmd(cmd): Cmd) -> Value {
    if cmd.name_upper.as_ref() == b"COMMAND" {
        if let Some(Value::Bulk(arg)) = cmd.args.get(0) {
            if arg.as_ref() == b"DOCS" {
                return Value::Array(Vec::new());
            }
        }
    }

    let mut history = state.history.lock().unwrap();
    let mut entry = Vec::with_capacity(1 + cmd.args.len());
    entry.push(Value::Bulk(cmd.name));
    entry.extend(cmd.args);
    history.push(Value::Array(entry));
    Value::Array(history.clone())
}

async fn ping() -> Value {
    Value::Simple(Bytes::from_static(b"PONG"))
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:6379".to_string());

    let app = Router::from_state(AppState::default())
        .route("HISTORY", history)
        .route("COMMAND", history)
        .route("PING", ping);

    Server::bind(&addr)
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
        })
        .serve(app)
        .await
}
