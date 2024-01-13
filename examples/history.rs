extern crate resp_async;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate simplelog;

use std::env;
use std::sync::Mutex;

use simplelog::*;
use tokio::signal;

use resp_async::error::*;
use resp_async::*;

#[derive(Default)]
struct HistoryServer {
    connections: Mutex<u64>,
}

impl EventHandler for HistoryServer {
    type ClientUserData = i32;

    async fn on_request(
        &self,
        peer: &mut PeerContext<Self::ClientUserData>,
        request: Value,
    ) -> Result<Value> {
        let mut resp = None;
        if let Value::Array(array) = &request {
            if array.len() == 2 {
                if let (Some(Value::Bulk(cmd)), Some(Value::Bulk(arg))) =
                    (array.get(0), array.get(1))
                {
                    if cmd == "COMMAND".as_bytes() && arg == "DOCS".as_bytes() {
                        resp = Some(Value::Array(Vec::new()));
                    }
                }
            }
        }

        let value = vec![peer.user_data.into(), request].into();
        if let Some(Value::Array(history)) = peer.get_mut("history") {
            history.push(value);
        } else {
            peer.set("history", vec![value].into());
        }
        peer.user_data += 1;
        Ok(resp.unwrap_or_else(|| Clone::clone(peer.get("history").unwrap())))
    }

    async fn on_connect(&self, id: u64) -> Result<Self::ClientUserData> {
        let mut connections = self.connections.lock().unwrap();
        *connections += 1;
        info!("New connection: {}", id);
        info!("Total connected users: {}", *connections);
        Ok(0)
    }

    async fn on_disconnect(&self, id: u64) {
        let mut connections = self.connections.lock().unwrap();
        *connections -= 1;
        info!("Disconnected connection: {}", id);
        info!("Total connected users: {}", *connections);
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let _ = TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    );
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:6379".to_string());
    let mut server = Server::new(HistoryServer::default());
    server.listen(&addr)?.serve(signal::ctrl_c()).await
}
