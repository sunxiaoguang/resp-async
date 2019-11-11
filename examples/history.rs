extern crate futures;
extern crate resp_async;
extern crate tokio;
extern crate tokio_codec;
#[macro_use]
extern crate log;
extern crate simplelog;

use std::env;
use std::net::SocketAddr;
use std::sync::Mutex;

use futures::future;
use simplelog::*;
use tokio::prelude::*;

use resp_async::error::*;
use resp_async::*;

#[derive(Default)]
struct HistoryServer {
    connections: Mutex<u64>,
}

impl EventHandler for HistoryServer {
    type ClientUserData = i32;

    fn on_request(
        &self,
        peer: &mut PeerContext<Self::ClientUserData>,
        request: Value,
    ) -> Box<dyn Future<Item = Value, Error = Error> + Send + Sync> {
        let value = vec![peer.user_data.into(), request].into();
        if let Some(Value::Array(history)) = peer.get_mut("history") {
            history.push(value);
        } else {
            peer.set("history", vec![value].into());
        }
        peer.user_data += 1;
        Box::new(future::ok(Clone::clone(peer.get("history").unwrap())))
    }

    fn on_connect(&self, id: u64) -> Result<Self::ClientUserData> {
        let mut connections = self.connections.lock().unwrap();
        *connections += 1;
        info!("New connection: {}", id);
        info!("Total connected users: {}", *connections);
        Ok(0)
    }

    fn on_disconnect(&self, id: u64) {
        let mut connections = self.connections.lock().unwrap();
        *connections -= 1;
        info!("Disconnected connection: {}", id);
        info!("Total connected users: {}", *connections);
    }
}

fn main() {
    let _ = TermLogger::init(LevelFilter::Info, Config::default());
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>().unwrap();
    let mut server = Server::new(HistoryServer::default());
    server.listen(&addr).unwrap();
    server.serve().unwrap();
}
