use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::resp::Value;
use crate::response::RespError;

/// A parsed Redis command with normalized name and arguments.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Command {
    pub name: Bytes,
    pub name_upper: Bytes,
    pub args: Vec<Value>,
}

impl Command {
    pub fn new(name: Bytes, args: Vec<Value>) -> Self {
        let name_upper = normalize_command_name(&name);
        Self {
            name,
            name_upper,
            args,
        }
    }

    pub fn from_value(value: Value) -> Result<Self, RespError> {
        match value {
            Value::Array(mut items) => {
                if items.is_empty() {
                    return Err(RespError::invalid_data("ERR empty command"));
                }
                let name_value = items.remove(0);
                let name = match name_value {
                    Value::Bulk(b) | Value::Simple(b) => b,
                    other => {
                        return Err(RespError::invalid_data(format!(
                            "ERR invalid command name: {:?}",
                            other
                        )));
                    }
                };
                Ok(Command::new(name, items))
            }
            other => Err(RespError::invalid_data(format!(
                "ERR expected array, got {:?}",
                other
            ))),
        }
    }
}

fn normalize_command_name(name: &Bytes) -> Bytes {
    let mut needs = false;
    for &b in name.iter() {
        if b.is_ascii_lowercase() {
            needs = true;
            break;
        }
    }
    if !needs {
        return name.clone();
    }
    let mut buf = BytesMut::with_capacity(name.len());
    for &b in name.iter() {
        buf.put_u8(b.to_ascii_uppercase());
    }
    buf.freeze()
}

/// Per-request context passed to handlers.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub command: Command,
    pub peer_addr: SocketAddr,
    pub local_addr: SocketAddr,
    pub client_id: u64,
    pub extensions: Extensions,
    pub push: PushHandle,
    pub pubsub: PubSubHandle,
}

/// Typed extensions map stored in the request context.
#[derive(Debug, Default, Clone)]
pub struct Extensions {
    inner: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Extensions {
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.inner.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.inner
            .get(&TypeId::of::<T>())
            .and_then(|value| value.as_ref().downcast_ref::<T>())
    }

    /// Returns a mutable reference when this entry is uniquely owned.
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        self.inner
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| Arc::get_mut(value))
            .and_then(|value| value.downcast_mut::<T>())
    }
}

/// Handle for sending out-of-band push responses.
#[derive(Debug, Clone)]
pub struct PushHandle {
    tx: mpsc::Sender<Value>,
    close_tx: mpsc::Sender<()>,
}

impl PushHandle {
    pub(crate) fn new(tx: mpsc::Sender<Value>, close_tx: mpsc::Sender<()>) -> Self {
        Self { tx, close_tx }
    }

    pub async fn send(&self, value: Value) -> Result<(), PushError> {
        match self.tx.try_send(value) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let _ = self.close_tx.try_send(());
                Err(PushError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(PushError::Closed),
        }
    }
}

/// Errors when sending push messages.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PushError {
    Full,
    Closed,
}

/// Handle for updating Pub/Sub subscription count.
#[derive(Debug, Clone)]
pub struct PubSubHandle {
    count: Arc<AtomicUsize>,
}

impl PubSubHandle {
    pub(crate) fn new(count: Arc<AtomicUsize>) -> Self {
        Self { count }
    }

    pub fn set(&self, count: usize) {
        self.count.store(count, Ordering::Release);
    }

    pub fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn decrement(&self) -> usize {
        let prev = self
            .count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value == 0 { Some(0) } else { Some(value - 1) }
            })
            .unwrap_or(0);
        prev.saturating_sub(1)
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

/// Extractor wrapper for command.
#[derive(Debug, Clone)]
pub struct Cmd(pub Command);

/// Extractor wrapper for shared application state.
#[derive(Debug, Clone)]
pub struct State<T>(pub Arc<T>);

/// Extractor wrapper for peer address.
#[derive(Debug, Clone, Copy)]
pub struct PeerAddr(pub SocketAddr);

/// Extractor wrapper for local address.
#[derive(Debug, Clone, Copy)]
pub struct LocalAddr(pub SocketAddr);

/// Extractor wrapper for client id.
#[derive(Debug, Clone, Copy)]
pub struct ClientId(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn command_from_value_normalizes() {
        let value = Value::Array(vec![
            Value::Bulk(Bytes::from_static(b"ping")),
            Value::Bulk(Bytes::from_static(b"hi")),
        ]);
        let cmd = Command::from_value(value).unwrap();
        assert_eq!(cmd.name_upper.as_ref(), b"PING");
        assert_eq!(cmd.args.len(), 1);
    }

    #[test]
    fn extensions_insert_get_mut() {
        let mut ext = Extensions::default();
        ext.insert(42usize);
        assert_eq!(ext.get::<usize>(), Some(&42));
        if let Some(value) = ext.get_mut::<usize>() {
            *value = 43;
        }
        assert_eq!(ext.get::<usize>(), Some(&43));

        let _clone = ext.clone();
        assert!(ext.get_mut::<usize>().is_none());
    }

    #[test]
    fn pubsub_handle_counts() {
        let count = Arc::new(AtomicUsize::new(0));
        let handle = PubSubHandle::new(count);
        assert_eq!(handle.count(), 0);
        assert_eq!(handle.increment(), 1);
        assert_eq!(handle.decrement(), 0);
        handle.set(3);
        assert_eq!(handle.count(), 3);
    }
}
