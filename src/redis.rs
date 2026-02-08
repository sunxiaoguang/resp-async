// common errors and commands used in redis server
use bytes::Bytes;

use crate::resp::Value;

pub const ACL_WRONGPASS_ERROR: Value =
    Value::Error(Bytes::from_static(b"WRONGPASS invalid username-password pair or user is disabled."));

pub const BLOCKED_UNBLOCKED_ERROR: Value = Value::Error(Bytes::from_static(
    b"UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)",
));

pub const BLOCKED_NOGROUP_ERROR: Value = Value::Error(Bytes::from_static(
    b"NOGROUP the consumer group this client was blocked on no longer exists",
));

pub const CLUSTER_IOERR_ERROR: Value =
    Value::Error(Bytes::from_static(b"IOERR error or timeout connecting to the client"));

pub const NOAUTH_ERROR: Value = Value::Error(Bytes::from_static(b"NOAUTH Authentication required."));

pub const STRING_OK: Value = Value::Simple(Bytes::from_static(b"OK"));
