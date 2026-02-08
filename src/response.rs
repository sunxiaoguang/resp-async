use bytes::Bytes;
use std::convert::Infallible;

use crate::resp::Value;

/// RESP response type returned by handlers.
pub type Response = Value;

/// A lightweight RESP error for mapping failures to protocol responses.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RespError {
    /// Invalid client data or protocol violation.
    InvalidData(Bytes),
    /// Authentication required.
    NoAuth,
    /// Internal server error.
    Internal,
}

impl RespError {
    /// Build an invalid data error with an `ERR`-prefixed message.
    pub fn invalid_data(msg: impl Into<Bytes>) -> Self {
        let msg = msg.into();
        if msg.starts_with(b"ERR ") {
            RespError::InvalidData(msg)
        } else {
            let mut buf = Vec::with_capacity(4 + msg.len());
            buf.extend_from_slice(b"ERR ");
            buf.extend_from_slice(&msg);
            RespError::InvalidData(Bytes::from(buf))
        }
    }

    /// Build an internal error response.
    pub fn internal() -> Self {
        RespError::Internal
    }
}

/// Convert a handler result into a RESP response.
pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for RespError {
    fn into_response(self) -> Response {
        match self {
            RespError::InvalidData(msg) => Value::Error(msg),
            RespError::NoAuth => {
                Value::Error(Bytes::from_static(b"NOAUTH Authentication required."))
            }
            RespError::Internal => Value::Error(Bytes::from_static(b"ERR internal error")),
        }
    }
}

impl<T> IntoResponse for Result<T, RespError>
where
    T: IntoResponse,
{
    fn into_response(self) -> Response {
        match self {
            Ok(value) => value.into_response(),
            Err(err) => err.into_response(),
        }
    }
}

impl IntoResponse for Bytes {
    fn into_response(self) -> Response {
        Value::Bulk(self)
    }
}

impl IntoResponse for Vec<u8> {
    fn into_response(self) -> Response {
        Value::Bulk(Bytes::from(self))
    }
}

impl IntoResponse for &'static [u8] {
    fn into_response(self) -> Response {
        Value::Bulk(Bytes::from_static(self))
    }
}

impl IntoResponse for &'static str {
    fn into_response(self) -> Response {
        Value::Bulk(Bytes::from_static(self.as_bytes()))
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response {
        Value::Bulk(Bytes::from(self.into_bytes()))
    }
}

impl IntoResponse for Infallible {
    fn into_response(self) -> Response {
        match self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_data_prefixes_err() {
        let err = RespError::invalid_data("oops");
        if let RespError::InvalidData(msg) = err {
            assert!(msg.starts_with(b"ERR "));
        } else {
            panic!("expected InvalidData");
        }
    }

    #[test]
    fn result_into_response() {
        let ok: Result<Response, RespError> = Ok(Value::Integer(1));
        assert_eq!(ok.into_response(), Value::Integer(1));
        let err: Result<Response, RespError> = Err(RespError::Internal);
        assert_eq!(
            err.into_response(),
            Value::Error(Bytes::from_static(b"ERR internal error"))
        );
    }
}
