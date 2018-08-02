use std::{error, io};

pub type Error = io::Error;
pub type Result<T> = io::Result<T>;

pub(crate) fn to_error<E>(e: E) -> Error
where
    E: error::Error + Send + Sync + 'static,
{
    Error::new(io::ErrorKind::InvalidData, e)
}

pub(crate) fn invalid_data<A, T>(msg: T) -> Result<A>
where
    T: Into<Option<String>>,
{
    Err(if let Some(msg) = msg.into() {
        Error::new(io::ErrorKind::InvalidData, msg)
    } else {
        io::ErrorKind::InvalidData.into()
    })
}
