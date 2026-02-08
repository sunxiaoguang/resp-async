use std::convert::{From, TryFrom};
use std::str;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::response::RespError;

/// Limits for decoding RESP frames.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DecodeLimits {
    pub max_bulk_len: usize,
    pub max_array_len: usize,
    pub max_depth: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_bulk_len: 1 << 20,
            max_array_len: 1024,
            max_depth: 16,
        }
    }
}

/// RESP frame value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Value>),
}

impl Value {
    pub fn encode(&self, buf: &mut BytesMut) {
        ValueEncoder::encode(buf, self);
    }

    pub fn as_integer(&self) -> Result<i64, RespError> {
        match self {
            Value::Integer(i) => Ok(*i),
            Value::Simple(s) | Value::Bulk(s) => parse_int(s),
            _ => inconvertible(self, "Integer"),
        }
    }

    pub fn to_integer(self) -> Result<i64, RespError> {
        self.as_integer()
    }

    pub fn as_float(&self) -> Result<f64, RespError> {
        match self {
            Value::Integer(i) => Ok(*i as f64),
            Value::Simple(s) | Value::Bulk(s) => parse_float(s),
            _ => inconvertible(self, "Float"),
        }
    }

    pub fn to_float(self) -> Result<f64, RespError> {
        self.as_float()
    }

    pub fn as_str(&self) -> Result<&str, RespError> {
        match self {
            Value::Bulk(v) | Value::Simple(v) => str::from_utf8(v)
                .map_err(|err| RespError::invalid_data(format!("invalid utf-8: {}", err))),
            _ => inconvertible(self, "&str"),
        }
    }

    pub fn to_string(self) -> Result<String, RespError> {
        match self {
            Value::Bulk(b) | Value::Simple(b) => Ok(String::from_utf8(b.to_vec())
                .map_err(|err| RespError::invalid_data(format!("invalid utf-8: {}", err)))?),
            Value::Integer(i) => Ok(i.to_string()),
            Value::Null => Ok(String::new()),
            _ => inconvertible(&self, "String"),
        }
    }

    pub fn as_slice(&self) -> Result<&[u8], RespError> {
        match self {
            Value::Bulk(v) | Value::Simple(v) => Ok(v.as_ref()),
            _ => inconvertible(self, "&[u8]"),
        }
    }

    pub fn to_bytes(self) -> Result<Vec<u8>, RespError> {
        match self {
            Value::Bulk(b) | Value::Simple(b) => Ok(b.to_vec()),
            Value::Integer(i) => Ok(i.to_string().into_bytes()),
            Value::Null => Ok(Vec::new()),
            _ => inconvertible(&self, "Vec<u8>"),
        }
    }
}

fn parse_int(bytes: &Bytes) -> Result<i64, RespError> {
    str::from_utf8(bytes)
        .map_err(|err| RespError::invalid_data(format!("invalid utf-8: {}", err)))?
        .parse()
        .map_err(|err| RespError::invalid_data(format!("invalid integer: {}", err)))
}

fn parse_float(bytes: &Bytes) -> Result<f64, RespError> {
    str::from_utf8(bytes)
        .map_err(|err| RespError::invalid_data(format!("invalid utf-8: {}", err)))?
        .parse()
        .map_err(|err| RespError::invalid_data(format!("invalid float: {}", err)))
}

fn inconvertible<A>(from: &Value, target: &str) -> Result<A, RespError> {
    Err(RespError::invalid_data(format!(
        "'{:?}' is not convertible to '{}'",
        from, target
    )))
}

impl TryFrom<&Value> for String {
    type Error = RespError;
    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        val.as_str().map(ToOwned::to_owned)
    }
}

impl TryFrom<Value> for String {
    type Error = RespError;
    fn try_from(val: Value) -> Result<Self, Self::Error> {
        val.to_string()
    }
}

impl TryFrom<&Value> for Vec<u8> {
    type Error = RespError;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        val.as_slice().map(ToOwned::to_owned)
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = RespError;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        val.to_bytes()
    }
}

impl TryFrom<&Value> for Vec<String> {
    type Error = RespError;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        if let Value::Array(array) = val {
            array.iter().map(TryInto::try_into).collect()
        } else {
            inconvertible(val, "Vec<String>")
        }
    }
}

impl TryFrom<Value> for Vec<String> {
    type Error = RespError;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        if let Value::Array(array) = val {
            array.into_iter().map(Value::to_string).collect()
        } else {
            inconvertible(&val, "Vec<String>")
        }
    }
}

impl From<Value> for Vec<Value> {
    fn from(val: Value) -> Self {
        if let Value::Array(array) = val {
            array
        } else {
            Vec::from(val)
        }
    }
}

impl From<&Value> for Vec<Value> {
    fn from(val: &Value) -> Self {
        if let Value::Array(array) = val {
            array.to_vec()
        } else {
            Vec::from(val)
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = RespError;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        val.as_integer()
    }
}

impl TryFrom<&Value> for i64 {
    type Error = RespError;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        val.as_integer()
    }
}

impl TryFrom<Value> for f64 {
    type Error = RespError;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        val.as_float()
    }
}

impl TryFrom<&Value> for f64 {
    type Error = RespError;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        val.as_float()
    }
}

impl TryFrom<Value> for Option<Vec<u8>> {
    type Error = RespError;

    fn try_from(val: Value) -> Result<Self, Self::Error> {
        if let Value::Null = val {
            Ok(None)
        } else {
            val.to_bytes().map(Some)
        }
    }
}

impl TryFrom<&Value> for Option<Vec<u8>> {
    type Error = RespError;

    fn try_from(val: &Value) -> Result<Self, Self::Error> {
        if let Value::Null = val {
            Ok(None)
        } else {
            val.as_slice().map(ToOwned::to_owned).map(Some)
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i64::from(i))
    }
}

impl From<usize> for Value {
    fn from(i: usize) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<u32> for Value {
    fn from(i: u32) -> Self {
        Value::Integer(i64::from(i))
    }
}

impl From<u64> for Value {
    fn from(i: u64) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<Bytes> for Value {
    fn from(b: Bytes) -> Self {
        Value::Bulk(b)
    }
}

impl From<&'static [u8]> for Value {
    fn from(b: &'static [u8]) -> Self {
        Value::Bulk(Bytes::from_static(b))
    }
}

impl From<&'static str> for Value {
    fn from(s: &'static str) -> Self {
        Value::Bulk(Bytes::from_static(s.as_bytes()))
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bulk(Bytes::from(b))
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(b: Option<Vec<u8>>) -> Self {
        match b {
            Some(b) => Value::Bulk(Bytes::from(b)),
            None => Value::Null,
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(a: Vec<T>) -> Self {
        Value::Array(a.into_iter().map(Into::into).collect())
    }
}

/// Encoder for RESP values.
pub struct ValueEncoder;

impl ValueEncoder {
    #[inline]
    fn ensure_capacity(buf: &mut BytesMut, size: usize) {
        if buf.remaining_mut() < size {
            buf.reserve(size)
        }
    }

    #[inline]
    fn write_crlf(buf: &mut BytesMut) {
        buf.put_slice(b"\r\n");
    }

    fn write_header(buf: &mut BytesMut, ty: u8, number: i64) {
        let mut hdr = [0u8; 32];
        let mut idx = hdr.len();
        hdr[idx - 1] = b'\n';
        hdr[idx - 2] = b'\r';
        idx -= 2;

        let negative = number < 0;
        let mut n = if negative {
            number.wrapping_neg() as u64
        } else {
            number as u64
        };
        loop {
            idx -= 1;
            hdr[idx] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        if negative {
            idx -= 1;
            hdr[idx] = b'-';
        }
        idx -= 1;
        hdr[idx] = ty;
        let hdr = &hdr[idx..];
        Self::ensure_capacity(buf, hdr.len());
        buf.put_slice(hdr);
    }

    fn write_bulk(buf: &mut BytesMut, ty: u8, bytes: &Bytes) {
        Self::ensure_capacity(buf, bytes.len() + 32);
        Self::write_header(buf, ty, bytes.len() as i64);
        buf.put(bytes.as_ref());
        Self::write_crlf(buf);
    }

    fn write_simple(buf: &mut BytesMut, ty: u8, bytes: &Bytes) {
        Self::ensure_capacity(buf, bytes.len() + 3);
        buf.put_u8(ty);
        buf.put(bytes.as_ref());
        Self::write_crlf(buf);
    }

    pub fn encode(buf: &mut BytesMut, value: &Value) {
        match value {
            Value::Null => Self::write_header(buf, b'$', -1),
            Value::Array(a) => {
                Self::write_header(buf, b'*', a.len() as i64);
                for e in a {
                    Self::encode(buf, e);
                }
            }
            Value::Integer(i) => Self::write_header(buf, b':', *i),
            Value::Bulk(b) => Self::write_bulk(buf, b'$', b),
            Value::Simple(s) => Self::write_simple(buf, b'+', s),
            Value::Error(e) => Self::write_simple(buf, b'-', e),
        }
    }
}

const RESP_TYPE_BULK_STRING: u8 = b'$';
const RESP_TYPE_ARRAY: u8 = b'*';
const RESP_TYPE_INTEGER: u8 = b':';
const RESP_TYPE_SIMPLE_STRING: u8 = b'+';
const RESP_TYPE_ERROR: u8 = b'-';

fn split_input(input: &mut BytesMut, at: usize) -> Bytes {
    let mut buf = input.split_to(at);
    let len = buf.len();
    buf.truncate(len - 2);
    buf.freeze()
}

#[derive(Debug, Default)]
struct StringDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl StringDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> Result<Bytes, RespError> {
        Ok(split_input(input, self.inspect))
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Bytes>, RespError> {
        let length = input.len();
        loop {
            if length <= self.inspect {
                return Ok(None);
            }
            let inspect = self.inspect;
            self.inspect += 1;
            match (self.expect_lf, input[inspect]) {
                (false, b'\r') => self.expect_lf = true,
                (false, _) => (),
                (true, b'\n') => return self.decode(input).map(Some),
                (true, b) => {
                    return Err(RespError::invalid_data(format!(
                        "Invalid last tailing line feed: '{}'",
                        b
                    )));
                }
            }
        }
    }
}

#[derive(Debug)]
struct BulkDecoder {
    length_decoder: Option<IntegerDecoder>,
    length: i64,
    max_bulk_len: usize,
}

impl BulkDecoder {
    fn new(limits: DecodeLimits) -> Self {
        BulkDecoder {
            length_decoder: Some(IntegerDecoder::default()),
            length: 0,
            max_bulk_len: limits.max_bulk_len,
        }
    }

    fn try_decode_length(&mut self, input: &mut BytesMut) -> Result<bool, RespError> {
        if let Some(length) = self.length_decoder.as_mut().unwrap().try_decode(input)? {
            self.length_decoder = None;
            self.length = length;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_bulk(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        if self.length < 0 {
            return if self.length == -1 {
                Ok(Some(Value::Null))
            } else {
                Err(RespError::invalid_data(format!(
                    "Invalid bulk length: '{}'",
                    self.length
                )))
            };
        }
        let length = self.length as usize;
        if length > self.max_bulk_len {
            return Err(RespError::invalid_data(format!(
                "Invalid bulk length: '{}' (limit {})",
                length, self.max_bulk_len
            )));
        }
        let len = input.len();
        if len < length + 2 {
            return Ok(None);
        }
        if input[length] != b'\r' || input[length + 1] != b'\n' {
            return Err(RespError::invalid_data(format!(
                "Invalid bulk tailing bytes: '[{}, {}]'",
                input[length],
                input[length + 1]
            )));
        }
        let mut bulk = input.split_to(length + 2);
        bulk.truncate(length);
        Ok(Some(Value::Bulk(bulk.freeze())))
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        if self.length_decoder.is_some() && !self.try_decode_length(input)? {
            return Ok(None);
        }
        self.try_decode_bulk(input)
    }
}

#[derive(Debug)]
struct ArrayDecoder {
    size_decoder: Option<IntegerDecoder>,
    value_decoder: Box<ValueDecoder>,
    array: Option<Vec<Value>>,
    size: usize,
    max_array_len: usize,
}

impl ArrayDecoder {
    fn new(limits: DecodeLimits, depth: usize) -> Self {
        ArrayDecoder {
            size_decoder: Some(IntegerDecoder::default()),
            array: None,
            value_decoder: Box::new(ValueDecoder::with_depth(limits, depth)),
            size: 0,
            max_array_len: limits.max_array_len,
        }
    }

    fn try_decode_size(&mut self, input: &mut BytesMut) -> Result<bool, RespError> {
        if let Some(size) = self.size_decoder.as_mut().unwrap().try_decode(input)? {
            self.size_decoder = None;
            if size < 0 {
                return Err(RespError::invalid_data(format!(
                    "Invalid array size '{}'",
                    size
                )));
            }
            self.size = size as usize;
            if self.size > self.max_array_len {
                return Err(RespError::invalid_data(format!(
                    "Invalid array size '{}' (limit {})",
                    self.size, self.max_array_len
                )));
            }
            self.array = Some(Vec::with_capacity(self.size));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_element(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        if self.size == 0 {
            return Ok(Some(Value::Array(Vec::new())));
        }
        while !input.is_empty() {
            if let Some(value) = self.value_decoder.try_decode(input)? {
                self.array.as_mut().unwrap().push(value);
                if self.array.as_ref().unwrap().len() == self.size {
                    return Ok(self.array.take().map(Value::Array));
                }
            } else {
                break;
            }
        }
        Ok(None)
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        if self.size_decoder.is_some() && !self.try_decode_size(input)? {
            return Ok(None);
        }
        self.try_decode_element(input)
    }
}

#[derive(Debug, Default)]
struct IntegerDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl IntegerDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> Result<i64, RespError> {
        let bytes = split_input(input, self.inspect);
        str::from_utf8(&bytes)
            .map_err(|err| RespError::invalid_data(format!("invalid utf-8: {}", err)))?
            .parse()
            .map_err(|err| RespError::invalid_data(format!("invalid integer: {}", err)))
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<i64>, RespError> {
        let length = input.len();
        loop {
            if length <= self.inspect {
                return Ok(None);
            }
            let inspect = self.inspect;
            self.inspect += 1;
            match (self.expect_lf, input[inspect]) {
                (false, b'0'..=b'9') => (),
                (false, b'-') => (),
                (false, b'\r') => self.expect_lf = true,
                (true, b'\n') => return self.decode(input).map(Some),
                (_, b) => {
                    return Err(RespError::invalid_data(format!(
                        "Invalid byte '{}' when decoding integer",
                        b
                    )));
                }
            }
        }
    }
}

#[derive(Debug)]
enum TypedDecoder {
    String(StringDecoder),
    Error(StringDecoder),
    Integer(IntegerDecoder),
    Bulk(BulkDecoder),
    Array(ArrayDecoder),
}

impl TypedDecoder {
    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        match self {
            TypedDecoder::String(decoder) => {
                decoder.try_decode(input).map(|x| x.map(Value::Simple))
            }
            TypedDecoder::Error(decoder) => decoder.try_decode(input).map(|x| x.map(Value::Error)),
            TypedDecoder::Integer(decoder) => {
                decoder.try_decode(input).map(|x| x.map(Value::Integer))
            }
            TypedDecoder::Bulk(decoder) => decoder.try_decode(input),
            TypedDecoder::Array(decoder) => decoder.try_decode(input),
        }
    }
}

/// Streaming RESP decoder.
#[derive(Debug)]
pub struct ValueDecoder {
    decoder: Option<TypedDecoder>,
    limits: DecodeLimits,
    depth: usize,
}

impl ValueDecoder {
    pub fn new(limits: DecodeLimits) -> Self {
        Self {
            decoder: None,
            limits,
            depth: 0,
        }
    }

    fn with_depth(limits: DecodeLimits, depth: usize) -> Self {
        Self {
            decoder: None,
            limits,
            depth,
        }
    }

    pub fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>, RespError> {
        if input.is_empty() {
            return Ok(None);
        }
        if self.decoder.is_none() {
            let decoder = match input[0] {
                RESP_TYPE_BULK_STRING => TypedDecoder::Bulk(BulkDecoder::new(self.limits)),
                RESP_TYPE_ARRAY => {
                    if self.depth + 1 > self.limits.max_depth {
                        return Err(RespError::invalid_data(format!(
                            "ERR max depth exceeded (limit {})",
                            self.limits.max_depth
                        )));
                    }
                    TypedDecoder::Array(ArrayDecoder::new(self.limits, self.depth + 1))
                }
                RESP_TYPE_INTEGER => TypedDecoder::Integer(IntegerDecoder::default()),
                RESP_TYPE_SIMPLE_STRING => TypedDecoder::String(StringDecoder::default()),
                RESP_TYPE_ERROR => TypedDecoder::Error(StringDecoder::default()),
                t => {
                    return Err(RespError::invalid_data(format!(
                        "Invalid value type '{}'",
                        t
                    )));
                }
            };
            input.advance(1);
            self.decoder = Some(decoder);
        }
        let result = self.decoder.as_mut().unwrap().try_decode(input)?;
        if result.is_some() {
            self.decoder = None;
        }
        Ok(result)
    }
}

impl Default for ValueDecoder {
    fn default() -> Self {
        ValueDecoder::new(DecodeLimits::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_decode_partially(input: &BytesMut) {
        let len = input.len();
        for i in 1..len {
            let mut s = input[0..i].into();
            let v = ValueDecoder::default().try_decode(&mut s);
            assert!(v.is_ok());
            let v = v.unwrap();
            assert!(v.is_none());
        }
    }

    fn test_decode(mut input: BytesMut, expect: Value) {
        test_decode_partially(&input);
        let mut decoder = ValueDecoder::default();
        if let Ok(Some(v)) = decoder.try_decode(&mut input) {
            assert_eq!(v, expect);
        } else {
            assert!(false);
        }
    }

    fn test_encode(expect: &BytesMut, value: &Value) {
        let mut buf = BytesMut::with_capacity(128);
        ValueEncoder::encode(&mut buf, value);
        assert_eq!(&buf, expect);
    }

    fn test_codec(serialized: BytesMut, value: Value) {
        test_encode(&serialized, &value);
        test_decode(serialized, value);
    }

    #[test]
    fn test_simple_string() {
        test_codec(
            b"+OK\r\n"[..].into(),
            Value::Simple(Bytes::from_static(b"OK")),
        );
    }

    #[test]
    fn test_error() {
        test_codec(
            b"-Error message\r\n"[..].into(),
            Value::Error(Bytes::from_static(b"Error message")),
        );
    }

    #[test]
    fn test_integer() {
        test_codec(b":1000\r\n"[..].into(), Value::Integer(1000));
    }

    #[test]
    fn test_bulk_string() {
        test_codec(
            b"$6\r\nfoobar\r\n"[..].into(),
            Value::Bulk(Bytes::from_static(b"foobar")),
        );
        test_codec(
            b"$0\r\n\r\n"[..].into(),
            Value::Bulk(Bytes::from_static(b"")),
        );
        test_codec(b"$-1\r\n"[..].into(), Value::Null);
    }

    #[test]
    fn test_array() {
        test_codec(b"*0\r\n"[..].into(), Value::Array(vec![]));
        test_codec(
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..].into(),
            Value::Array(vec![
                Value::Bulk(Bytes::from_static(b"foo")),
                Value::Bulk(Bytes::from_static(b"bar")),
            ]),
        );
        test_codec(
            b"*3\r\n:1\r\n:2\r\n:3\r\n"[..].into(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        );
        test_codec(
            b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"[..].into(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
                Value::Bulk(Bytes::from_static(b"foobar")),
            ]),
        );
    }

    #[test]
    fn test_bulk_limit() {
        let limits = DecodeLimits {
            max_bulk_len: 3,
            max_array_len: 1024,
            max_depth: 16,
        };
        let mut decoder = ValueDecoder::new(limits);
        let mut input = BytesMut::from(&b"$4\r\ntest\r\n"[..]);
        let err = decoder.try_decode(&mut input).unwrap_err();
        assert!(matches!(err, RespError::InvalidData(_)));
    }

    #[test]
    fn test_array_limit() {
        let limits = DecodeLimits {
            max_bulk_len: 1024,
            max_array_len: 1,
            max_depth: 16,
        };
        let mut decoder = ValueDecoder::new(limits);
        let mut input = BytesMut::from(&b"*2\r\n:1\r\n:2\r\n"[..]);
        let err = decoder.try_decode(&mut input).unwrap_err();
        assert!(matches!(err, RespError::InvalidData(_)));
    }

    #[test]
    fn test_depth_limit() {
        let limits = DecodeLimits {
            max_bulk_len: 1024,
            max_array_len: 1024,
            max_depth: 1,
        };
        let mut decoder = ValueDecoder::new(limits);
        let mut input = BytesMut::from(&b"*1\r\n*1\r\n:1\r\n"[..]);
        let err = decoder.try_decode(&mut input).unwrap_err();
        assert!(matches!(err, RespError::InvalidData(_)));
    }
}
