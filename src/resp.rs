use std::convert::{From, TryFrom};
use std::{mem, str};

use bytes::{BufMut, BytesMut};

use error::{invalid_data, to_error, Error, Result};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    String(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Value>),
}

fn inconvertible<A>(from: &Value, target: &str) -> Result<A> {
    invalid_data(format!("'{:?}' is not convertible to '{}'", from, target))
}

impl TryFrom<Value> for String {
    type Error = Error;

    fn try_from(val: Value) -> Result<String> {
        match val {
            Value::Bulk(b) => Ok(String::from_utf8(b).map_err(to_error)?),
            Value::Integer(i) => Ok(i.to_string()),
            Value::String(s) => Ok(s),
            _ => inconvertible(&val, "String"),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Error;

    fn try_from(val: Value) -> Result<Vec<u8>> {
        if let Value::Bulk(b) = val {
            Ok(b)
        } else {
            inconvertible(&val, "Vec<u8>")
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(val: Value) -> Result<i64> {
        if let Value::Integer(i) = val {
            Ok(i)
        } else {
            inconvertible(&val, "i64")
        }
    }
}

impl TryFrom<Value> for Option<Vec<u8>> {
    type Error = Error;

    fn try_from(val: Value) -> Result<Option<Vec<u8>>> {
        match val {
            Value::Bulk(b) => Ok(Some(b)),
            Value::Null => Ok(None),
            _ => inconvertible(&val, "Option<Vec<u8>>"),
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

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bulk(b)
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(b: Option<Vec<u8>>) -> Self {
        match b {
            Some(b) => Value::Bulk(b),
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

    fn write_header_format(buf: &mut [u8], ty: u8, number: i64) -> &[u8] {
        let s = format!("{}{}\r\n", char::from(ty), number);
        let s = s.as_bytes();
        let buf = &mut buf[0..s.len()];
        buf.copy_from_slice(s);
        buf
    }

    fn write_header_fast(buf: &mut [u8], ty: u8, number: i64) -> &[u8] {
        let len = buf.len();
        buf[len - 1] = b'\n';
        buf[len - 2] = b'\r';
        let mut i = len - 3;
        let mut number = number;
        loop {
            buf[i] = b'0' + (number % 10) as u8;
            i -= 1;
            number /= 10;
            if number == 0 {
                break;
            }
        }
        buf[i] = ty;
        &buf[i..]
    }

    fn write_header(buf: &mut BytesMut, ty: u8, number: i64) {
        let mut hdr = [0u8; 32];
        let hdr = if number < 0 {
            Self::write_header_format(&mut hdr, ty, number)
        } else {
            Self::write_header_fast(&mut hdr, ty, number)
        };
        Self::ensure_capacity(buf, hdr.len());
        buf.put(hdr)
    }

    fn write_bulk(buf: &mut BytesMut, ty: u8, bytes: &[u8]) {
        Self::ensure_capacity(buf, bytes.len() + 32);
        Self::write_header(buf, ty, bytes.len() as i64);
        buf.put(bytes);
        Self::write_crlf(buf);
    }

    fn write_string(buf: &mut BytesMut, ty: u8, str: &str) {
        Self::ensure_capacity(buf, str.len() + 3);
        buf.put_u8(ty);
        buf.put(str.as_bytes());
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
            Value::Bulk(b) => Self::write_bulk(buf, b'$', &b),
            Value::String(s) => Self::write_string(buf, b'+', &s),
            Value::Error(e) => Self::write_string(buf, b'-', &e),
        }
    }
}

const RESP_TYPE_BULK_STRING: u8 = b'$';
const RESP_TYPE_ARRAY: u8 = b'*';
const RESP_TYPE_INTEGER: u8 = b':';
const RESP_TYPE_SIMPLE_STRING: u8 = b'+';
const RESP_TYPE_ERROR: u8 = b'-';

fn split_input(input: &mut BytesMut, at: usize) -> BytesMut {
    let mut buf = input.split_to(at);
    let len = buf.len();
    buf.truncate(len - 2);
    buf
}

#[derive(Debug, Default)]
struct StringDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl StringDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> Result<String> {
        Ok(str::from_utf8(&split_input(input, self.inspect))
            .map_err(to_error)?
            .into())
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<String>> {
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
                    return invalid_data(format!("Invalid last tailing line feed: '{}'", b))
                }
            }
        }
    }
}

#[derive(Debug)]
struct BulkDecoder {
    length_decoder: Option<IntegerDecoder>,
    length: i64,
}

impl BulkDecoder {
    fn try_decode_length(&mut self, input: &mut BytesMut) -> Result<bool> {
        if let Some(length) = self.length_decoder.as_mut().unwrap().try_decode(input)? {
            self.length_decoder = None;
            self.length = length;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_bulk(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        if self.length < 0 {
            return if self.length == -1 {
                Ok(Some(Value::Null))
            } else {
                invalid_data(format!("Invalid bulk length: '{}'", self.length))
            };
        }
        let length = self.length as usize;
        let len = input.len();
        if len < length + 2 {
            return Ok(None);
        }
        if input[length] != b'\r' || input[length + 1] != b'\n' {
            return invalid_data(format!(
                "Invalid bulk tailing bytes: '[{}, {}]'",
                input[length],
                input[length + 1]
            ));
        }
        let mut bulk = input.split_to(length + 2).to_vec();
        bulk.truncate(length);
        Ok(Some(Value::Bulk(bulk)))
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        if self.length_decoder.is_some() && !self.try_decode_length(input)? {
            return Ok(None);
        }
        self.try_decode_bulk(input)
    }
}

impl Default for BulkDecoder {
    fn default() -> Self {
        BulkDecoder {
            length_decoder: Some(IntegerDecoder::default()),
            length: 0,
        }
    }
}

#[derive(Debug)]
struct ArrayDecoder {
    size_decoder: Option<IntegerDecoder>,
    value_decoder: Option<Box<ValueDecoder>>,
    array: Option<Vec<Value>>,
    size: usize,
}

impl ArrayDecoder {
    fn try_decode_size(&mut self, input: &mut BytesMut) -> Result<bool> {
        if let Some(size) = self.size_decoder.as_mut().unwrap().try_decode(input)? {
            self.size_decoder = None;
            if size < 0 {
                return invalid_data(format!("Invalid array size '{}'", size));
            }
            self.size = size as usize;
            self.array = Some(Vec::with_capacity(self.size));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_element(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        if self.size == 0 {
            return Ok(Some(Value::Array(Vec::new())));
        }
        while !input.is_empty() {
            if self.value_decoder.is_none() {
                self.value_decoder = Some(Box::new(ValueDecoder::new(input)?))
            }
            let decoded = {
                let decoder = self.value_decoder.as_mut().unwrap();
                if let Some(value) = decoder.try_decode(input)? {
                    self.array.as_mut().unwrap().push(value);
                    true
                } else {
                    false
                }
            };
            if decoded {
                self.value_decoder = None;
                if self.array.as_ref().unwrap().len() == self.size {
                    return Ok(mem::replace(&mut self.array, None).map(Value::Array));
                }
            } else {
                break;
            }
        }
        Ok(None)
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        if self.size_decoder.is_some() && !self.try_decode_size(input)? {
            return Ok(None);
        }
        self.try_decode_element(input)
    }
}

impl Default for ArrayDecoder {
    fn default() -> Self {
        ArrayDecoder {
            size_decoder: Some(IntegerDecoder::default()),
            array: None,
            value_decoder: None,
            size: 0,
        }
    }
}

#[derive(Debug, Default)]
struct IntegerDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl IntegerDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> Result<i64> {
        Ok(str::from_utf8(&split_input(input, self.inspect))
            .map_err(to_error)?
            .parse()
            .map_err(to_error)?)
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<i64>> {
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
                    return invalid_data(format!("Invalid byte '{}' when decoding integer", b))
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
    fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        match self {
            TypedDecoder::String(decoder) => {
                decoder.try_decode(input).map(|x| x.map(Value::String))
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

#[derive(Debug)]
pub struct ValueDecoder {
    decoder: TypedDecoder,
}

impl ValueDecoder {
    pub fn new(input: &mut BytesMut) -> Result<Self> {
        let value_type = input[0];
        let decoder = match value_type {
            RESP_TYPE_BULK_STRING => TypedDecoder::Bulk(BulkDecoder::default()),
            RESP_TYPE_ARRAY => TypedDecoder::Array(ArrayDecoder::default()),
            RESP_TYPE_INTEGER => TypedDecoder::Integer(IntegerDecoder::default()),
            RESP_TYPE_SIMPLE_STRING => TypedDecoder::String(StringDecoder::default()),
            RESP_TYPE_ERROR => TypedDecoder::Error(StringDecoder::default()),
            t => return invalid_data(format!("Invalid value type '{}'", t)),
        };
        input.split_to(1);
        Ok(ValueDecoder { decoder })
    }

    pub fn try_decode(&mut self, input: &mut BytesMut) -> Result<Option<Value>> {
        if input.is_empty() {
            Ok(None)
        } else {
            self.decoder.try_decode(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_decode_partially(input: &BytesMut) {
        let len = input.len();
        for i in 1..len {
            let mut s = input[0..i].into();
            let decoder = ValueDecoder::new(&mut s);
            assert!(decoder.is_ok());
            let mut decoder = decoder.unwrap();
            assert_eq!(s.len(), i - 1);
            let v = decoder.try_decode(&mut s);
            assert!(v.is_ok());
            let v = v.unwrap();
            assert!(v.is_none());
        }
    }

    fn test_decode(mut input: BytesMut, expect: Value) {
        test_decode_partially(&input);
        let mut decoder = ValueDecoder::new(&mut input).unwrap();
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
        test_codec(b"+OK\r\n"[..].into(), Value::String("OK".into()));
    }

    #[test]
    fn test_error() {
        test_codec(
            b"-Error message\r\n"[..].into(),
            Value::Error("Error message".into()),
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
            Value::Bulk(b"foobar"[..].into()),
        );
        test_codec(b"$0\r\n\r\n"[..].into(), Value::Bulk(b""[..].into()));
        test_codec(b"$-1\r\n"[..].into(), Value::Null);
    }

    #[test]
    fn test_array() {
        test_codec(b"*0\r\n"[..].into(), Value::Array(vec![]));
        test_codec(
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..].into(),
            Value::Array(vec![
                Value::Bulk(b"foo"[..].into()),
                Value::Bulk(b"bar"[..].into()),
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
                Value::Bulk(b"foobar"[..].into()),
            ]),
        );
    }
}
