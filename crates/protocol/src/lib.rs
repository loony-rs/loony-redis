use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;

/// A RESP2 value that can be sent or received over the wire.
#[derive(Debug, Clone)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    /// Bulk string — `None` encodes as a null bulk string (`$-1\r\n`).
    Bulk(Option<Bytes>),
    /// Array — `None` encodes as a null array (`*-1\r\n`).
    Array(Option<Vec<Frame>>),
}

impl Frame {
    pub fn ok() -> Frame {
        Frame::SimpleString("OK".into())
    }

    pub fn pong() -> Frame {
        Frame::SimpleString("PONG".into())
    }

    pub fn null_bulk() -> Frame {
        Frame::Bulk(None)
    }

    pub fn error(msg: impl Into<String>) -> Frame {
        Frame::Error(msg.into())
    }

    pub fn integer(n: i64) -> Frame {
        Frame::Integer(n)
    }

    pub fn bulk_bytes(b: Bytes) -> Frame {
        Frame::Bulk(Some(b))
    }

    pub fn bulk_str(s: impl Into<String>) -> Frame {
        Frame::Bulk(Some(Bytes::from(s.into().into_bytes())))
    }

    pub fn array(items: Vec<Frame>) -> Frame {
        Frame::Array(Some(items))
    }

    pub fn empty_array() -> Frame {
        Frame::Array(Some(vec![]))
    }
}

// ── Parser ─────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum ParseError {
    Incomplete,
    Invalid(String),
}

/// Try to decode one RESP frame from `buf`.
/// Returns `(frame, bytes_consumed)` or `None` if the buffer is incomplete.
pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    let mut cursor = Cursor::new(buf);
    match parse_value(&mut cursor) {
        Ok(frame) => Ok(Some((frame, cursor.position() as usize))),
        Err(ParseError::Incomplete) => Ok(None),
        Err(ParseError::Invalid(msg)) => Err(msg),
    }
}

fn parse_value<'a>(cur: &mut Cursor<&'a [u8]>) -> Result<Frame, ParseError> {
    if !cur.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    let first = cur.chunk()[0];
    cur.advance(1);

    match first {
        b'+' => {
            let line = read_line(cur)?;
            Ok(Frame::SimpleString(String::from_utf8_lossy(line).into_owned()))
        }
        b'-' => {
            let line = read_line(cur)?;
            Ok(Frame::Error(String::from_utf8_lossy(line).into_owned()))
        }
        b':' => {
            let line = read_line(cur)?;
            parse_int(line)
                .map(Frame::Integer)
                .ok_or_else(|| ParseError::Invalid(format!("invalid integer: {:?}", line)))
        }
        b'$' => {
            let line = read_line(cur)?;
            let len: i64 = parse_int(line)
                .ok_or_else(|| ParseError::Invalid(format!("invalid bulk length: {:?}", line)))?;
            if len == -1 {
                return Ok(Frame::Bulk(None));
            }
            if len < 0 {
                return Err(ParseError::Invalid(format!("invalid bulk length: {len}")));
            }
            let len = len as usize;
            if cur.remaining() < len + 2 {
                return Err(ParseError::Incomplete);
            }
            // Copy bulk body into an owned Bytes — the caller's BytesMut buffer is
            // split_to'd immediately after, so a reference into it would be unsound.
            let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
            cur.advance(len + 2);
            Ok(Frame::Bulk(Some(data)))
        }
        b'*' => {
            let line = read_line(cur)?;
            let count: i64 = parse_int(line)
                .ok_or_else(|| ParseError::Invalid(format!("invalid array count: {:?}", line)))?;
            if count == -1 {
                return Ok(Frame::Array(None));
            }
            if count < 0 {
                return Err(ParseError::Invalid(format!("invalid array count: {count}")));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(parse_value(cur)?);
            }
            Ok(Frame::Array(Some(items)))
        }
        _ => {
            // Inline command — back up one byte and consume until \r\n or \n.
            let pos = cur.position();
            cur.set_position(pos - 1);
            let line = read_line(cur)?;
            let s = std::str::from_utf8(line).unwrap_or("");
            let parts: Vec<Frame> = s
                .split_whitespace()
                .map(|p| Frame::Bulk(Some(Bytes::copy_from_slice(p.as_bytes()))))
                .collect();
            Ok(Frame::Array(Some(parts)))
        }
    }
}

/// Read bytes up to (and consuming) the next `\r\n`.
/// Returns a zero-copy slice of the input buffer — no allocation.
fn read_line<'a>(cur: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    let start = cur.position() as usize;
    // SAFETY: get_ref() returns &&'a [u8]; deref gives &'a [u8].
    let data: &'a [u8] = *cur.get_ref();
    let remaining = &data[start..];

    let limit = remaining.len().saturating_sub(1);
    for i in 0..limit {
        if remaining[i] == b'\r' && remaining[i + 1] == b'\n' {
            // Advance cursor past the line and \r\n.
            cur.set_position((start + i + 2) as u64);
            return Ok(&remaining[..i]);
        }
    }
    Err(ParseError::Incomplete)
}

/// Parse an ASCII decimal integer from bytes without allocating.
#[inline]
fn parse_int(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

// ── Serializer ─────────────────────────────────────────────────────────────

/// Serialise `frame` into a fresh `Bytes`.  Pre-sizes the buffer to avoid
/// reallocation for the common small-response cases.
pub fn serialize_frame(frame: &Frame) -> Bytes {
    let cap = capacity_hint(frame);
    let mut buf = BytesMut::with_capacity(cap);
    write_frame(&mut buf, frame);
    buf.freeze()
}

/// Write `frame` directly into `buf` — used for pipeline response batching so
/// multiple responses can be flushed to the socket in a single syscall.
pub fn write_frame_into(buf: &mut BytesMut, frame: &Frame) {
    write_frame(buf, frame);
}

fn capacity_hint(frame: &Frame) -> usize {
    match frame {
        Frame::SimpleString(s) => s.len() + 3,
        Frame::Error(s)        => s.len() + 3,
        Frame::Integer(_)      => 24,
        Frame::Bulk(None)      => 5,
        Frame::Bulk(Some(b))   => b.len() + 16,
        Frame::Array(None)     => 5,
        Frame::Array(Some(v))  => 8 + v.iter().map(capacity_hint).sum::<usize>(),
    }
}

fn write_frame(buf: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Bulk(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        Frame::Bulk(Some(data)) => {
            buf.put_u8(b'$');
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(data.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        Frame::Array(None) => {
            buf.extend_from_slice(b"*-1\r\n");
        }
        Frame::Array(Some(items)) => {
            buf.put_u8(b'*');
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(items.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for item in items {
                write_frame(buf, item);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(data: &[u8]) -> Frame {
        let (frame, consumed) = parse_frame(data).unwrap().unwrap();
        assert_eq!(consumed, data.len());
        frame
    }

    #[test]
    fn test_parse_simple_string() {
        let f = parse_one(b"+OK\r\n");
        assert!(matches!(f, Frame::SimpleString(s) if s == "OK"));
    }

    #[test]
    fn test_parse_error() {
        let f = parse_one(b"-ERR bad\r\n");
        assert!(matches!(f, Frame::Error(s) if s == "ERR bad"));
    }

    #[test]
    fn test_parse_integer() {
        let f = parse_one(b":42\r\n");
        assert!(matches!(f, Frame::Integer(42)));
    }

    #[test]
    fn test_parse_bulk_string() {
        let f = parse_one(b"$5\r\nhello\r\n");
        assert!(matches!(f, Frame::Bulk(Some(b)) if b == "hello"));
    }

    #[test]
    fn test_parse_null_bulk() {
        let f = parse_one(b"$-1\r\n");
        assert!(matches!(f, Frame::Bulk(None)));
    }

    #[test]
    fn test_parse_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let f = parse_one(data);
        if let Frame::Array(Some(items)) = f {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_incomplete_returns_none() {
        assert!(parse_frame(b"$5\r\nhel").unwrap().is_none());
        assert!(parse_frame(b"*2\r\n$3\r\nGET\r\n").unwrap().is_none());
    }

    #[test]
    fn test_inline_command() {
        let f = parse_one(b"PING\r\n");
        if let Frame::Array(Some(items)) = f {
            assert_eq!(items.len(), 1);
            assert!(matches!(&items[0], Frame::Bulk(Some(b)) if b.as_ref() == b"PING"));
        } else {
            panic!("expected array from inline parse");
        }
    }

    #[test]
    fn test_serialize_round_trip() {
        let frame = Frame::array(vec![
            Frame::bulk_str("SET"),
            Frame::bulk_str("key"),
            Frame::bulk_str("value"),
        ]);
        let bytes = serialize_frame(&frame);
        let (parsed, _) = parse_frame(&bytes).unwrap().unwrap();
        if let Frame::Array(Some(items)) = parsed {
            assert_eq!(items.len(), 3);
        } else {
            panic!("round-trip failed");
        }
    }

    #[test]
    fn test_serialize_null_bulk() {
        let bytes = serialize_frame(&Frame::null_bulk());
        assert_eq!(&bytes[..], b"$-1\r\n");
    }

    #[test]
    fn test_multi_frame_buffer() {
        let data = b"+OK\r\n+PONG\r\n";
        let (_, consumed) = parse_frame(data).unwrap().unwrap();
        assert_eq!(consumed, 5);
        let (f2, _) = parse_frame(&data[consumed..]).unwrap().unwrap();
        assert!(matches!(f2, Frame::SimpleString(s) if s == "PONG"));
    }
}
