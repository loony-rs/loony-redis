//! Minimal RESP2 TCP server for Phase 2 (see PLAN.md).
//!
//! Deliberately narrow scope: no AOF, no replication, no Raft, no cluster
//! routing -- those are rebuilt in later phases (see docs/architecture.md's
//! "starting point" section). This crate validates the network + dispatch
//! layer against `storage::Store` directly, plus the configurable request
//! limits required by docs/protocol.md, before any of that is layered on
//! top.

use bytes::{Bytes, BytesMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error};

use protocol::{parse_frame, write_frame_into, Frame};
use storage::{Store, Value};

// ── Limits (docs/protocol.md) ───────────────────────────────────────────────

/// Configurable request limits. Defaults are conservative placeholders, not
/// tuned for production -- see docs/performance.md for the benchmarking
/// pass that will inform real defaults later.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub max_command_size: usize,
    pub max_request_size: usize,
    pub max_pipeline_depth: usize,
    pub max_connections: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_key_size: 8 * 1024,
            max_value_size: 64 * 1024 * 1024,
            max_command_size: 64 * 1024 * 1024,
            max_request_size: 64 * 1024 * 1024,
            max_pipeline_depth: 1024,
            max_connections: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: String,
    pub limits: Limits,
}

// ── Server ───────────────────────────────────────────────────────────────

pub struct Server {
    store: Arc<Store>,
    limits: Arc<Limits>,
    conn_count: Arc<AtomicUsize>,
}

impl Server {
    pub fn new(store: Arc<Store>, limits: Limits) -> Self {
        Server {
            store,
            limits: Arc::new(limits),
            conn_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn run(self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve(listener).await
    }

    /// Split out from `run` so tests can bind to an ephemeral port (`:0`)
    /// and learn the real address before serving.
    pub async fn serve(self, listener: TcpListener) -> anyhow::Result<()> {
        tracing::info!("listening on {}", listener.local_addr()?);
        loop {
            let (socket, peer) = listener.accept().await?;

            if self.conn_count.load(Ordering::Relaxed) >= self.limits.max_connections {
                let mut out = BytesMut::new();
                write_frame_into(&mut out, &Frame::error("ERR max number of clients reached"));
                let mut socket = socket;
                let _ = socket.write_all(&out).await;
                continue;
            }

            let _ = socket.set_nodelay(true);
            let store = self.store.clone();
            let limits = self.limits.clone();
            let guard = ConnGuard::new(self.conn_count.clone());

            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = handle_connection(socket, store, limits).await {
                    if !is_conn_reset(&e) {
                        error!("connection {peer} error: {e}");
                    }
                }
                debug!("connection {peer} closed");
            });
        }
    }
}

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        ConnGuard(counter)
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn is_conn_reset(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .map(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            )
        })
        .unwrap_or(false)
}

// ── Connection handling ──────────────────────────────────────────────────

async fn handle_connection(
    mut socket: TcpStream,
    store: Arc<Store>,
    limits: Arc<Limits>,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut out = BytesMut::with_capacity(16 * 1024);

    loop {
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        // Reject before doing any further buffering/parsing once we're
        // already holding more unparsed bytes than the configured cap --
        // this bounds memory even if a client just keeps sending without
        // ever completing a frame (docs/protocol.md).
        if buf.len() > limits.max_request_size {
            write_frame_into(&mut out, &Frame::error("ERR max request size exceeded"));
            socket.write_all(&out).await?;
            return Ok(());
        }

        // Peek declared bulk-string lengths in whatever header bytes have
        // arrived so far, without waiting for the (possibly huge) body to
        // be buffered. This is the "check the length header before reading
        // the body" requirement -- it never allocates the body.
        if let Some(declared) = max_declared_bulk_len(&buf) {
            if declared > limits.max_value_size as i64 {
                write_frame_into(
                    &mut out,
                    &Frame::error("ERR bulk length exceeds configured max_value_size"),
                );
                socket.write_all(&out).await?;
                return Ok(());
            }
        }

        let mut pipeline_count = 0usize;
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    if consumed > limits.max_command_size {
                        let _ = buf.split_to(consumed);
                        write_frame_into(
                            &mut out,
                            &Frame::error("ERR command exceeds configured max_command_size"),
                        );
                        socket.write_all(&out).await?;
                        return Ok(());
                    }
                    let _ = buf.split_to(consumed);

                    pipeline_count += 1;
                    if pipeline_count > limits.max_pipeline_depth {
                        write_frame_into(
                            &mut out,
                            &Frame::error("ERR max pipeline depth exceeded"),
                        );
                        socket.write_all(&out).await?;
                        return Ok(());
                    }

                    let response = dispatch(frame, &store, &limits);
                    write_frame_into(&mut out, &response);
                }
                Ok(None) => break,
                Err(e) => {
                    write_frame_into(&mut out, &Frame::error(format!("ERR protocol error: {e}")));
                    buf.clear();
                    break;
                }
            }
        }

        if !out.is_empty() {
            socket.write_all(&out).await?;
            out.clear();
        }
    }
}

/// Scan as much of `buf` as has arrived so far for RESP bulk-string length
/// headers (the common `*N\r\n$len\r\n...` command shape, or a bare bulk
/// string), and return the largest declared length found. Stops scanning
/// (without error) at the first header that hasn't fully arrived yet --
/// the request-size cap in `handle_connection` is the backstop for that
/// case. Never reads into a body past what's already buffered.
fn max_declared_bulk_len(buf: &[u8]) -> Option<i64> {
    let mut max_len: Option<i64> = None;
    match buf.first() {
        Some(b'*') => {
            let (count, mut pos) = read_len_line(buf, 1)?;
            let count = count.max(0) as usize;
            for _ in 0..count {
                if pos >= buf.len() || buf[pos] != b'$' {
                    return max_len;
                }
                let (len, next) = match read_len_line(buf, pos + 1) {
                    Some(v) => v,
                    None => return max_len,
                };
                max_len = Some(max_len.map_or(len, |m: i64| m.max(len)));
                let body_end = next + len.max(0) as usize + 2;
                if body_end > buf.len() {
                    return max_len;
                }
                pos = body_end;
            }
            max_len
        }
        Some(b'$') => read_len_line(buf, 1).map(|(len, _)| len),
        _ => None,
    }
}

/// Parse a `<digits>\r\n` line starting at `start`, returning the parsed
/// value and the position just past the line. `None` if the line hasn't
/// fully arrived in `buf` yet.
fn read_len_line(buf: &[u8], start: usize) -> Option<(i64, usize)> {
    let rest = buf.get(start..)?;
    let pos = rest.iter().position(|&b| b == b'\r')?;
    if rest.get(pos + 1) != Some(&b'\n') {
        return None;
    }
    let n: i64 = std::str::from_utf8(&rest[..pos])
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some((n, start + pos + 2))
}

// ── Command dispatch ─────────────────────────────────────────────────────

fn dispatch(frame: Frame, store: &Store, limits: &Limits) -> Frame {
    let args = match frame {
        Frame::Array(Some(a)) if !a.is_empty() => a,
        Frame::Array(Some(_)) => return Frame::error("ERR empty command"),
        _ => return Frame::error("ERR invalid command format"),
    };

    let cmd = match bulk_bytes(&args, 0) {
        Some(b) => b.to_ascii_uppercase(),
        None => return Frame::error("ERR invalid command name"),
    };

    match cmd.as_slice() {
        b"PING" => cmd_ping(&args),
        b"GET" => cmd_get(&args, store),
        b"SET" => cmd_set(&args, store, limits),
        b"DEL" => cmd_del(&args, store, limits),
        b"LPUSH" => cmd_lpush(&args, store, limits),
        b"RPUSH" => cmd_rpush(&args, store, limits),
        b"LPOP" => cmd_lpop(&args, store, limits),
        b"HSET" => cmd_hset(&args, store, limits),
        b"HGET" => cmd_hget(&args, store, limits),
        b"SADD" => cmd_sadd(&args, store, limits),
        b"SMEMBERS" => cmd_smembers(&args, store, limits),
        b"EXPIRE" => cmd_expire(&args, store, limits),
        b"TTL" => cmd_ttl(&args, store, limits),
        b"INFO" => cmd_info(),
        other => Frame::error(format!(
            "ERR unknown command `{}`",
            String::from_utf8_lossy(other)
        )),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn bulk_bytes(args: &[Frame], idx: usize) -> Option<Bytes> {
    match args.get(idx)? {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }
}

fn bulk_as_str(args: &[Frame], idx: usize) -> Option<String> {
    bulk_bytes(args, idx).map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn bulk_as_i64(args: &[Frame], idx: usize) -> Option<i64> {
    bulk_bytes(args, idx).and_then(|b| String::from_utf8_lossy(&b).trim().parse().ok())
}

fn wrong_num_args(cmd: &str) -> Frame {
    Frame::error(format!("ERR wrong number of arguments for '{cmd}' command"))
}

fn check_key_size(key: &str, limits: &Limits) -> Result<(), Frame> {
    if key.len() > limits.max_key_size {
        return Err(Frame::error("ERR key exceeds configured max_key_size"));
    }
    Ok(())
}

fn check_value_size(val: &Bytes, limits: &Limits) -> Result<(), Frame> {
    if val.len() > limits.max_value_size {
        return Err(Frame::error("ERR value exceeds configured max_value_size"));
    }
    Ok(())
}

fn from_wrongtype<T>(res: anyhow::Result<T>, ok: impl FnOnce(T) -> Frame) -> Frame {
    match res {
        Ok(v) => ok(v),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Connection ─────────────────────────────────────────────────────────

fn cmd_ping(args: &[Frame]) -> Frame {
    match bulk_bytes(args, 1) {
        Some(msg) => Frame::Bulk(Some(msg)),
        None => Frame::pong(),
    }
}

fn cmd_info() -> Frame {
    Frame::bulk_str("# Server\r\nloony-redis-server:phase2\r\nrole:standalone\r\n")
}

// ── Strings ──────────────────────────────────────────────────────────────

fn cmd_get(args: &[Frame], store: &Store) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("get");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("get"),
    };
    match store.get(&key) {
        Some(Value::String(b)) => Frame::Bulk(Some(b)),
        Some(_) => {
            Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => Frame::null_bulk(),
    }
}

fn cmd_set(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("set");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("set"),
    };
    let val = match bulk_bytes(args, 2) {
        Some(v) => v,
        None => return wrong_num_args("set"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    if let Err(e) = check_value_size(&val, limits) {
        return e;
    }
    store.set(key, Value::String(val), None);
    Frame::ok()
}

// ── Keyspace ───────────────────────────────────────────────────────────

fn cmd_del(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("del");
    }
    let mut keys = Vec::with_capacity(args.len() - 1);
    for f in &args[1..] {
        match f {
            Frame::Bulk(Some(b)) => {
                let key = String::from_utf8_lossy(b).into_owned();
                if let Err(e) = check_key_size(&key, limits) {
                    return e;
                }
                keys.push(key);
            }
            _ => return Frame::error("ERR invalid key"),
        }
    }
    Frame::integer(store.del(&keys) as i64)
}

fn cmd_expire(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("expire");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("expire"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let secs = match bulk_as_i64(args, 2) {
        Some(n) if n >= 0 => n as u64,
        _ => return Frame::error("ERR invalid expire time"),
    };
    // expire_at is computed once, here, as an absolute epoch-ms timestamp --
    // see storage::now_ms's doc comment and docs/invariants.md S5.
    let set = store.expire(&key, storage::now_ms() + secs * 1000);
    Frame::integer(if set { 1 } else { 0 })
}

fn cmd_ttl(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("ttl");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("ttl"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let pttl = store.pttl(&key);
    Frame::integer(if pttl > 0 { pttl / 1000 } else { pttl })
}

// ── Lists ──────────────────────────────────────────────────────────────

fn cmd_lpush(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    push(args, store, limits, "lpush", true)
}

fn cmd_rpush(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    push(args, store, limits, "rpush", false)
}

fn push(args: &[Frame], store: &Store, limits: &Limits, name: &str, left: bool) -> Frame {
    if args.len() < 3 {
        return wrong_num_args(name);
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args(name),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let mut values = Vec::with_capacity(args.len() - 2);
    for f in &args[2..] {
        match f {
            Frame::Bulk(Some(b)) => {
                if let Err(e) = check_value_size(b, limits) {
                    return e;
                }
                values.push(b.clone());
            }
            _ => return Frame::error("ERR invalid value"),
        }
    }
    let res = if left {
        store.lpush(key, values)
    } else {
        store.rpush(key, values)
    };
    from_wrongtype(res, |n| Frame::integer(n as i64))
}

fn cmd_lpop(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() < 2 || args.len() > 3 {
        return wrong_num_args("lpop");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("lpop"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let with_count = args.len() == 3;
    let count = if with_count {
        match bulk_as_i64(args, 2) {
            Some(n) if n >= 0 => n as usize,
            _ => return Frame::error("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };
    match store.lpop(&key, count) {
        Ok(mut items) => {
            if with_count {
                Frame::array(items.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
            } else {
                match items.pop() {
                    Some(b) => Frame::Bulk(Some(b)),
                    None => Frame::null_bulk(),
                }
            }
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Hashes ─────────────────────────────────────────────────────────────

fn cmd_hset(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 4 {
        return wrong_num_args("hset");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("hset"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let field = match bulk_bytes(args, 2) {
        Some(f) => f,
        None => return wrong_num_args("hset"),
    };
    let value = match bulk_bytes(args, 3) {
        Some(v) => v,
        None => return wrong_num_args("hset"),
    };
    if let Err(e) = check_value_size(&value, limits) {
        return e;
    }
    from_wrongtype(store.hset(key, field, value), |n| Frame::integer(n as i64))
}

fn cmd_hget(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("hget");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("hget"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let field = match bulk_bytes(args, 2) {
        Some(f) => f,
        None => return wrong_num_args("hget"),
    };
    match store.hget(&key, &field) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None) => Frame::null_bulk(),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Sets ───────────────────────────────────────────────────────────────

fn cmd_sadd(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("sadd");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("sadd"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    let mut members = Vec::with_capacity(args.len() - 2);
    for f in &args[2..] {
        match f {
            Frame::Bulk(Some(b)) => {
                if let Err(e) = check_value_size(b, limits) {
                    return e;
                }
                members.push(b.clone());
            }
            _ => return Frame::error("ERR invalid member"),
        }
    }
    from_wrongtype(store.sadd(key, members), |n| Frame::integer(n as i64))
}

fn cmd_smembers(args: &[Frame], store: &Store, limits: &Limits) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("smembers");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("smembers"),
    };
    if let Err(e) = check_key_size(&key, limits) {
        return e;
    }
    from_wrongtype(store.smembers(&key), |members| {
        Frame::array(members.into_iter().map(|b| Frame::Bulk(Some(b))).collect())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    async fn start_test_server(limits: Limits) -> (std::net::SocketAddr, Arc<Store>) {
        let store = Arc::new(Store::new());
        let server = Server::new(store.clone(), limits);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(server.serve(listener));
        (addr, store)
    }

    async fn read_response(socket: &mut TcpStream) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    }

    #[tokio::test]
    async fn test_ping_get_set_del() {
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        assert_eq!(read_response(&mut sock).await, b"+PONG\r\n");

        sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"+OK\r\n");

        sock.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"$1\r\nv\r\n");

        sock.write_all(b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b":1\r\n");
    }

    #[tokio::test]
    async fn test_lists_hashes_sets() {
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*3\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\na\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b":1\r\n");

        sock.write_all(b"*2\r\n$4\r\nLPOP\r\n$1\r\nl\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"$1\r\na\r\n");

        sock.write_all(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b":1\r\n");

        sock.write_all(b"*3\r\n$4\r\nHGET\r\n$1\r\nh\r\n$1\r\nf\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"$1\r\nv\r\n");

        sock.write_all(b"*3\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\nx\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b":1\r\n");

        sock.write_all(b"*2\r\n$8\r\nSMEMBERS\r\n$1\r\ns\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"*1\r\n$1\r\nx\r\n");
    }

    #[tokio::test]
    async fn test_expire_ttl() {
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        let _ = read_response(&mut sock).await;

        sock.write_all(b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n10\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b":1\r\n");

        sock.write_all(b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with(':'), "expected integer reply, got {s}");
        let ttl: i64 = s.trim_start_matches(':').trim_end().parse().unwrap();
        assert!((0..=10).contains(&ttl), "ttl {ttl} out of expected range");
    }

    #[tokio::test]
    async fn test_fragmented_packet() {
        // Send the same SET command byte-by-byte across many small writes --
        // the parser must still assemble exactly one command.
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        let full = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        for chunk in full.chunks(3) {
            sock.write_all(chunk).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(read_response(&mut sock).await, b"+OK\r\n");

        sock.write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"$3\r\nbar\r\n");
    }

    #[tokio::test]
    async fn test_pipelining_batches_responses() {
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        // Three PINGs in one write; expect all three replies, batched or
        // not, in one logical read (may arrive as 1+ TCP segments, so keep
        // reading until we have three replies worth of bytes).
        sock.write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();

        let mut got = Vec::new();
        while got.len() < b"+PONG\r\n+PONG\r\n+PONG\r\n".len() {
            let chunk = read_response(&mut sock).await;
            got.extend_from_slice(&chunk);
        }
        assert_eq!(got, b"+PONG\r\n+PONG\r\n+PONG\r\n");
    }

    #[tokio::test]
    async fn test_oversized_value_rejected_before_body() {
        let limits = Limits {
            max_value_size: 16,
            ..Limits::default()
        };
        let (addr, _store) = start_test_server(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        // Declare a huge bulk length but never actually send that many
        // bytes -- if the server tried to buffer up to the declared
        // length before checking, this would hang instead of erroring.
        sock.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1000000000\r\n")
            .await
            .unwrap();

        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with('-'), "expected error reply, got {s}");
        assert!(s.contains("max_value_size"), "unexpected error: {s}");
    }

    #[tokio::test]
    async fn test_oversized_key_rejected() {
        let limits = Limits {
            max_key_size: 4,
            ..Limits::default()
        };
        let (addr, _store) = start_test_server(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*3\r\n$3\r\nSET\r\n$8\r\ntoolongk\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(
            s.starts_with('-') && s.contains("max_key_size"),
            "unexpected: {s}"
        );
    }

    #[tokio::test]
    async fn test_pipeline_depth_limit() {
        let limits = Limits {
            max_pipeline_depth: 2,
            ..Limits::default()
        };
        let (addr, _store) = start_test_server(limits).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();

        let mut got = Vec::new();
        loop {
            let chunk = read_response(&mut sock).await;
            if chunk.is_empty() {
                break;
            }
            got.extend_from_slice(&chunk);
            if got.ends_with(b"\r\n") && got.windows(1).any(|w| w == b"-") {
                break;
            }
        }
        let s = String::from_utf8_lossy(&got);
        assert!(s.contains("max pipeline depth"), "unexpected: {s}");
    }

    #[test]
    fn test_max_declared_bulk_len_scans_headers_only() {
        // Header present, body not yet arrived -- must still detect the
        // declared length without the body.
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$500000000\r\n";
        assert_eq!(max_declared_bulk_len(buf), Some(500_000_000));
    }

    #[test]
    fn test_max_declared_bulk_len_incomplete_header_returns_running_max() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$50";
        assert_eq!(max_declared_bulk_len(buf), Some(3));
    }
}
