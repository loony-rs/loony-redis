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
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error};

use protocol::{parse_frame, write_frame_into, Frame};
use storage::{Store, Value};

mod observability;
pub use observability::ServerMetrics;

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
    cluster: Arc<ClusterRouting>,
    metrics: Arc<ServerMetrics>,
    registry: metrics::Registry,
    health: Arc<metrics::Health>,
}

/// This shard's routing identity. `slot_table` is `None` by default,
/// meaning routing is a no-op and every command is handled locally --
/// the correct behavior for Phase 5/6 (a single real shard) and for any
/// deployment that hasn't been given a routing table at all. Wrapped in
/// `Arc` rather than `Arc<RwLock<_>>` for now: nothing in this phase
/// mutates it after `Server::new`. Phase 7's consensus-backed
/// `ClusterState` will need it to change at runtime, at which point this
/// becomes a shared, updatable handle instead of a fixed snapshot.
struct ClusterRouting {
    my_shard: cluster::ShardId,
    slot_table: Option<cluster::SlotTable>,
}

impl Server {
    pub fn new(store: Arc<Store>, limits: Limits) -> Self {
        let registry = metrics::Registry::new();
        let metrics = ServerMetrics::new(&registry)
            .expect("metric registration cannot fail: names/labels are fixed and unique");
        Server {
            store,
            limits: Arc::new(limits),
            conn_count: Arc::new(AtomicUsize::new(0)),
            cluster: Arc::new(ClusterRouting {
                my_shard: 0,
                slot_table: None,
            }),
            metrics,
            registry,
            health: Arc::new(metrics::Health::new()),
        }
    }

    /// Enable slot-based routing: commands whose keys map to a slot this
    /// shard doesn't own get `-MOVED` (or `-CROSSSLOT` if a multi-key
    /// command's keys don't all map to the same slot) instead of being
    /// executed locally. See docs/sharding.md.
    pub fn with_cluster_routing(
        mut self,
        my_shard: cluster::ShardId,
        slot_table: cluster::SlotTable,
    ) -> Self {
        let mut shard_ids: Vec<_> = slot_table
            .ranges()
            .into_iter()
            .map(|(_, _, shard, _)| shard)
            .collect();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        self.metrics.cluster_shards.set(shard_ids.len() as i64);
        self.metrics.cluster_nodes.set(shard_ids.len() as i64);
        self.cluster = Arc::new(ClusterRouting {
            my_shard,
            slot_table: Some(slot_table),
        });
        self
    }

    /// The Prometheus registry this server's metrics are registered
    /// into -- pass to `metrics::serve` to expose `/metrics` (plus
    /// `/live`/`/ready`/`/health` off `health()`) over HTTP.
    pub fn registry(&self) -> metrics::Registry {
        self.registry.clone()
    }

    /// Shared liveness/readiness handle -- see `metrics::Health`. This
    /// server sets `ready`/`healthy` together once it starts accepting
    /// connections (`serve`): `crates/server` alone has no deeper
    /// dependency (WAL, Raft quorum) to distinguish "ready" from
    /// "healthy" against -- that distinction becomes real once this
    /// crate is wired to real shards (PLAN.md's tracked, deferred gap).
    pub fn health(&self) -> Arc<metrics::Health> {
        self.health.clone()
    }

    pub async fn run(self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve(listener).await
    }

    /// Split out from `run` so tests can bind to an ephemeral port (`:0`)
    /// and learn the real address before serving.
    pub async fn serve(self, listener: TcpListener) -> anyhow::Result<()> {
        tracing::info!("listening on {}", listener.local_addr()?);
        self.health.set_ready(true);
        self.health.set_healthy(true);
        spawn_memory_sampler(self.metrics.clone());
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
            let cluster = self.cluster.clone();
            let metrics = self.metrics.clone();
            let guard = ConnGuard::new(self.conn_count.clone(), metrics.clone());

            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = handle_connection(socket, store, limits, cluster, metrics).await {
                    if !is_conn_reset(&e) {
                        error!("connection {peer} error: {e}");
                    }
                }
                debug!("connection {peer} closed");
            });
        }
    }
}

/// Refresh `memory_used_bytes` on a fixed interval rather than only when
/// scraped, so a scrape always reads a recent value instead of paying the
/// `/proc` read on the metrics server's own request path.
fn spawn_memory_sampler(metrics: Arc<ServerMetrics>) {
    tokio::spawn(async move {
        loop {
            if let Some(rss) = observability::read_rss_bytes() {
                metrics.memory_used_bytes.set(rss);
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
}

struct ConnGuard(Arc<AtomicUsize>, Arc<ServerMetrics>);

impl ConnGuard {
    fn new(counter: Arc<AtomicUsize>, metrics: Arc<ServerMetrics>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        metrics.connections_active.inc();
        ConnGuard(counter, metrics)
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
        self.1.connections_active.dec();
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
    cluster: Arc<ClusterRouting>,
    metrics: Arc<ServerMetrics>,
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

                    let response = dispatch(frame, &store, &limits, &cluster, &metrics);
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

fn dispatch(
    frame: Frame,
    store: &Store,
    limits: &Limits,
    cluster: &ClusterRouting,
    metrics: &ServerMetrics,
) -> Frame {
    let args = match frame {
        Frame::Array(Some(a)) if !a.is_empty() => a,
        Frame::Array(Some(_)) => return Frame::error("ERR empty command"),
        _ => return Frame::error("ERR invalid command format"),
    };

    let cmd = match bulk_bytes(&args, 0) {
        Some(b) => b.to_ascii_uppercase(),
        None => return Frame::error("ERR invalid command name"),
    };
    let cmd_label = String::from_utf8_lossy(&cmd).into_owned();
    let start = Instant::now();
    let response = dispatch_inner(&cmd, &args, store, limits, cluster);
    metrics.record(&cmd_label, &response, start.elapsed());
    response
}

fn dispatch_inner(
    cmd: &[u8],
    args: &[Frame],
    store: &Store,
    limits: &Limits,
    cluster: &ClusterRouting,
) -> Frame {
    if let Some(table) = &cluster.slot_table {
        let cmd_str = std::str::from_utf8(cmd).unwrap_or("");
        // No migrations tracked here yet: crates/server isn't wired to a
        // real shard/metadata group (see PLAN.md Phase 8/9's explicitly
        // deferred integration note), so there is no live ClusterState
        // to source SlotMigration records from. The Ask arm exists so
        // the protocol-level behavior is in place ahead of that wiring.
        match cluster::route(table, &[], cluster.my_shard, cmd_str, args) {
            cluster::RoutingDecision::Local => {}
            cluster::RoutingDecision::CrossSlot => return cluster::crossslot_error(),
            cluster::RoutingDecision::Moved { slot, leader_addr } => {
                return cluster::moved_error(slot, &leader_addr);
            }
            cluster::RoutingDecision::Ask { slot, leader_addr } => {
                return cluster::ask_error(slot, &leader_addr);
            }
        }
    }

    match cmd {
        b"PING" => cmd_ping(args),
        b"GET" => cmd_get(args, store),
        b"SET" => cmd_set(args, store, limits),
        b"DEL" => cmd_del(args, store, limits),
        b"LPUSH" => cmd_lpush(args, store, limits),
        b"RPUSH" => cmd_rpush(args, store, limits),
        b"LPOP" => cmd_lpop(args, store, limits),
        b"HSET" => cmd_hset(args, store, limits),
        b"HGET" => cmd_hget(args, store, limits),
        b"SADD" => cmd_sadd(args, store, limits),
        b"SMEMBERS" => cmd_smembers(args, store, limits),
        b"EXPIRE" => cmd_expire(args, store, limits),
        b"TTL" => cmd_ttl(args, store, limits),
        b"INFO" => cmd_info(),
        b"CLUSTER" => cmd_cluster(args, cluster),
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

// ── Cluster admin/debugging (docs/observability.md's "Admin / debugging") ──
//
// `CLUSTER SLOTS`/`NODES`/`INFO` report exactly what `ClusterRouting`
// holds -- the slot table this server was constructed with. There's no
// membership crate wired in here (the deferred crates/server integration
// gap tracked since Phase 8/9), so "known nodes" is approximated as the
// distinct shard count in the slot table; a real node registry would
// give an exact figure instead. `RAFT INFO` has no equivalent here since
// this crate holds no Raft handle at all -- see `test-utils`'s
// `test_node` admin protocol for that, where a real Raft group runs.

fn cmd_cluster(args: &[Frame], cluster: &ClusterRouting) -> Frame {
    let sub = match bulk_bytes(args, 1) {
        Some(b) => b.to_ascii_uppercase(),
        None => return wrong_num_args("cluster"),
    };
    match sub.as_slice() {
        b"INFO" => cmd_cluster_info(cluster),
        b"SLOTS" => cmd_cluster_slots(cluster),
        b"NODES" => cmd_cluster_nodes(cluster),
        other => Frame::error(format!(
            "ERR unknown CLUSTER subcommand `{}`",
            String::from_utf8_lossy(other)
        )),
    }
}

fn shard_ids(ranges: &[(u16, u16, cluster::ShardId, String)]) -> Vec<cluster::ShardId> {
    let mut ids: Vec<_> = ranges.iter().map(|(_, _, shard, _)| *shard).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn cmd_cluster_info(cluster: &ClusterRouting) -> Frame {
    let enabled = cluster.slot_table.is_some();
    let ranges = cluster
        .slot_table
        .as_ref()
        .map(|t| t.ranges())
        .unwrap_or_default();
    let assigned: u32 = ranges.iter().map(|(s, e, _, _)| (*e - *s + 1) as u32).sum();
    let known_shards = shard_ids(&ranges).len();
    Frame::bulk_str(format!(
        "cluster_enabled:{}\r\n\
         cluster_state:ok\r\n\
         cluster_slots_assigned:{}\r\n\
         cluster_known_nodes:{}\r\n\
         cluster_size:{}\r\n\
         cluster_my_shard:{}\r\n",
        enabled as u8, assigned, known_shards, known_shards, cluster.my_shard
    ))
}

/// Split `"host:port"` into its parts; falls back to `(addr, 0)` if `addr`
/// doesn't have a parseable trailing port (defensive only -- every
/// `SlotTable` entry in this codebase is populated from a real listen
/// address).
fn split_host_port(addr: &str) -> (&str, u16) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host, port.parse().unwrap_or(0)),
        None => (addr, 0),
    }
}

fn cmd_cluster_slots(cluster: &ClusterRouting) -> Frame {
    let ranges = match &cluster.slot_table {
        Some(t) => t.ranges(),
        None => return Frame::array(vec![]),
    };
    let entries = ranges
        .into_iter()
        .map(|(start, end, shard, addr)| {
            let (host, port) = split_host_port(&addr);
            Frame::array(vec![
                Frame::integer(start as i64),
                Frame::integer(end as i64),
                Frame::array(vec![
                    Frame::bulk_str(host),
                    Frame::integer(port as i64),
                    Frame::bulk_str(format!("shard-{shard}")),
                ]),
            ])
        })
        .collect();
    Frame::array(entries)
}

fn cmd_cluster_nodes(cluster: &ClusterRouting) -> Frame {
    let ranges = match &cluster.slot_table {
        Some(t) => t.ranges(),
        None => return Frame::bulk_str(""),
    };
    let mut lines = String::new();
    for (start, end, shard, addr) in ranges {
        let flags = if shard == cluster.my_shard {
            "myself,master"
        } else {
            "master"
        };
        lines.push_str(&format!(
            "shard-{shard} {addr} {flags} - 0 0 0 connected {start}-{end}\n"
        ));
    }
    Frame::bulk_str(lines)
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

    async fn start_routed_test_server(
        my_shard: cluster::ShardId,
        slot_table: cluster::SlotTable,
    ) -> std::net::SocketAddr {
        let store = Arc::new(Store::new());
        let server =
            Server::new(store, Limits::default()).with_cluster_routing(my_shard, slot_table);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(server.serve(listener));
        addr
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

    #[tokio::test]
    async fn test_moved_reply_when_another_shard_owns_the_slot() {
        let key = b"routed-key";
        let slot = cluster::slot_for_key(key);
        let mut table = cluster::SlotTable::new();
        table.set_owner(slot..=slot, 2, "127.0.0.1:9999");

        let addr = start_routed_test_server(1, table).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*2\r\n$3\r\nGET\r\n$10\r\nrouted-key\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert_eq!(s, format!("-MOVED {slot} 127.0.0.1:9999\r\n"));
    }

    #[tokio::test]
    async fn test_unowned_slot_defaults_to_local() {
        // No entry recorded for this key's slot at all -- Phase 5 has no
        // consensus-backed ClusterState yet (that's Phase 7), so an unset
        // slot must default to local rather than erroring.
        let table = cluster::SlotTable::new();
        let addr = start_routed_test_server(1, table).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"+OK\r\n");
    }

    #[tokio::test]
    async fn test_crossslot_rejected_when_keys_span_slots() {
        let table = cluster::SlotTable::new();
        let addr = start_routed_test_server(1, table).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();

        // "foo" and "bar" are known (from the CRC16 test vectors) to hash
        // to different slots.
        sock.write_all(b"*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("-CROSSSLOT"), "unexpected: {s}");
    }

    fn two_shard_table() -> cluster::SlotTable {
        let mut table = cluster::SlotTable::new();
        table.set_owner(0..=(cluster::SLOT_COUNT / 2 - 1), 1, "127.0.0.1:7001");
        table.set_owner(
            (cluster::SLOT_COUNT / 2)..=(cluster::SLOT_COUNT - 1),
            2,
            "127.0.0.1:7002",
        );
        table
    }

    #[tokio::test]
    async fn test_cluster_info_reports_slot_and_shard_counts() {
        let addr = start_routed_test_server(1, two_shard_table()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("cluster_enabled:1"), "unexpected: {s}");
        assert!(
            s.contains(&format!("cluster_slots_assigned:{}", cluster::SLOT_COUNT)),
            "unexpected: {s}"
        );
        assert!(s.contains("cluster_known_nodes:2"), "unexpected: {s}");
        assert!(s.contains("cluster_my_shard:1"), "unexpected: {s}");
    }

    #[tokio::test]
    async fn test_cluster_slots_returns_owned_ranges() {
        let addr = start_routed_test_server(1, two_shard_table()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*2\r\n"), "expected two ranges, got: {s}");
        assert!(s.contains("127.0.0.1"), "unexpected: {s}");
        assert!(s.contains("7001") && s.contains("7002"), "unexpected: {s}");
    }

    #[tokio::test]
    async fn test_cluster_nodes_marks_this_shard_as_myself() {
        let addr = start_routed_test_server(1, two_shard_table()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
            .await
            .unwrap();
        let resp = read_response(&mut sock).await;
        let s = String::from_utf8_lossy(&resp);
        assert!(
            s.contains("shard-1 127.0.0.1:7001 myself,master"),
            "unexpected: {s}"
        );
        assert!(
            s.contains("shard-2 127.0.0.1:7002 master"),
            "unexpected: {s}"
        );
        assert!(
            !s.contains("shard-2 127.0.0.1:7002 myself"),
            "unexpected: {s}"
        );
    }

    #[tokio::test]
    async fn test_cluster_disabled_returns_empty_results() {
        let (addr, _store) = start_test_server(Limits::default()).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n")
            .await
            .unwrap();
        assert_eq!(read_response(&mut sock).await, b"*0\r\n");
    }

    #[tokio::test]
    async fn test_metrics_and_health_endpoints_served_over_http() {
        let store = Arc::new(Store::new());
        let server = Server::new(store, Limits::default());
        let registry = server.registry();
        let health = server.health();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(server.serve(listener));

        // `serve` marks the node ready/healthy as soon as it starts
        // accepting -- give the spawned task a moment to run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(health.is_ready());
        assert!(health.is_healthy());

        let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        metrics::serve(metrics_listener, registry, health);

        // Drive one real command through the RESP port so requests_total
        // has a sample, then confirm it shows up on /metrics.
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let _ = read_response(&mut sock).await;

        let mut mstream = TcpStream::connect(metrics_addr).await.unwrap();
        mstream
            .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 8192];
        let n = mstream.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]);
        assert!(body.contains("200 OK"), "unexpected: {body}");
        assert!(body.contains("requests_total"), "unexpected: {body}");
        assert!(body.contains("command=\"PING\""), "unexpected: {body}");

        let mut lstream = TcpStream::connect(metrics_addr).await.unwrap();
        lstream
            .write_all(b"GET /health HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let n = lstream.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 OK"));
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
