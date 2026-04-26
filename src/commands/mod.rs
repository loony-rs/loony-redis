use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::cluster::{command_slot, node_id_of, CommandSlot, NodeState, SharedCluster, SharedHealth};
use crate::consensus::Raft;
use crate::observability::Metrics;
use crate::persistence::Aof;
use crate::protocol::{serialize_frame, Frame};
use crate::replication::Replication;
use crate::storage::{ScoreBound, Store, Value};

pub struct CommandContext {
    pub store:   Arc<Store>,
    pub aof:     Option<Arc<Aof>>,
    pub repl:    Arc<Replication>,
    /// Raft handle, present when consensus-based replication is active.
    pub raft:    Option<Arc<Raft>>,
    /// Cluster config, present when slot-based sharding is active.
    pub cluster: Option<SharedCluster>,
    /// Node health table, present when cluster heartbeat is active.
    pub health:  Option<SharedHealth>,
    /// Metrics collector, absent during replica replay.
    pub metrics: Option<Arc<Metrics>>,
    /// True when replaying commands received from the leader or from the Raft
    /// log. Skips READONLY checks and suppresses re-broadcasting/re-logging.
    pub is_replica_replay: bool,
}

/// Commands that mutate state. Used for READONLY enforcement and replication.
static WRITE_COMMANDS: &[&str] = &[
    "SET", "MSET", "SETNX", "GETSET", "APPEND",
    "INCR", "INCRBY", "DECR", "DECRBY",
    "DEL",
    "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LINSERT", "LREM", "LTRIM", "LMOVE",
    "HSET", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT", "HSETNX",
    "SADD", "SREM", "SMOVE", "SPOP", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
    "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
    "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
    "EXPIRE", "PEXPIRE", "PERSIST",
    "FLUSHDB", "FLUSHALL",
];

fn is_write(cmd: &str) -> bool {
    WRITE_COMMANDS.contains(&cmd)
}

/// Dispatch a decoded frame as a command and return the response frame.
pub async fn execute(frame: Frame, ctx: &CommandContext) -> Frame {
    let args = match frame {
        Frame::Array(Some(a)) if !a.is_empty() => a,
        Frame::Array(Some(_)) => return Frame::error("ERR empty command"),
        _ => return Frame::error("ERR invalid command format"),
    };

    // Uppercase the command name into a stack buffer — no heap allocation.
    // All Redis command names are ASCII and fit within 32 bytes.
    let mut upper_buf = [0u8; 32];
    let cmd_str: &str = {
        let cmd = match bulk_bytes(&args, 0) {
            Some(b) => b,
            None    => return Frame::error("ERR invalid command name"),
        };
        let n = cmd.len().min(32);
        for (i, &b) in cmd.iter().take(32).enumerate() {
            upper_buf[i] = b.to_ascii_uppercase();
        }
        std::str::from_utf8(&upper_buf[..n]).unwrap_or("")
    };

    // ── Raft path: route writes through consensus ─────────────────────────
    if is_write(cmd_str) && !ctx.is_replica_replay {
        if let Some(raft) = &ctx.raft {
            if !raft.is_leader.load(std::sync::atomic::Ordering::Acquire) {
                let leader = raft.current_leader().await.unwrap_or_default();
                return Frame::error(format!("MOVED {leader}"));
            }
            // Serialise the command and submit to Raft.
            let cmd_bytes = serialize_frame(&Frame::Array(Some(args)));
            return raft.propose(cmd_bytes).await;
        }
    }

    // ── Cluster routing ───────────────────────────────────────────────────
    if !ctx.is_replica_replay {
        if let Some(cluster) = &ctx.cluster {
            // CLUSTER subcommands are always handled locally.
            if cmd_str != "CLUSTER" {
                // Acquire read lock, compute routing decision, release before I/O.
                let routing = {
                    let state = cluster.read().await;
                    match command_slot(&cmd_str, &args) {
                        CommandSlot::CrossSlot => Some(Err("CROSSSLOT Keys in request don't hash to the same slot")),
                        CommandSlot::Slot(slot) if !state.config.is_mine(slot) => {
                            Some(Ok(state.config.node_for_slot(slot).to_string()))
                        }
                        _ => None,
                    }
                };
                match routing {
                    Some(Err(e)) => return Frame::error(e),
                    Some(Ok(target)) => {
                        return crate::cluster::proxy_to(&target, Frame::Array(Some(args))).await;
                    }
                    None => {}
                }
            }
        }
    }

    // ── Phase-4 replication path ──────────────────────────────────────────
    // Reject writes on replicas (unless we're in the replay path).
    if is_write(&cmd_str) && !ctx.is_replica_replay && !ctx.repl.is_leader {
        return Frame::error(
            "READONLY You can't write against a read only replica.",
        );
    }

    let response = dispatch(cmd_str, &args, ctx).await;

    // Record metrics for every command (skipped during replica replay).
    if let Some(m) = &ctx.metrics {
        m.record_cmd(cmd_str, matches!(&response, Frame::Error(_)));
    }

    // Broadcast successful writes to connected replicas.
    if is_write(cmd_str) && !ctx.is_replica_replay && ctx.repl.is_leader && ctx.raft.is_none() {
        if !matches!(&response, Frame::Error(_)) {
            let cmd_bytes = serialize_frame(&Frame::Array(Some(args)));
            ctx.repl.broadcast(cmd_bytes);
        }
    }

    response
}

async fn dispatch(cmd_str: &str, args: &[Frame], ctx: &CommandContext) -> Frame {
    match cmd_str {
        // ── Connection ────────────────────────────────────────────────────
        "PING" => cmd_ping(&args),
        "ECHO" => cmd_echo(&args),
        "QUIT" => Frame::ok(),
        "SELECT" => Frame::ok(), // single-namespace — ignore DB index
        "CLIENT" => Frame::ok(),
        "COMMAND" => Frame::empty_array(),
        "CONFIG" => Frame::empty_array(),
        "CLUSTER" => cmd_cluster(&args, ctx).await,
        "MIGRATE" => cmd_migrate(&args, ctx).await,
        "INFO"  => cmd_info(&args, ctx).await,
        "DEBUG" => cmd_debug(&args, ctx).await,

        // ── Keyspace ──────────────────────────────────────────────────────
        "DEL" => cmd_del(&args, ctx).await,
        "EXISTS" => cmd_exists(&args, ctx),
        "TYPE" => cmd_type(&args, ctx),
        "EXPIRE" => cmd_expire(&args, ctx),
        "PEXPIRE" => cmd_pexpire(&args, ctx),
        "TTL" => cmd_ttl(&args, ctx),
        "PTTL" => cmd_pttl(&args, ctx),
        "PERSIST" => cmd_persist(&args, ctx),
        "KEYS" => cmd_keys(&args, ctx),
        "DBSIZE" => Frame::integer(ctx.store.keys_count() as i64),
        "FLUSHDB" | "FLUSHALL" => {
            ctx.store.flush();
            Frame::ok()
        }

        // ── Strings ───────────────────────────────────────────────────────
        "GET" => cmd_get(&args, ctx),
        "SET" => cmd_set(&args, ctx).await,
        "MGET" => cmd_mget(&args, ctx),
        "MSET" => cmd_mset(&args, ctx).await,
        "STRLEN" => cmd_strlen(&args, ctx),
        "APPEND" => cmd_append(&args, ctx).await,
        "INCR" => cmd_incrby(&args, ctx, 1).await,
        "DECR" => cmd_incrby(&args, ctx, -1).await,
        "INCRBY" => cmd_incrby_by(&args, ctx, 1).await,
        "DECRBY" => cmd_incrby_by(&args, ctx, -1).await,
        "GETSET" => cmd_getset(&args, ctx).await,
        "SETNX" => cmd_setnx(&args, ctx).await,

        // ── Lists ─────────────────────────────────────────────────────────
        "LPUSH" => cmd_lpush(&args, ctx).await,
        "RPUSH" => cmd_rpush(&args, ctx).await,
        "LPOP"  => cmd_lpop(&args, ctx),
        "RPOP"  => cmd_rpop(&args, ctx),
        "LLEN"  => cmd_llen(&args, ctx),
        "LRANGE" => cmd_lrange(&args, ctx),
        "LINDEX" => cmd_lindex(&args, ctx),
        "LSET"   => cmd_lset(&args, ctx),
        "LINSERT" => cmd_linsert(&args, ctx),
        "LREM"   => cmd_lrem(&args, ctx),
        "LTRIM"  => cmd_ltrim(&args, ctx),
        "LMOVE"  => cmd_lmove(&args, ctx),

        // ── Hashes ────────────────────────────────────────────────────────
        "HSET" | "HMSET" => cmd_hset(&args, ctx).await,
        "HGET"    => cmd_hget(&args, ctx),
        "HMGET"   => cmd_hmget(&args, ctx),
        "HGETALL" => cmd_hgetall(&args, ctx),
        "HDEL"    => cmd_hdel(&args, ctx),
        "HLEN"    => cmd_hlen(&args, ctx),
        "HEXISTS" => cmd_hexists(&args, ctx),
        "HKEYS"   => cmd_hkeys(&args, ctx),
        "HVALS"   => cmd_hvals(&args, ctx),
        "HSETNX"  => cmd_hsetnx(&args, ctx).await,
        "HINCRBY" => cmd_hincrby(&args, ctx).await,
        "HINCRBYFLOAT" => cmd_hincrbyfloat(&args, ctx).await,

        // ── Sets ──────────────────────────────────────────────────────────
        "SADD"     => cmd_sadd(&args, ctx).await,
        "SMEMBERS" => cmd_smembers(&args, ctx),
        "SISMEMBER" => cmd_sismember(&args, ctx),
        "SREM"     => cmd_srem(&args, ctx),
        "SCARD"    => cmd_scard(&args, ctx),
        "SMOVE"    => cmd_smove(&args, ctx),
        "SRANDMEMBER" => cmd_srandmember(&args, ctx),
        "SPOP"     => cmd_spop(&args, ctx),
        "SUNION"   => cmd_sunion(&args, ctx),
        "SINTER"   => cmd_sinter(&args, ctx),
        "SDIFF"    => cmd_sdiff(&args, ctx),
        "SUNIONSTORE" => cmd_sunionstore(&args, ctx),
        "SINTERSTORE" => cmd_sinterstore(&args, ctx),
        "SDIFFSTORE"  => cmd_sdiffstore(&args, ctx),

        // ── Sorted sets ───────────────────────────────────────────────────
        "ZADD"   => cmd_zadd(&args, ctx).await,
        "ZSCORE" => cmd_zscore(&args, ctx),
        "ZRANK"  => cmd_zrank(&args, ctx),
        "ZREVRANK" => cmd_zrevrank(&args, ctx),
        "ZCARD"  => cmd_zcard(&args, ctx),
        "ZCOUNT" => cmd_zcount(&args, ctx),
        "ZINCRBY" => cmd_zincrby(&args, ctx).await,
        "ZREM"   => cmd_zrem(&args, ctx),
        "ZRANGE" => cmd_zrange(&args, ctx),
        "ZREVRANGE" => cmd_zrevrange(&args, ctx),
        "ZRANGEBYSCORE" => cmd_zrangebyscore(&args, ctx),
        "ZREVRANGEBYSCORE" => cmd_zrevrangebyscore(&args, ctx),
        "ZPOPMIN" => cmd_zpopmin(&args, ctx),
        "ZPOPMAX" => cmd_zpopmax(&args, ctx),
        "ZREMRANGEBYRANK"  => cmd_zremrangebyrank(&args, ctx),
        "ZREMRANGEBYSCORE" => cmd_zremrangebyscore(&args, ctx),

        _ => Frame::error(format!("ERR unknown command `{cmd_str}`")),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

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
    bulk_bytes(args, idx)
        .and_then(|b| String::from_utf8_lossy(&b).trim().parse().ok())
}

fn wrong_num_args(cmd: &str) -> Frame {
    Frame::error(format!(
        "ERR wrong number of arguments for '{cmd}' command"
    ))
}

// ── Connection commands ────────────────────────────────────────────────────

fn cmd_ping(args: &[Frame]) -> Frame {
    match bulk_bytes(args, 1) {
        Some(msg) => Frame::Bulk(Some(msg)),
        None => Frame::pong(),
    }
}

fn cmd_echo(args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("echo");
    }
    match bulk_bytes(args, 1) {
        Some(b) => Frame::Bulk(Some(b)),
        None => wrong_num_args("echo"),
    }
}

// ── Keyspace commands ──────────────────────────────────────────────────────

async fn cmd_del(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("del");
    }
    let keys: Vec<String> = args[1..]
        .iter()
        .filter_map(|f| match f {
            Frame::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect();

    let deleted = ctx.store.del(&keys);

    if deleted > 0 {
        if let Some(aof) = &ctx.aof {
            let _ = aof.log_del(&keys).await;
        }
    }
    Frame::integer(deleted as i64)
}

fn cmd_exists(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("exists");
    }
    let count = args[1..]
        .iter()
        .filter(|f| {
            if let Frame::Bulk(Some(b)) = f {
                ctx.store.exists(&String::from_utf8_lossy(b))
            } else {
                false
            }
        })
        .count();
    Frame::integer(count as i64)
}

fn cmd_type(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("type");
    }
    let key = match bulk_as_str(args, 1) {
        Some(k) => k,
        None => return wrong_num_args("type"),
    };
    Frame::SimpleString(ctx.store.type_of(&key).to_string())
}

fn cmd_expire(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("expire");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("expire") };
    let secs = match bulk_as_i64(args, 2) {
        Some(n) if n >= 0 => n as u64,
        _ => return Frame::error("ERR invalid expire time"),
    };
    let set = ctx.store.expire(&key, std::time::Duration::from_secs(secs));
    Frame::integer(if set { 1 } else { 0 })
}

fn cmd_pexpire(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("pexpire");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("pexpire") };
    let ms = match bulk_as_i64(args, 2) {
        Some(n) if n >= 0 => n as u64,
        _ => return Frame::error("ERR invalid expire time"),
    };
    let set = ctx.store.expire(&key, std::time::Duration::from_millis(ms));
    Frame::integer(if set { 1 } else { 0 })
}

fn cmd_ttl(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("ttl");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("ttl") };
    let pttl = ctx.store.pttl(&key);
    Frame::integer(if pttl > 0 { pttl / 1000 } else { pttl })
}

fn cmd_pttl(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("pttl");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("pttl") };
    Frame::integer(ctx.store.pttl(&key))
}

fn cmd_persist(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("persist");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("persist") };
    Frame::integer(if ctx.store.persist(&key) { 1 } else { 0 })
}

fn cmd_keys(args: &[Frame], ctx: &CommandContext) -> Frame {
    // Only support * glob for now.
    let _pattern = bulk_as_str(args, 1).unwrap_or_else(|| "*".into());
    let keys: Vec<Frame> = ctx
        .store
        .all_keys()
        .into_iter()
        .map(|k| Frame::Bulk(Some(Bytes::from(k.into_bytes()))))
        .collect();
    Frame::Array(Some(keys))
}

// ── String commands ────────────────────────────────────────────────────────

fn cmd_get(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("get");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("get") };
    match ctx.store.get(&key) {
        Some(Value::String(b)) => Frame::Bulk(Some(b)),
        Some(_) => Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => Frame::null_bulk(),
    }
}

async fn cmd_set(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("set");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("set") };
    let val = match bulk_bytes(args, 2) { Some(v) => v, None => return wrong_num_args("set") };

    // Parse optional EX / PX options
    let mut ttl: Option<std::time::Duration> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 3;
    while i < args.len() {
        match bulk_as_str(args, i).as_deref() {
            Some("EX") => {
                i += 1;
                ttl = bulk_as_i64(args, i)
                    .filter(|&n| n > 0)
                    .map(|n| std::time::Duration::from_secs(n as u64));
            }
            Some("PX") => {
                i += 1;
                ttl = bulk_as_i64(args, i)
                    .filter(|&n| n > 0)
                    .map(|n| std::time::Duration::from_millis(n as u64));
            }
            Some("NX") => nx = true,
            Some("XX") => xx = true,
            _ => {}
        }
        i += 1;
    }

    let exists = ctx.store.exists(&key);
    if nx && exists {
        return Frame::null_bulk();
    }
    if xx && !exists {
        return Frame::null_bulk();
    }

    ctx.store.set(key.clone(), Value::String(val.clone()), ttl);

    if let Some(aof) = &ctx.aof {
        let _ = aof.log_set(&key, &val).await;
    }
    Frame::ok()
}

fn cmd_mget(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("mget");
    }
    let items: Vec<Frame> = args[1..]
        .iter()
        .map(|f| match f {
            Frame::Bulk(Some(k)) => match ctx.store.get(&String::from_utf8_lossy(k)) {
                Some(Value::String(b)) => Frame::Bulk(Some(b)),
                _ => Frame::null_bulk(),
            },
            _ => Frame::null_bulk(),
        })
        .collect();
    Frame::Array(Some(items))
}

async fn cmd_mset(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return wrong_num_args("mset");
    }
    let mut i = 1;
    while i + 1 < args.len() {
        let key = match bulk_as_str(args, i) { Some(k) => k, None => { i += 2; continue } };
        let val = match bulk_bytes(args, i + 1) { Some(v) => v, None => { i += 2; continue } };
        ctx.store.set(key.clone(), Value::String(val.clone()), None);
        if let Some(aof) = &ctx.aof {
            let _ = aof.log_set(&key, &val).await;
        }
        i += 2;
    }
    Frame::ok()
}

fn cmd_strlen(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("strlen");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("strlen") };
    match ctx.store.get(&key) {
        Some(Value::String(b)) => Frame::integer(b.len() as i64),
        Some(_) => Frame::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => Frame::integer(0),
    }
}

async fn cmd_append(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("append");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("append") };
    let suffix = match bulk_bytes(args, 2) { Some(v) => v, None => return wrong_num_args("append") };
    match ctx.store.append(key.clone(), suffix.clone()) {
        Ok(new_len) => {
            if let Some(aof) = &ctx.aof {
                let _ = aof.log_generic(&[b"APPEND", key.as_bytes(), &suffix]).await;
            }
            Frame::integer(new_len as i64)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_incrby(args: &[Frame], ctx: &CommandContext, delta: i64) -> Frame {
    if args.len() != 2 {
        return wrong_num_args(if delta > 0 { "incr" } else { "decr" });
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("incr") };
    match ctx.store.incrby(key.clone(), delta) {
        Ok(n) => {
            if let Some(aof) = &ctx.aof {
                let cmd = if delta > 0 { b"INCR".as_slice() } else { b"DECR".as_slice() };
                let _ = aof.log_generic(&[cmd, key.as_bytes()]).await;
            }
            Frame::integer(n)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_incrby_by(args: &[Frame], ctx: &CommandContext, sign: i64) -> Frame {
    if args.len() != 3 {
        let name = if sign > 0 { "incrby" } else { "decrby" };
        return wrong_num_args(name);
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("incrby") };
    let by = match bulk_as_i64(args, 2) {
        Some(n) => n * sign,
        None => return Frame::error("ERR value is not an integer or out of range"),
    };
    match ctx.store.incrby(key.clone(), by) {
        Ok(n) => {
            if let Some(aof) = &ctx.aof {
                let cmd = if sign > 0 { b"INCRBY".as_slice() } else { b"DECRBY".as_slice() };
                let by_str = by.abs().to_string();
                let _ = aof.log_generic(&[cmd, key.as_bytes(), by_str.as_bytes()]).await;
            }
            Frame::integer(n)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_getset(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("getset");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("getset") };
    let val = match bulk_bytes(args, 2) { Some(v) => v, None => return wrong_num_args("getset") };
    let old = ctx.store.getset(key.clone(), val.clone());
    if let Some(aof) = &ctx.aof {
        let _ = aof.log_set(&key, &val).await;
    }
    Frame::Bulk(old)
}

async fn cmd_setnx(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("setnx");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("setnx") };
    let val = match bulk_bytes(args, 2) { Some(v) => v, None => return wrong_num_args("setnx") };
    if ctx.store.exists(&key) {
        return Frame::integer(0);
    }
    ctx.store.set(key.clone(), Value::String(val.clone()), None);
    if let Some(aof) = &ctx.aof {
        let _ = aof.log_set(&key, &val).await;
    }
    Frame::integer(1)
}

// ── List commands ──────────────────────────────────────────────────────────

async fn cmd_lpush(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("lpush");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lpush") };
    let values: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.lpush(key.clone(), values.clone()) {
        Ok(len) => {
            if let Some(aof) = &ctx.aof {
                let _ = aof.log_list_push("LPUSH", &key, &values).await;
            }
            Frame::integer(len as i64)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_rpush(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("rpush");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("rpush") };
    let values: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.rpush(key.clone(), values.clone()) {
        Ok(len) => {
            if let Some(aof) = &ctx.aof {
                let _ = aof.log_list_push("RPUSH", &key, &values).await;
            }
            Frame::integer(len as i64)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_lpop(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("lpop");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lpop") };
    let count = bulk_as_i64(args, 2).unwrap_or(1).max(0) as usize;
    let with_count = args.len() >= 3;
    match ctx.store.lpop(&key, count.max(1)) {
        Ok(mut v) => {
            if with_count {
                Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect()))
            } else {
                match v.pop() {
                    Some(b) => Frame::Bulk(Some(b)),
                    None => Frame::null_bulk(),
                }
            }
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_rpop(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("rpop");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("rpop") };
    let count = bulk_as_i64(args, 2).unwrap_or(1).max(0) as usize;
    let with_count = args.len() >= 3;
    match ctx.store.rpop(&key, count.max(1)) {
        Ok(mut v) => {
            if with_count {
                Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect()))
            } else {
                match v.pop() {
                    Some(b) => Frame::Bulk(Some(b)),
                    None => Frame::null_bulk(),
                }
            }
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_llen(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("llen");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("llen") };
    match ctx.store.llen(&key) {
        Ok(n) => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_lrange(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 {
        return wrong_num_args("lrange");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lrange") };
    let start = match bulk_as_i64(args, 2) {
        Some(n) => n,
        None => return Frame::error("ERR value is not an integer or out of range"),
    };
    let stop = match bulk_as_i64(args, 3) {
        Some(n) => n,
        None => return Frame::error("ERR value is not an integer or out of range"),
    };
    match ctx.store.lrange(&key, start, stop) {
        Ok(items) => Frame::Array(Some(
            items.into_iter().map(|b| Frame::Bulk(Some(b))).collect(),
        )),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_lindex(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("lindex");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lindex") };
    let index = match bulk_as_i64(args, 2) {
        Some(n) => n,
        None => return Frame::error("ERR value is not an integer or out of range"),
    };
    match ctx.store.lindex(&key, index) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None) => Frame::null_bulk(),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Hash commands ──────────────────────────────────────────────────────────

async fn cmd_hset(args: &[Frame], ctx: &CommandContext) -> Frame {
    // HSET key field value [field value ...]
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return wrong_num_args("hset");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hset") };
    let mut added = 0usize;
    let mut i = 2;
    while i + 1 < args.len() {
        let field = match bulk_bytes(args, i) { Some(f) => f, None => { i += 2; continue } };
        let value = match bulk_bytes(args, i + 1) { Some(v) => v, None => { i += 2; continue } };
        match ctx.store.hset(key.clone(), field.clone(), value.clone()) {
            Ok(n) => added += n,
            Err(e) => return Frame::error(e.to_string()),
        }
        if let Some(aof) = &ctx.aof {
            let _ = aof.log_hset(&key, &field, &value).await;
        }
        i += 2;
    }
    Frame::integer(added as i64)
}

fn cmd_hget(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("hget");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hget") };
    let field = match bulk_bytes(args, 2) { Some(f) => f, None => return wrong_num_args("hget") };
    match ctx.store.hget(&key, &field) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None) => Frame::null_bulk(),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hmget(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("hmget");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hmget") };
    let items: Vec<Frame> = args[2..].iter().map(|f| {
        let field = match f { Frame::Bulk(Some(b)) => b, _ => return Frame::null_bulk() };
        match ctx.store.hget(&key, field) {
            Ok(Some(v)) => Frame::Bulk(Some(v)),
            _ => Frame::null_bulk(),
        }
    }).collect();
    Frame::Array(Some(items))
}

fn cmd_hgetall(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("hgetall");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hgetall") };
    match ctx.store.hgetall(&key) {
        Ok(pairs) => {
            let mut items = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                items.push(Frame::Bulk(Some(k)));
                items.push(Frame::Bulk(Some(v)));
            }
            Frame::Array(Some(items))
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hdel(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("hdel");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hdel") };
    let fields: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.hdel(&key, &fields) {
        Ok(n) => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hlen(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("hlen");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hlen") };
    match ctx.store.hlen(&key) {
        Ok(n) => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hexists(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("hexists");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hexists") };
    let field = match bulk_bytes(args, 2) { Some(f) => f, None => return wrong_num_args("hexists") };
    match ctx.store.hexists(&key, &field) {
        Ok(b) => Frame::integer(if b { 1 } else { 0 }),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hkeys(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("hkeys");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hkeys") };
    match ctx.store.hkeys(&key) {
        Ok(keys) => Frame::Array(Some(keys.into_iter().map(|b| Frame::Bulk(Some(b))).collect())),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_hvals(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("hvals");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hvals") };
    match ctx.store.hvals(&key) {
        Ok(vals) => Frame::Array(Some(vals.into_iter().map(|b| Frame::Bulk(Some(b))).collect())),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Set commands ───────────────────────────────────────────────────────────

async fn cmd_sadd(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("sadd");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("sadd") };
    let members: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.sadd(key.clone(), members.clone()) {
        Ok(n) => {
            if n > 0 {
                if let Some(aof) = &ctx.aof {
                    let _ = aof.log_sadd(&key, &members).await;
                }
            }
            Frame::integer(n as i64)
        }
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_smembers(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("smembers");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("smembers") };
    match ctx.store.smembers(&key) {
        Ok(members) => Frame::Array(Some(
            members.into_iter().map(|b| Frame::Bulk(Some(b))).collect(),
        )),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sismember(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 {
        return wrong_num_args("sismember");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("sismember") };
    let member = match bulk_bytes(args, 2) { Some(m) => m, None => return wrong_num_args("sismember") };
    match ctx.store.sismember(&key, &member) {
        Ok(b) => Frame::integer(if b { 1 } else { 0 }),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_srem(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 {
        return wrong_num_args("srem");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("srem") };
    let members: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.srem(&key, &members) {
        Ok(n) => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_scard(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 {
        return wrong_num_args("scard");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("scard") };
    match ctx.store.scard(&key) {
        Ok(n) => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── CLUSTER commands ───────────────────────────────────────────────────────

async fn cmd_cluster(args: &[Frame], ctx: &CommandContext) -> Frame {
    use crate::cluster::{
        drain_slots, push_entries_to, send_internal, slot_for_key, ClusterConfig,
    };
    use tracing::info;

    let sub = bulk_as_str(args, 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_default();

    match sub.as_str() {
        // ── Read-only info commands ───────────────────────────────────────
        "INFO" => {
            let body = match &ctx.cluster {
                Some(c) => c.read().await.config.cluster_info(),
                None => "cluster_enabled:0\r\ncluster_state:ok\r\n".to_string(),
            };
            Frame::bulk_str(body)
        }
        "NODES" => {
            let body = match &ctx.cluster {
                Some(c) => c.read().await.config.cluster_nodes_text(),
                None => String::new(),
            };
            Frame::bulk_str(body)
        }
        "SLOTS" => match &ctx.cluster {
            Some(c) => c.read().await.config.cluster_slots_frame(),
            None => Frame::empty_array(),
        },
        "SHARDS" => match &ctx.cluster {
            Some(c) => c.read().await.config.cluster_shards_frame(),
            None => Frame::empty_array(),
        },
        "MYID" => match &ctx.cluster {
            Some(c) => Frame::bulk_str(crate::cluster::node_id_of(c.read().await.config.my_addr())),
            None => Frame::bulk_str(""),
        },
        "KEYSLOT" => {
            if args.len() < 3 {
                return wrong_num_args("cluster|keyslot");
            }
            match bulk_bytes(args, 2) {
                Some(k) => Frame::integer(slot_for_key(&k) as i64),
                None => wrong_num_args("cluster|keyslot"),
            }
        }
        "RESET" => Frame::ok(),

        // ── CLUSTER HEALTH — operator view of peer liveness ───────────────
        "HEALTH" => {
            match &ctx.health {
                None => Frame::bulk_str("cluster health monitoring not enabled"),
                Some(ht) => {
                    let mut rows = ht.snapshot().await;
                    rows.sort_by(|a, b| a.0.cmp(&b.0));
                    let body = rows.iter()
                        .map(|(addr, state)| format!("{addr} {}", state.as_str()))
                        .collect::<Vec<_>>()
                        .join("\r\n");
                    Frame::bulk_str(body)
                }
            }
        }

        // ── CLUSTER GOSSIP health-csv — internal peer health update ────────
        "GOSSIP" => {
            let payload = match bulk_as_str(args, 2) {
                Some(p) => p,
                None => return wrong_num_args("cluster|gossip"),
            };
            if let Some(ht) = &ctx.health {
                let updates: Vec<(String, NodeState)> = payload
                    .split(',')
                    .filter_map(|part| {
                        let mut it = part.splitn(2, '=');
                        let addr = it.next()?.trim().to_string();
                        let state = match it.next()?.trim() {
                            "alive"     => NodeState::Alive,
                            "suspected" => NodeState::Suspected,
                            "failed"    => NodeState::Failed,
                            _           => return None,
                        };
                        Some((addr, state))
                    })
                    .collect();
                ht.apply_gossip(&updates).await;
            }
            Frame::ok()
        }

        // ── CLUSTER MEET host port — add a node and rebalance ─────────────
        "MEET" => {
            if args.len() < 4 {
                return wrong_num_args("cluster|meet");
            }
            let host = match bulk_as_str(args, 2) {
                Some(h) => h,
                None => return wrong_num_args("cluster|meet"),
            };
            let port = match bulk_as_str(args, 3).and_then(|s| s.parse::<u16>().ok()) {
                Some(p) => p,
                None => return Frame::error("ERR invalid port"),
            };
            let new_addr = format!("{host}:{port}");

            let cluster = match &ctx.cluster {
                Some(c) => c,
                None => return Frame::error("ERR cluster mode not enabled"),
            };

            // Snapshot old state.
            let (old_nodes, my_addr) = {
                let state = cluster.read().await;
                if state.config.nodes.contains(&new_addr) {
                    return Frame::error("ERR node already in cluster");
                }
                (state.config.nodes.clone(), state.config.my_addr().to_string())
            };

            // Compute the new sorted node list — same algorithm every node uses.
            let mut new_nodes = old_nodes.clone();
            new_nodes.push(new_addr.clone());
            new_nodes.sort();
            new_nodes.dedup();
            let nodes_csv = new_nodes.join(",");

            // Tell new node about the full cluster first so it can accept data.
            let sync_frame = Frame::Array(Some(vec![
                Frame::bulk_str("CLUSTER"),
                Frame::bulk_str("SYNC"),
                Frame::bulk_str(&nodes_csv),
            ]));
            send_internal(&new_addr, sync_frame).await;

            // Tell all existing peers (triggers their own migrations).
            for peer in &old_nodes {
                if *peer != my_addr {
                    let f = Frame::Array(Some(vec![
                        Frame::bulk_str("CLUSTER"),
                        Frame::bulk_str("SYNC"),
                        Frame::bulk_str(&nodes_csv),
                    ]));
                    send_internal(peer, f).await;
                }
            }

            // Perform this node's own migrations.
            let old_cfg = ClusterConfig::new(old_nodes, &my_addr).unwrap();
            let new_cfg = ClusterConfig::new(new_nodes, &my_addr).unwrap();
            let to_send = old_cfg.slots_to_send(&new_cfg);
            let mut migrated = 0usize;
            for (target, slots) in to_send {
                let entries = drain_slots(&slots, &ctx.store);
                let n = entries.len();
                push_entries_to(&target, entries).await;
                migrated += n;
            }
            if migrated > 0 {
                info!("MEET {new_addr}: migrated {migrated} keys outbound");
            }

            // Commit new config locally.
            cluster.write().await.config = new_cfg;
            Frame::ok()
        }

        // ── CLUSTER FORGET node-id — gracefully remove a node ────────────
        "FORGET" => {
            if args.len() < 3 {
                return wrong_num_args("cluster|forget");
            }
            let node_id_arg = match bulk_as_str(args, 2) {
                Some(s) => s,
                None => return wrong_num_args("cluster|forget"),
            };

            let cluster = match &ctx.cluster {
                Some(c) => c,
                None => return Frame::error("ERR cluster mode not enabled"),
            };

            // Resolve node-id → address.
            let (leaving_addr, old_nodes, my_addr) = {
                let state = cluster.read().await;
                let found = state.config.nodes.iter().find(|a| {
                    crate::cluster::node_id_of(a) == node_id_arg || **a == node_id_arg
                });
                match found {
                    None => return Frame::error(format!("ERR unknown node id `{node_id_arg}`")),
                    Some(a) => {
                        if *a == state.config.my_addr() {
                            return Frame::error("ERR cannot FORGET self");
                        }
                        (a.clone(), state.config.nodes.clone(), state.config.my_addr().to_string())
                    }
                }
            };

            let mut new_nodes = old_nodes.clone();
            new_nodes.retain(|a| *a != leaving_addr);
            let nodes_csv = new_nodes.join(",");

            let new_cfg = ClusterConfig::new(new_nodes.clone(), &my_addr).unwrap();

            // Step 1: Update this node's routing FIRST so it can accept incoming keys.
            cluster.write().await.config = new_cfg;

            // Step 2: Update all remaining peers — they must have correct routing
            //         before the departing node pushes keys to them.
            for peer in &old_nodes {
                if *peer != my_addr && *peer != leaving_addr {
                    let f = Frame::Array(Some(vec![
                        Frame::bulk_str("CLUSTER"),
                        Frame::bulk_str("SYNC"),
                        Frame::bulk_str(&nodes_csv),
                    ]));
                    send_internal(peer, f).await;
                }
            }

            // Step 3: DRAIN the departing node (all receivers now have correct routing).
            let drain_frame = Frame::Array(Some(vec![
                Frame::bulk_str("CLUSTER"),
                Frame::bulk_str("DRAIN"),
                Frame::bulk_str(&nodes_csv),
            ]));
            let drain_resp = send_internal(&leaving_addr, drain_frame).await;
            if let Frame::Error(e) = &drain_resp {
                return Frame::error(format!("ERR DRAIN failed: {e}"));
            }

            info!("FORGET {leaving_addr}: cluster shrunk to {} nodes", new_nodes.len());
            Frame::ok()
        }

        // ── CLUSTER SYNC node-list — internal: peer says membership changed ─
        "SYNC" => {
            let nodes_csv = match bulk_as_str(args, 2) {
                Some(s) => s,
                None => return wrong_num_args("cluster|sync"),
            };
            let new_nodes: Vec<String> = nodes_csv
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();

            let cluster = match &ctx.cluster {
                Some(c) => c,
                None => return Frame::error("ERR cluster mode not enabled"),
            };

            let (old_nodes, my_addr) = {
                let state = cluster.read().await;
                (state.config.nodes.clone(), state.config.my_addr().to_string())
            };

            if !new_nodes.contains(&my_addr) {
                return Frame::error("ERR this node is not in the new node list");
            }

            let old_cfg = ClusterConfig::new(old_nodes, &my_addr).unwrap();
            let new_cfg = ClusterConfig::new(new_nodes, &my_addr).unwrap();

            // Migrate outbound slots.
            let to_send = old_cfg.slots_to_send(&new_cfg);
            let mut migrated = 0usize;
            for (target, slots) in to_send {
                let entries = drain_slots(&slots, &ctx.store);
                let n = entries.len();
                push_entries_to(&target, entries).await;
                migrated += n;
            }
            if migrated > 0 {
                info!("SYNC: migrated {migrated} keys outbound");
            }

            cluster.write().await.config = new_cfg;
            Frame::ok()
        }

        // ── CLUSTER DRAIN new-node-list — internal: migrate all my slots ──
        "DRAIN" => {
            let nodes_csv = match bulk_as_str(args, 2) {
                Some(s) => s,
                None => return wrong_num_args("cluster|drain"),
            };
            let new_nodes: Vec<String> = nodes_csv
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();

            let cluster = match &ctx.cluster {
                Some(c) => c,
                None => return Frame::error("ERR cluster mode not enabled"),
            };

            let (my_addr, all_slots) = {
                let state = cluster.read().await;
                (state.config.my_addr().to_string(), state.config.my_slots())
            };

            // Build a routing table for the new cluster to know where to send each key.
            let new_routing = ClusterConfig::for_routing(new_nodes.clone());

            // Group keys by new owner.
            let mut by_target: std::collections::HashMap<String, Vec<u16>> =
                std::collections::HashMap::new();
            for slot in all_slots {
                let owner = new_routing.node_for_slot(slot).to_string();
                if owner != my_addr {
                    by_target.entry(owner).or_default().push(slot);
                }
            }

            let mut total = 0usize;
            for (target, slots) in by_target {
                let entries = drain_slots(&slots, &ctx.store);
                let n = entries.len();
                push_entries_to(&target, entries).await;
                total += n;
            }
            if total > 0 {
                info!("DRAIN: pushed {total} keys to new owners");
            }

            Frame::integer(total as i64)
        }

        _ => Frame::error(format!("ERR unknown CLUSTER subcommand `{sub}`")),
    }
}

// ── MIGRATE command ────────────────────────────────────────────────────────

async fn cmd_migrate(args: &[Frame], ctx: &CommandContext) -> Frame {
    // MIGRATE host port key db timeout [COPY] [REPLACE]
    if args.len() < 6 {
        return wrong_num_args("migrate");
    }
    let host = match bulk_as_str(args, 1) {
        Some(h) => h,
        None => return wrong_num_args("migrate"),
    };
    let port = match bulk_as_str(args, 2).and_then(|s| s.parse::<u16>().ok()) {
        Some(p) => p,
        None => return Frame::error("ERR invalid port"),
    };
    let key = match bulk_as_str(args, 3) {
        Some(k) => k,
        None => return wrong_num_args("migrate"),
    };
    let copy = args[6..].iter().any(|f| {
        matches!(f, Frame::Bulk(Some(b)) if b.to_ascii_uppercase() == b"COPY")
    });

    let target = format!("{host}:{port}");

    let value = match ctx.store.get(&key) {
        Some(v) => v,
        None => return Frame::bulk_str("NOKEY"),
    };
    let pttl = ctx.store.pttl(&key);

    let frame = crate::cluster::build_value_frame(&key, &value, pttl);
    let resp = crate::cluster::send_internal(&target, frame).await;

    match resp {
        Frame::SimpleString(ref s) if s == "OK" => {
            if !copy {
                ctx.store.del(&[key]);
            }
            Frame::ok()
        }
        Frame::Error(e) => Frame::error(format!("ERR target: {e}")),
        _ => Frame::error("ERR unexpected response from migration target"),
    }
}

// ── Extended List commands ─────────────────────────────────────────────────

fn cmd_lset(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("lset"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lset") };
    let index = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid index") };
    let val   = match bulk_bytes(args, 3)  { Some(v) => v, None => return wrong_num_args("lset") };
    match ctx.store.lset(&key, index, val) {
        Ok(true)  => Frame::ok(),
        Ok(false) => Frame::error("ERR index out of range"),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_linsert(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 5 { return wrong_num_args("linsert"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("linsert") };
    let pos   = match bulk_as_str(args, 2) { Some(p) => p, None => return wrong_num_args("linsert") };
    let pivot = match bulk_bytes(args, 3)  { Some(v) => v, None => return wrong_num_args("linsert") };
    let val   = match bulk_bytes(args, 4)  { Some(v) => v, None => return wrong_num_args("linsert") };
    let before = match pos.to_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER"  => false,
        _ => return Frame::error("ERR syntax error"),
    };
    match ctx.store.linsert(&key, before, pivot, val) {
        Ok(n)  => Frame::integer(n),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_lrem(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("lrem"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lrem") };
    let count = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid count") };
    let val   = match bulk_bytes(args, 3)  { Some(v) => v, None => return wrong_num_args("lrem") };
    match ctx.store.lrem(&key, count, val) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_ltrim(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("ltrim"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("ltrim") };
    let start = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid start") };
    let stop  = match bulk_as_i64(args, 3) { Some(n) => n, None => return Frame::error("ERR invalid stop") };
    match ctx.store.ltrim(&key, start, stop) {
        Ok(()) => Frame::ok(),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_lmove(args: &[Frame], ctx: &CommandContext) -> Frame {
    // LMOVE source dest LEFT|RIGHT LEFT|RIGHT
    if args.len() != 5 { return wrong_num_args("lmove"); }
    let src     = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("lmove") };
    let dst     = match bulk_as_str(args, 2) { Some(k) => k, None => return wrong_num_args("lmove") };
    let wherefrom = match bulk_as_str(args, 3) { Some(s) => s, None => return wrong_num_args("lmove") };
    let whereto   = match bulk_as_str(args, 4) { Some(s) => s, None => return wrong_num_args("lmove") };
    let from_left = match wherefrom.to_uppercase().as_str() {
        "LEFT"  => true,
        "RIGHT" => false,
        _ => return Frame::error("ERR syntax error"),
    };
    let to_left = match whereto.to_uppercase().as_str() {
        "LEFT"  => true,
        "RIGHT" => false,
        _ => return Frame::error("ERR syntax error"),
    };
    match ctx.store.lmove(&src, &dst, from_left, to_left) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None)    => Frame::null_bulk(),
        Err(e)      => Frame::error(e.to_string()),
    }
}

// ── Extended Hash commands ─────────────────────────────────────────────────

async fn cmd_hsetnx(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("hsetnx"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hsetnx") };
    let field = match bulk_bytes(args, 2)  { Some(f) => f, None => return wrong_num_args("hsetnx") };
    let val   = match bulk_bytes(args, 3)  { Some(v) => v, None => return wrong_num_args("hsetnx") };
    match ctx.store.hsetnx(key, field, val) {
        Ok(set) => Frame::integer(if set { 1 } else { 0 }),
        Err(e)  => Frame::error(e.to_string()),
    }
}

async fn cmd_hincrby(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("hincrby"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hincrby") };
    let field = match bulk_bytes(args, 2)  { Some(f) => f, None => return wrong_num_args("hincrby") };
    let delta = match bulk_as_i64(args, 3) { Some(n) => n, None => return Frame::error("ERR not an integer") };
    match ctx.store.hincrby(key, field, delta) {
        Ok(n)  => Frame::integer(n),
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_hincrbyfloat(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("hincrbyfloat"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("hincrbyfloat") };
    let field = match bulk_bytes(args, 2)  { Some(f) => f, None => return wrong_num_args("hincrbyfloat") };
    let delta: f64 = match bulk_as_str(args, 3).as_deref().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None    => return Frame::error("ERR not a float"),
    };
    match ctx.store.hincrbyfloat(key, field, delta) {
        Ok(v)  => Frame::bulk_str(format!("{v}")),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Extended Set commands ──────────────────────────────────────────────────

fn cmd_smove(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("smove"); }
    let src    = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("smove") };
    let dst    = match bulk_as_str(args, 2) { Some(k) => k, None => return wrong_num_args("smove") };
    let member = match bulk_bytes(args, 3)  { Some(m) => m, None => return wrong_num_args("smove") };
    match ctx.store.smove(&src, &dst, &member) {
        Ok(b)  => Frame::integer(if b { 1 } else { 0 }),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_srandmember(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("srandmember"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("srandmember") };
    let count = bulk_as_i64(args, 2);
    match ctx.store.srandmember(&key, count.unwrap_or(1)) {
        Err(e) => Frame::error(e.to_string()),
        Ok(v) => {
            if count.is_none() {
                // No count arg: return single element or nil
                match v.into_iter().next() {
                    Some(b) => Frame::Bulk(Some(b)),
                    None    => Frame::null_bulk(),
                }
            } else {
                Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect()))
            }
        }
    }
}

fn cmd_spop(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("spop"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("spop") };
    let count = bulk_as_i64(args, 2).map(|n| n.max(0) as usize);
    let n = count.unwrap_or(1);
    match ctx.store.spop(&key, n) {
        Err(e) => Frame::error(e.to_string()),
        Ok(v) => {
            if count.is_none() {
                match v.into_iter().next() {
                    Some(b) => Frame::Bulk(Some(b)),
                    None    => Frame::null_bulk(),
                }
            } else {
                Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect()))
            }
        }
    }
}

fn multi_key_set_args(args: &[Frame], start: usize) -> Vec<String> {
    args[start..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }).collect()
}

fn cmd_sunion(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("sunion"); }
    let keys = multi_key_set_args(args, 1);
    match ctx.store.sunion(&keys) {
        Ok(v)  => Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect())),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sinter(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("sinter"); }
    let keys = multi_key_set_args(args, 1);
    match ctx.store.sinter(&keys) {
        Ok(v)  => Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect())),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sdiff(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("sdiff"); }
    let keys = multi_key_set_args(args, 1);
    match ctx.store.sdiff(&keys) {
        Ok(v)  => Frame::Array(Some(v.into_iter().map(|b| Frame::Bulk(Some(b))).collect())),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sunionstore(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("sunionstore"); }
    let dst  = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("sunionstore") };
    let keys = multi_key_set_args(args, 2);
    match ctx.store.sunionstore(&dst, &keys) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sinterstore(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("sinterstore"); }
    let dst  = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("sinterstore") };
    let keys = multi_key_set_args(args, 2);
    match ctx.store.sinterstore(&dst, &keys) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_sdiffstore(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("sdiffstore"); }
    let dst  = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("sdiffstore") };
    let keys = multi_key_set_args(args, 2);
    match ctx.store.sdiffstore(&dst, &keys) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── Sorted-set commands ────────────────────────────────────────────────────

async fn cmd_zadd(args: &[Frame], ctx: &CommandContext) -> Frame {
    // ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    if args.len() < 4 { return wrong_num_args("zadd"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zadd") };

    let mut nx = false; let mut xx = false;
    let mut gt = false; let mut lt = false;
    let mut ch = false;
    let mut i = 2usize;

    // Consume flags
    loop {
        match bulk_as_str(args, i).as_deref() {
            Some("NX") => { nx = true; i += 1; }
            Some("XX") => { xx = true; i += 1; }
            Some("GT") => { gt = true; i += 1; }
            Some("LT") => { lt = true; i += 1; }
            Some("CH") => { ch = true; i += 1; }
            _ => break,
        }
    }

    if i + 1 >= args.len() { return wrong_num_args("zadd"); }

    let mut added = 0usize;
    let mut changed = 0usize;
    while i + 1 < args.len() {
        let score_s = match bulk_as_str(args, i) { Some(s) => s, None => { i += 2; continue } };
        let score: f64 = match score_s.parse() {
            Ok(v) => v,
            Err(_) => return Frame::error("ERR value is not a valid float"),
        };
        let member = match bulk_bytes(args, i + 1) { Some(m) => m, None => { i += 2; continue } };
        match ctx.store.zadd(key.clone(), score, member, nx, xx, gt, lt) {
            Ok((a, c)) => { added += a; changed += c; }
            Err(e) => return Frame::error(e.to_string()),
        }
        i += 2;
    }

    let result = if ch { changed } else { added };

    if result > 0 {
        if let Some(aof) = &ctx.aof {
            // Re-build the original command for AOF logging.
            let parts: Vec<&[u8]> = std::iter::once(b"ZADD".as_slice())
                .chain(std::iter::once(key.as_bytes()))
                .collect();
            let _ = aof.log_generic(&parts).await;
        }
    }
    Frame::integer(result as i64)
}

fn cmd_zscore(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 3 { return wrong_num_args("zscore"); }
    let key    = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zscore") };
    let member = match bulk_bytes(args, 2)  { Some(m) => m, None => return wrong_num_args("zscore") };
    match ctx.store.zscore(&key, &member) {
        Ok(Some(s)) => Frame::bulk_str(format!("{s}")),
        Ok(None)    => Frame::null_bulk(),
        Err(e)      => Frame::error(e.to_string()),
    }
}

fn cmd_zrank(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("zrank"); }
    let key    = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrank") };
    let member = match bulk_bytes(args, 2)  { Some(m) => m, None => return wrong_num_args("zrank") };
    match ctx.store.zrank(&key, &member) {
        Ok(Some(r)) => Frame::integer(r as i64),
        Ok(None)    => Frame::null_bulk(),
        Err(e)      => Frame::error(e.to_string()),
    }
}

fn cmd_zrevrank(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("zrevrank"); }
    let key    = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrevrank") };
    let member = match bulk_bytes(args, 2)  { Some(m) => m, None => return wrong_num_args("zrevrank") };
    match ctx.store.zrevrank(&key, &member) {
        Ok(Some(r)) => Frame::integer(r as i64),
        Ok(None)    => Frame::null_bulk(),
        Err(e)      => Frame::error(e.to_string()),
    }
}

fn cmd_zcard(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 2 { return wrong_num_args("zcard"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zcard") };
    match ctx.store.zcard(&key) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_zcount(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("zcount"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zcount") };
    let min = match bulk_as_str(args, 2).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR min is not a float"),
    };
    let max = match bulk_as_str(args, 3).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR max is not a float"),
    };
    match ctx.store.zcount(&key, min, max) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

async fn cmd_zincrby(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("zincrby"); }
    let key    = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zincrby") };
    let delta: f64 = match bulk_as_str(args, 2).as_deref().and_then(|s| s.parse().ok()) {
        Some(v) => v, None => return Frame::error("ERR not a float"),
    };
    let member = match bulk_bytes(args, 3)  { Some(m) => m, None => return wrong_num_args("zincrby") };
    match ctx.store.zincrby(key, delta, member) {
        Ok(s)  => Frame::bulk_str(format!("{s}")),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_zrem(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 3 { return wrong_num_args("zrem"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrem") };
    let members: Vec<Bytes> = args[2..].iter().filter_map(|f| match f {
        Frame::Bulk(Some(b)) => Some(b.clone()),
        _ => None,
    }).collect();
    match ctx.store.zrem(&key, &members) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn zset_pairs_to_frame(pairs: Vec<(Bytes, f64)>, withscores: bool) -> Frame {
    let mut items = Vec::with_capacity(pairs.len() * if withscores { 2 } else { 1 });
    for (member, score) in pairs {
        items.push(Frame::Bulk(Some(member)));
        if withscores {
            items.push(Frame::bulk_str(format!("{score}")));
        }
    }
    Frame::Array(Some(items))
}

fn has_withscores(args: &[Frame], from: usize) -> bool {
    args[from..].iter().any(|f| {
        matches!(f, Frame::Bulk(Some(b)) if b.to_ascii_uppercase() == b"WITHSCORES")
    })
}

fn cmd_zrange(args: &[Frame], ctx: &CommandContext) -> Frame {
    // ZRANGE key start stop [WITHSCORES]
    if args.len() < 4 { return wrong_num_args("zrange"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrange") };
    let start = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid start") };
    let stop  = match bulk_as_i64(args, 3) { Some(n) => n, None => return Frame::error("ERR invalid stop") };
    let ws = has_withscores(args, 4);
    match ctx.store.zrange(&key, start, stop, false) {
        Ok(pairs) => zset_pairs_to_frame(pairs, ws),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_zrevrange(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 4 { return wrong_num_args("zrevrange"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrevrange") };
    let start = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid start") };
    let stop  = match bulk_as_i64(args, 3) { Some(n) => n, None => return Frame::error("ERR invalid stop") };
    let ws = has_withscores(args, 4);
    match ctx.store.zrange(&key, start, stop, true) {
        Ok(pairs) => zset_pairs_to_frame(pairs, ws),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn parse_limit(args: &[Frame], from: usize) -> (usize, Option<usize>) {
    // [LIMIT offset count]
    for i in from..args.len().saturating_sub(2) {
        if let Some(s) = bulk_as_str(args, i) {
            if s.to_uppercase() == "LIMIT" {
                let offset = bulk_as_i64(args, i + 1).unwrap_or(0).max(0) as usize;
                let count  = bulk_as_i64(args, i + 2).map(|n| if n < 0 { usize::MAX } else { n as usize });
                return (offset, count);
            }
        }
    }
    (0, None)
}

fn cmd_zrangebyscore(args: &[Frame], ctx: &CommandContext) -> Frame {
    // ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    if args.len() < 4 { return wrong_num_args("zrangebyscore"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrangebyscore") };
    let min = match bulk_as_str(args, 2).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR min is not a float"),
    };
    let max = match bulk_as_str(args, 3).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR max is not a float"),
    };
    let ws = has_withscores(args, 4);
    let (offset, limit) = parse_limit(args, 4);
    match ctx.store.zrangebyscore(&key, min, max, offset, limit, false) {
        Ok(pairs) => zset_pairs_to_frame(pairs, ws),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_zrevrangebyscore(args: &[Frame], ctx: &CommandContext) -> Frame {
    // ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    if args.len() < 4 { return wrong_num_args("zrevrangebyscore"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zrevrangebyscore") };
    let max = match bulk_as_str(args, 2).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR max is not a float"),
    };
    let min = match bulk_as_str(args, 3).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR min is not a float"),
    };
    let ws = has_withscores(args, 4);
    let (offset, limit) = parse_limit(args, 4);
    match ctx.store.zrangebyscore(&key, min, max, offset, limit, true) {
        Ok(pairs) => zset_pairs_to_frame(pairs, ws),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_zpopmin(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("zpopmin"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zpopmin") };
    let count = bulk_as_i64(args, 2).map(|n| n.max(0) as usize).unwrap_or(1);
    match ctx.store.zpopmin(&key, count) {
        Ok(pairs) => zset_pairs_to_frame(pairs, true),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_zpopmax(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 { return wrong_num_args("zpopmax"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zpopmax") };
    let count = bulk_as_i64(args, 2).map(|n| n.max(0) as usize).unwrap_or(1);
    match ctx.store.zpopmax(&key, count) {
        Ok(pairs) => zset_pairs_to_frame(pairs, true),
        Err(e)    => Frame::error(e.to_string()),
    }
}

fn cmd_zremrangebyrank(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("zremrangebyrank"); }
    let key   = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zremrangebyrank") };
    let start = match bulk_as_i64(args, 2) { Some(n) => n, None => return Frame::error("ERR invalid start") };
    let stop  = match bulk_as_i64(args, 3) { Some(n) => n, None => return Frame::error("ERR invalid stop") };
    match ctx.store.zremrangebyrank(&key, start, stop) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_zremrangebyscore(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() != 4 { return wrong_num_args("zremrangebyscore"); }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("zremrangebyscore") };
    let min = match bulk_as_str(args, 2).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR min is not a float"),
    };
    let max = match bulk_as_str(args, 3).as_deref().and_then(ScoreBound::parse) {
        Some(b) => b, None => return Frame::error("ERR max is not a float"),
    };
    match ctx.store.zremrangebyscore(&key, min, max) {
        Ok(n)  => Frame::integer(n as i64),
        Err(e) => Frame::error(e.to_string()),
    }
}

// ── INFO ──────────────────────────────────────────────────────────────────

async fn cmd_info(args: &[Frame], ctx: &CommandContext) -> Frame {
    let section = bulk_as_str(args, 1)
        .map(|s| s.to_uppercase())
        .unwrap_or_else(|| "ALL".to_string());

    let want = |name: &str| section == "ALL" || section == "DEFAULT" || section == name;

    let mut out = String::with_capacity(1024);

    // ── Server ────────────────────────────────────────────────────────────
    if want("SERVER") {
        out.push_str("# Server\r\n");
        out.push_str("redis_version:7.0.0-loony\r\n");
        if let Some(m) = &ctx.metrics {
            out.push_str(&format!("tcp_port:{}\r\n", m.port));
            out.push_str(&format!("uptime_in_seconds:{}\r\n", m.uptime_secs()));
            out.push_str(&format!("uptime_in_days:{}\r\n", m.uptime_secs() / 86400));
        }
        out.push_str("os:Linux\r\n");
        out.push_str("arch_bits:64\r\n");
        out.push_str("executable:loony-redis\r\n");
        out.push_str("\r\n");
    }

    // ── Stats ─────────────────────────────────────────────────────────────
    if want("STATS") {
        out.push_str("# Stats\r\n");
        if let Some(m) = &ctx.metrics {
            out.push_str(&format!("total_commands_processed:{}\r\n", m.total_commands()));
            out.push_str(&format!("total_connections_received:{}\r\n", m.total_connections()));
            out.push_str(&format!("connected_clients:{}\r\n", m.active_connections()));
        }
        out.push_str("\r\n");
    }

    // ── Replication ───────────────────────────────────────────────────────
    if want("REPLICATION") {
        out.push_str("# Replication\r\n");
        out.push_str(if ctx.repl.is_leader { "role:master\r\n" } else { "role:replica\r\n" });
        if let Some(ref addr) = ctx.repl.leader_addr {
            let host = addr.split(':').next().unwrap_or("");
            let port = addr.split(':').nth(1).unwrap_or("0");
            out.push_str(&format!("master_host:{host}\r\n"));
            out.push_str(&format!("master_port:{port}\r\n"));
        }
        out.push_str(&format!(
            "master_repl_offset:{}\r\n",
            ctx.repl.commit_count.load(Ordering::Relaxed)
        ));
        out.push_str("\r\n");
    }

    // ── Cluster ───────────────────────────────────────────────────────────
    if want("CLUSTER") {
        out.push_str("# Cluster\r\n");
        if let Some(ref c) = ctx.cluster {
            let state = c.read().await;
            out.push_str("cluster_enabled:1\r\n");
            out.push_str(&format!("cluster_my_id:{}\r\n", node_id_of(state.config.my_addr())));
            out.push_str(&format!("cluster_known_nodes:{}\r\n", state.config.nodes.len()));
            let slots_owned: u32 = state.config
                .slot_ranges_for(state.config.my_index)
                .iter()
                .map(|(s, e)| (e - s + 1) as u32)
                .sum();
            out.push_str(&format!("cluster_slots_owned:{slots_owned}\r\n"));
        } else {
            out.push_str("cluster_enabled:0\r\n");
        }
        out.push_str("\r\n");
    }

    // ── Keyspace ──────────────────────────────────────────────────────────
    if want("KEYSPACE") {
        out.push_str("# Keyspace\r\n");
        let (total, expires) = ctx.store.keyspace_info();
        if total > 0 {
            out.push_str(&format!("db0:keys={total},expires={expires},avg_ttl=0\r\n"));
        }
        out.push_str("\r\n");
    }

    Frame::Bulk(Some(Bytes::from(out.into_bytes())))
}

// ── DEBUG ─────────────────────────────────────────────────────────────────

async fn cmd_debug(args: &[Frame], ctx: &CommandContext) -> Frame {
    let sub = match bulk_as_str(args, 1).map(|s| s.to_uppercase()) {
        Some(s) => s,
        None    => return Frame::error("ERR wrong number of arguments for 'debug' command"),
    };

    match sub.as_str() {
        "SLEEP" => {
            let secs = bulk_as_str(args, 2)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            Frame::ok()
        }

        "OBJECT" => {
            let key = match bulk_as_str(args, 2) {
                Some(k) => k,
                None    => return Frame::error("ERR syntax error"),
            };
            match ctx.store.debug_object(&key) {
                Some(info) => Frame::Bulk(Some(Bytes::from(info.into_bytes()))),
                None       => Frame::error("ERR no such key"),
            }
        }

        // Cluster routing-table dump — useful for live inspection
        "CLUSTER" => {
            if let Some(ref c) = ctx.cluster {
                let state = c.read().await;
                let mut s = String::new();
                for (i, node) in state.config.nodes.iter().enumerate() {
                    let ranges = state.config.slot_ranges_for(i);
                    let range_str = ranges.iter()
                        .map(|(a, b)| format!("{a}-{b}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let marker = if i == state.config.my_index { " *" } else { "" };
                    s.push_str(&format!("{node}{marker} slots:[{range_str}]\n"));
                }
                Frame::Bulk(Some(Bytes::from(s.into_bytes())))
            } else {
                Frame::Bulk(Some(Bytes::from_static(b"cluster not enabled")))
            }
        }

        // Health-table dump with missed counts and last-seen timestamps
        "HEALTH" => {
            if let Some(ref h) = ctx.health {
                let peers = h.snapshot().await;
                let mut s = String::new();
                for (addr, state) in &peers {
                    s.push_str(&format!("{addr} {}\n", state.as_str()));
                }
                if s.is_empty() { s.push_str("no peers\n"); }
                Frame::Bulk(Some(Bytes::from(s.into_bytes())))
            } else {
                Frame::Bulk(Some(Bytes::from_static(b"cluster not enabled")))
            }
        }

        "RELOAD" | "LOADAOF" | "JMAP" => Frame::ok(),

        _ => Frame::error(format!("ERR unknown debug sub-command '{sub}'")),
    }
}
