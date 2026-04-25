use bytes::Bytes;
use std::sync::Arc;

use crate::persistence::Aof;
use crate::protocol::Frame;
use crate::storage::{Store, Value};

pub struct CommandContext {
    pub store: Arc<Store>,
    pub aof: Option<Arc<Aof>>,
}

/// Dispatch a decoded frame as a command and return the response frame.
pub async fn execute(frame: Frame, ctx: &CommandContext) -> Frame {
    let args = match frame {
        Frame::Array(Some(a)) if !a.is_empty() => a,
        Frame::Array(Some(_)) => return Frame::error("ERR empty command"),
        _ => return Frame::error("ERR invalid command format"),
    };

    let cmd = match bulk_bytes(&args, 0) {
        Some(b) => b,
        None => return Frame::error("ERR invalid command name"),
    };
    let cmd_str = String::from_utf8_lossy(&cmd).to_uppercase();

    match cmd_str.as_str() {
        // ── Connection ────────────────────────────────────────────────────
        "PING" => cmd_ping(&args),
        "ECHO" => cmd_echo(&args),
        "QUIT" => Frame::ok(),
        "SELECT" => Frame::ok(), // single-namespace — ignore DB index
        "CLIENT" => Frame::ok(),
        "COMMAND" => Frame::empty_array(),
        "CONFIG" => Frame::empty_array(),

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
        "LPOP" => cmd_lpop(&args, ctx),
        "RPOP" => cmd_rpop(&args, ctx),
        "LLEN" => cmd_llen(&args, ctx),
        "LRANGE" => cmd_lrange(&args, ctx),
        "LINDEX" => cmd_lindex(&args, ctx),

        // ── Hashes ────────────────────────────────────────────────────────
        "HSET" | "HMSET" => cmd_hset(&args, ctx).await,
        "HGET" => cmd_hget(&args, ctx),
        "HMGET" => cmd_hmget(&args, ctx),
        "HGETALL" => cmd_hgetall(&args, ctx),
        "HDEL" => cmd_hdel(&args, ctx),
        "HLEN" => cmd_hlen(&args, ctx),
        "HEXISTS" => cmd_hexists(&args, ctx),
        "HKEYS" => cmd_hkeys(&args, ctx),
        "HVALS" => cmd_hvals(&args, ctx),

        // ── Sets ──────────────────────────────────────────────────────────
        "SADD" => cmd_sadd(&args, ctx).await,
        "SMEMBERS" => cmd_smembers(&args, ctx),
        "SISMEMBER" => cmd_sismember(&args, ctx),
        "SREM" => cmd_srem(&args, ctx),
        "SCARD" => cmd_scard(&args, ctx),

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
    match ctx.store.lpop(&key) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None) => Frame::null_bulk(),
        Err(e) => Frame::error(e.to_string()),
    }
}

fn cmd_rpop(args: &[Frame], ctx: &CommandContext) -> Frame {
    if args.len() < 2 {
        return wrong_num_args("rpop");
    }
    let key = match bulk_as_str(args, 1) { Some(k) => k, None => return wrong_num_args("rpop") };
    match ctx.store.rpop(&key) {
        Ok(Some(v)) => Frame::Bulk(Some(v)),
        Ok(None) => Frame::null_bulk(),
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
