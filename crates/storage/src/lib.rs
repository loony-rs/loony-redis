pub mod hash;
pub mod list;
pub mod set;
pub mod zset;

pub use hash::Hash;
pub use list::List;
pub use set::Set;
pub use zset::{ScoreBound, ZSet};

use bytes::Bytes;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, used as the store's only notion of
/// "now". Expiry is always an explicit absolute value carried in a command
/// (see docs/invariants.md S5) rather than a per-replica-local `Instant`,
/// which is nondeterministic across replicas and meaningless after restart.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

// ── Value ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    List(List),
    Hash(Hash),
    Set(Set),
    ZSet(ZSet),
}

// ── Snapshot (replication full-sync) ──────────────────────────────────────

pub enum SnapshotEntry {
    String { key: String, value: Bytes },
    List   { key: String, items: Vec<Bytes> },
    Hash   { key: String, fields: Vec<(Bytes, Bytes)> },
    Set    { key: String, members: Vec<Bytes> },
    ZSet   { key: String, members: Vec<(Bytes, f64)> },
}

// ── Store internals ────────────────────────────────────────────────────────

#[derive(Debug)]
struct Entry {
    value: Value,
    /// Absolute expiry, in milliseconds since the Unix epoch. `None` means
    /// no expiry. Always an explicit value carried by the command that set
    /// it, never recomputed independently by each replica.
    expires_at: Option<u64>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.map(|t| now_ms() > t).unwrap_or(false)
    }
}

// ── Store ──────────────────────────────────────────────────────────────────
//
// Lock granularity: DashMap uses 64 shards by default, so concurrent writes
// to keys in different shards never contend.  Within a shard, operations are
// serialised at the DashMap level.  A further improvement — wrapping each
// entry in Arc<RwLock<Entry>> so readers on the same key don't block readers
// on other keys in the same shard — is left for Phase 11 (performance), where
// it can be benchmarked properly.

pub struct Store {
    data: DashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Store { data: DashMap::new() }
    }

    // ── Key-space helpers ──────────────────────────────────────────────────

    pub fn get(&self, key: &str) -> Option<Value> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    /// `expire_at`: absolute expiry in epoch milliseconds, or `None` for no
    /// expiry. This must be computed once by the caller (the command
    /// proposer), not derived from `now_ms()` inside the store on each
    /// replica — see docs/invariants.md S5.
    pub fn set(&self, key: String, value: Value, expire_at: Option<u64>) {
        self.data.insert(key, Entry { value, expires_at: expire_at });
    }

    pub fn del(&self, keys: &[String]) -> usize {
        keys.iter().filter(|k| self.data.remove(*k).is_some()).count()
    }

    pub fn exists(&self, key: &str) -> bool {
        self.data.get(key).map(|e| !e.is_expired()).unwrap_or(false)
    }

    /// `expire_at`: absolute expiry in epoch milliseconds, computed once by
    /// the caller (see `set`'s doc comment).
    pub fn expire(&self, key: &str, expire_at: u64) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() { return false; }
            entry.expires_at = Some(expire_at);
            true
        } else {
            false
        }
    }

    pub fn pttl(&self, key: &str) -> i64 {
        match self.data.get(key) {
            None => -2,
            Some(entry) => {
                if entry.is_expired() { return -2; }
                match entry.expires_at {
                    None => -1,
                    Some(t) => {
                        let now = now_ms();
                        if t <= now { -2 } else { (t - now) as i64 }
                    }
                }
            }
        }
    }

    pub fn persist(&self, key: &str) -> bool {
        if let Some(mut e) = self.data.get_mut(key) {
            if e.expires_at.is_some() { e.expires_at = None; return true; }
        }
        false
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        match self.data.get(key) {
            None => "none",
            Some(e) if e.is_expired() => "none",
            Some(e) => match &e.value {
                Value::String(_) => "string",
                Value::List(_)   => "list",
                Value::Hash(_)   => "hash",
                Value::Set(_)    => "set",
                Value::ZSet(_)   => "zset",
            },
        }
    }

    pub fn keys_count(&self) -> usize { self.data.len() }

    /// Returns (total_live_keys, keys_with_expiry).
    pub fn keyspace_info(&self) -> (usize, usize) {
        let mut total = 0usize;
        let mut with_expiry = 0usize;
        for e in self.data.iter() {
            if !e.value().is_expired() {
                total += 1;
                if e.value().expires_at.is_some() {
                    with_expiry += 1;
                }
            }
        }
        (total, with_expiry)
    }

    /// Returns a Redis-style DEBUG OBJECT string for `key`, or None if missing.
    pub fn debug_object(&self, key: &str) -> Option<String> {
        let e = self.data.get(key)?;
        if e.is_expired() { return None; }

        let (type_str, encoding, sz) = match &e.value {
            Value::String(b) => {
                let enc = if b.len() <= 44 { "embstr" } else { "raw" };
                ("string", enc, b.len())
            }
            Value::List(l) => {
                let enc = match l { list::List::Small(_) => "listpack", list::List::Large(_) => "quicklist" };
                ("list", enc, l.len())
            }
            Value::Hash(h) => {
                let enc = match h { hash::Hash::Small(_) => "listpack", hash::Hash::Large(_) => "hashtable" };
                ("hash", enc, h.len())
            }
            Value::Set(s) => {
                let enc = match s { set::Set::Small(_) => "listpack", set::Set::Large(_) => "hashtable" };
                ("set", enc, s.len())
            }
            Value::ZSet(z) => ("zset", "skiplist", z.len()),
        };

        let idle = match e.expires_at {
            Some(t) => t.saturating_sub(now_ms()) / 1000,
            None    => 0,
        };

        Some(format!(
            "Value at 0x0 refcount:1 encoding:{encoding} \
             serializedlength:{sz} lru:0 lru_seconds_idle:{idle} type:{type_str}"
        ))
    }

    pub fn all_keys(&self) -> Vec<String> {
        self.data.iter()
            .filter(|e| !e.value().is_expired())
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn flush(&self) { self.data.clear(); }

    pub fn snapshot(&self) -> Vec<SnapshotEntry> {
        self.data.iter()
            .filter(|e| !e.value().is_expired())
            .filter_map(|e| {
                let key = e.key().clone();
                match &e.value().value {
                    Value::String(b) => Some(SnapshotEntry::String { key, value: b.clone() }),
                    Value::List(l) if !l.is_empty() =>
                        Some(SnapshotEntry::List { key, items: l.to_vec() }),
                    Value::Hash(h) if !h.is_empty() =>
                        Some(SnapshotEntry::Hash { key, fields: h.all_pairs() }),
                    Value::Set(s) if !s.is_empty() =>
                        Some(SnapshotEntry::Set { key, members: s.members() }),
                    Value::ZSet(z) if !z.is_empty() =>
                        Some(SnapshotEntry::ZSet { key, members: z.all_with_scores() }),
                    _ => None,
                }
            })
            .collect()
    }

    // ── String operations ──────────────────────────────────────────────────

    pub fn getset(&self, key: String, new_val: Bytes) -> Option<Bytes> {
        let old = match self.get(&key) {
            Some(Value::String(b)) => Some(b),
            _ => None,
        };
        self.set(key, Value::String(new_val), None);
        old
    }

    pub fn append(&self, key: String, suffix: Bytes) -> anyhow::Result<usize> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::String(Bytes::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::String(b) => {
                let mut v = b.to_vec();
                v.extend_from_slice(&suffix);
                *b = Bytes::from(v);
                Ok(b.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn incrby(&self, key: String, delta: i64) -> anyhow::Result<i64> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::String(Bytes::from_static(b"0")),
            expires_at: None,
        });
        match &mut e.value {
            Value::String(b) => {
                let n: i64 = std::str::from_utf8(b)
                    .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                let result = n.checked_add(delta)
                    .ok_or_else(|| anyhow::anyhow!("ERR increment or decrement would overflow"))?;
                *b = Bytes::from(result.to_string().into_bytes());
                Ok(result)
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    // ── List operations ────────────────────────────────────────────────────

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> anyhow::Result<usize> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::List(List::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::List(l) => {
                for v in values { l.push_front(v); }
                Ok(l.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> anyhow::Result<usize> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::List(List::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::List(l) => {
                for v in values { l.push_back(v); }
                Ok(l.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn lpop(&self, key: &str, count: usize) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get_mut(key) {
            None => Ok(vec![]),
            Some(mut e) => match &mut e.value {
                Value::List(l) => Ok(l.pop_front_n(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn rpop(&self, key: &str, count: usize) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get_mut(key) {
            None => Ok(vec![]),
            Some(mut e) => match &mut e.value {
                Value::List(l) => Ok(l.pop_back_n(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn llen(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                Value::List(l) => Ok(l.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> anyhow::Result<Option<Bytes>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(e) => match &e.value {
                Value::List(l) => {
                    let len = l.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len { return Ok(None); }
                    Ok(l.get(idx as usize).cloned())
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::List(l) => {
                    let len = l.len() as i64;
                    let s = (if start < 0 { len + start } else { start }).max(0) as usize;
                    let e = (if stop < 0 { len + stop + 1 } else { stop + 1 }).min(len).max(0) as usize;
                    Ok(l.range(s, e))
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn lset(&self, key: &str, index: i64, val: Bytes) -> anyhow::Result<bool> {
        match self.data.get_mut(key) {
            None => Ok(false),
            Some(mut e) => match &mut e.value {
                Value::List(l) => {
                    let len = l.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        return Err(anyhow::anyhow!("ERR index out of range"));
                    }
                    Ok(l.set_index(idx as usize, val))
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn linsert(
        &self, key: &str, before: bool, pivot: Bytes, val: Bytes,
    ) -> anyhow::Result<i64> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::List(l) => {
                    let n = if before {
                        l.insert_before(&pivot, val)
                    } else {
                        l.insert_after(&pivot, val)
                    };
                    Ok(n)
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn lrem(&self, key: &str, count: i64, val: Bytes) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::List(l) => Ok(l.lrem(count, &val)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> anyhow::Result<()> {
        match self.data.get_mut(key) {
            None => Ok(()),
            Some(mut e) => match &mut e.value {
                Value::List(l) => { l.ltrim(start, stop); Ok(()) }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    /// LMOVE source dest wherefrom whereto.
    /// Returns the moved element, or None if source is empty.
    pub fn lmove(
        &self,
        src: &str,
        dst: &str,
        from_left: bool,
        to_left: bool,
    ) -> anyhow::Result<Option<Bytes>> {
        // Pop from source first (brief lock on src shard).
        let popped = {
            match self.data.get_mut(src) {
                None => return Ok(None),
                Some(mut e) => match &mut e.value {
                    Value::List(l) => {
                        if from_left { l.pop_front() } else { l.pop_back() }
                    }
                    _ => return Err(anyhow::anyhow!(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                    )),
                },
            }
        };
        let val = match popped {
            Some(v) => v,
            None => return Ok(None),
        };
        // Push to destination.
        {
            let mut e = self.data.entry(dst.to_string()).or_insert_with(|| Entry {
                value: Value::List(List::new()),
                expires_at: None,
            });
            match &mut e.value {
                Value::List(l) => {
                    if to_left { l.push_front(val.clone()); } else { l.push_back(val.clone()); }
                }
                _ => return Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            }
        }
        Ok(Some(val))
    }

    // ── Hash operations ────────────────────────────────────────────────────

    pub fn hset(&self, key: String, field: Bytes, value: Bytes) -> anyhow::Result<usize> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Hash(Hash::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Hash(h) => Ok(h.hset(field, value)),
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn hsetnx(&self, key: String, field: Bytes, value: Bytes) -> anyhow::Result<bool> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Hash(Hash::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Hash(h) => Ok(h.hsetnx(field, value)),
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn hget(&self, key: &str, field: &Bytes) -> anyhow::Result<Option<Bytes>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.get(field).cloned()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.all_pairs()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hdel(&self, key: &str, fields: &[Bytes]) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::Hash(h) => Ok(h.hdel(fields)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hlen(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hexists(&self, key: &str, field: &Bytes) -> anyhow::Result<bool> {
        match self.data.get(key) {
            None => Ok(false),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.contains(field)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hkeys(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.keys()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hvals(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::Hash(h) => Ok(h.values()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hincrby(&self, key: String, field: Bytes, delta: i64) -> anyhow::Result<i64> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Hash(Hash::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Hash(h) => {
                let old: i64 = match h.get(&field) {
                    None => 0,
                    Some(b) => std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.trim().parse().ok())
                        .ok_or_else(|| anyhow::anyhow!(
                            "ERR hash value is not an integer"
                        ))?,
                };
                let new_val = old.checked_add(delta)
                    .ok_or_else(|| anyhow::anyhow!("ERR increment would overflow"))?;
                h.hset(field, Bytes::from(new_val.to_string().into_bytes()));
                Ok(new_val)
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn hincrbyfloat(&self, key: String, field: Bytes, delta: f64) -> anyhow::Result<f64> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Hash(Hash::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Hash(h) => {
                let old: f64 = match h.get(&field) {
                    None => 0.0,
                    Some(b) => std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.trim().parse().ok())
                        .ok_or_else(|| anyhow::anyhow!(
                            "ERR hash value is not a float"
                        ))?,
                };
                let new_val = old + delta;
                let s = format!("{new_val}");
                h.hset(field, Bytes::from(s.into_bytes()));
                Ok(new_val)
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    // ── Set operations ─────────────────────────────────────────────────────

    pub fn sadd(&self, key: String, members: Vec<Bytes>) -> anyhow::Result<usize> {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Set(Set::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Set(s) => Ok(s.sadd(members)),
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn smembers(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::Set(s) => Ok(s.members()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn sismember(&self, key: &str, member: &Bytes) -> anyhow::Result<bool> {
        match self.data.get(key) {
            None => Ok(false),
            Some(e) => match &e.value {
                Value::Set(s) => Ok(s.contains(member)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn srem(&self, key: &str, members: &[Bytes]) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::Set(s) => Ok(s.srem(members)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn scard(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                Value::Set(s) => Ok(s.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    /// SMOVE: move member from src to dst atomically.
    pub fn smove(&self, src: &str, dst: &str, member: &Bytes) -> anyhow::Result<bool> {
        // Remove from source.
        let removed = {
            match self.data.get_mut(src) {
                None => return Ok(false),
                Some(mut e) => match &mut e.value {
                    Value::Set(s) => s.remove_one(member),
                    _ => return Err(anyhow::anyhow!(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                    )),
                },
            }
        };
        if !removed { return Ok(false); }
        // Add to destination.
        let mut e = self.data.entry(dst.to_string()).or_insert_with(|| Entry {
            value: Value::Set(Set::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::Set(s) => { s.sadd(std::iter::once(member.clone())); }
            _ => return Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
        Ok(true)
    }

    pub fn srandmember(&self, key: &str, count: i64) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(e) => match &e.value {
                Value::Set(s) => Ok(s.random_members(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn spop(&self, key: &str, count: usize) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get_mut(key) {
            None => Ok(vec![]),
            Some(mut e) => match &mut e.value {
                Value::Set(s) => Ok(s.spop(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    // Multi-key set operations: collect Sets by reading each key, compute
    // result, then write to destination if requested.

    fn get_set_clone(&self, key: &str) -> anyhow::Result<Option<Set>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(e) => match &e.value {
                Value::Set(s) => Ok(Some(s.clone())),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn sunion(&self, keys: &[String]) -> anyhow::Result<Vec<Bytes>> {
        let sets: anyhow::Result<Vec<Set>> = keys.iter()
            .map(|k| self.get_set_clone(k).map(|o| o.unwrap_or_else(Set::new)))
            .collect();
        let sets = sets?;
        if sets.is_empty() { return Ok(vec![]); }
        let others: Vec<&Set> = sets[1..].iter().collect();
        Ok(sets[0].union_with(&others).into_iter().collect())
    }

    pub fn sinter(&self, keys: &[String]) -> anyhow::Result<Vec<Bytes>> {
        let sets: anyhow::Result<Vec<Set>> = keys.iter()
            .map(|k| self.get_set_clone(k).map(|o| o.unwrap_or_else(Set::new)))
            .collect();
        let sets = sets?;
        if sets.is_empty() { return Ok(vec![]); }
        let others: Vec<&Set> = sets[1..].iter().collect();
        Ok(sets[0].intersect_with(&others).into_iter().collect())
    }

    pub fn sdiff(&self, keys: &[String]) -> anyhow::Result<Vec<Bytes>> {
        let sets: anyhow::Result<Vec<Set>> = keys.iter()
            .map(|k| self.get_set_clone(k).map(|o| o.unwrap_or_else(Set::new)))
            .collect();
        let sets = sets?;
        if sets.is_empty() { return Ok(vec![]); }
        let others: Vec<&Set> = sets[1..].iter().collect();
        Ok(sets[0].diff_with(&others).into_iter().collect())
    }

    pub fn sunionstore(&self, dst: &str, keys: &[String]) -> anyhow::Result<usize> {
        let result = self.sunion(keys)?;
        let count = result.len();
        self.data.insert(dst.to_string(), Entry {
            value: Value::Set({ let mut s = Set::new(); s.sadd(result); s }),
            expires_at: None,
        });
        Ok(count)
    }

    pub fn sinterstore(&self, dst: &str, keys: &[String]) -> anyhow::Result<usize> {
        let result = self.sinter(keys)?;
        let count = result.len();
        self.data.insert(dst.to_string(), Entry {
            value: Value::Set({ let mut s = Set::new(); s.sadd(result); s }),
            expires_at: None,
        });
        Ok(count)
    }

    pub fn sdiffstore(&self, dst: &str, keys: &[String]) -> anyhow::Result<usize> {
        let result = self.sdiff(keys)?;
        let count = result.len();
        self.data.insert(dst.to_string(), Entry {
            value: Value::Set({ let mut s = Set::new(); s.sadd(result); s }),
            expires_at: None,
        });
        Ok(count)
    }

    // ── Sorted-set operations ──────────────────────────────────────────────

    fn with_zset_mut<F, R>(&self, key: String, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut ZSet) -> anyhow::Result<R>,
    {
        let mut e = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::ZSet(ZSet::new()),
            expires_at: None,
        });
        match &mut e.value {
            Value::ZSet(z) => f(z),
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    fn with_zset<F, R>(&self, key: &str, default: R, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&ZSet) -> anyhow::Result<R>,
    {
        match self.data.get(key) {
            None => Ok(default),
            Some(e) => match &e.value {
                Value::ZSet(z) => f(z),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn zadd(
        &self, key: String, score: f64, member: Bytes,
        nx: bool, xx: bool, gt: bool, lt: bool,
    ) -> anyhow::Result<(usize, usize)> {
        self.with_zset_mut(key, |z| Ok(z.zadd(score, member, nx, xx, gt, lt)))
    }

    pub fn zscore(&self, key: &str, member: &Bytes) -> anyhow::Result<Option<f64>> {
        self.with_zset(key, None, |z| Ok(z.zscore(member)))
    }

    pub fn zrank(&self, key: &str, member: &Bytes) -> anyhow::Result<Option<usize>> {
        self.with_zset(key, None, |z| Ok(z.zrank(member)))
    }

    pub fn zrevrank(&self, key: &str, member: &Bytes) -> anyhow::Result<Option<usize>> {
        self.with_zset(key, None, |z| Ok(z.zrevrank(member)))
    }

    pub fn zincrby(&self, key: String, delta: f64, member: Bytes) -> anyhow::Result<f64> {
        self.with_zset_mut(key, |z| Ok(z.zincrby(delta, member)))
    }

    pub fn zrem(&self, key: &str, members: &[Bytes]) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::ZSet(z) => Ok(z.zrem(members)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn zcard(&self, key: &str) -> anyhow::Result<usize> {
        self.with_zset(key, 0, |z| Ok(z.len()))
    }

    pub fn zcount(&self, key: &str, min: ScoreBound, max: ScoreBound) -> anyhow::Result<usize> {
        self.with_zset(key, 0, |z| Ok(z.zcount(min, max)))
    }

    pub fn zrange(
        &self, key: &str, start: i64, stop: i64, rev: bool,
    ) -> anyhow::Result<Vec<(Bytes, f64)>> {
        self.with_zset(key, vec![], |z| Ok(z.zrange_by_rank(start, stop, rev)))
    }

    pub fn zrangebyscore(
        &self, key: &str, min: ScoreBound, max: ScoreBound,
        offset: usize, limit: Option<usize>, rev: bool,
    ) -> anyhow::Result<Vec<(Bytes, f64)>> {
        self.with_zset(key, vec![], |z| Ok(z.zrangebyscore(min, max, offset, limit, rev)))
    }

    pub fn zpopmin(&self, key: &str, count: usize) -> anyhow::Result<Vec<(Bytes, f64)>> {
        match self.data.get_mut(key) {
            None => Ok(vec![]),
            Some(mut e) => match &mut e.value {
                Value::ZSet(z) => Ok(z.zpopmin(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn zpopmax(&self, key: &str, count: usize) -> anyhow::Result<Vec<(Bytes, f64)>> {
        match self.data.get_mut(key) {
            None => Ok(vec![]),
            Some(mut e) => match &mut e.value {
                Value::ZSet(z) => Ok(z.zpopmax(count)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::ZSet(z) => Ok(z.zremrangebyrank(start, stop)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn zremrangebyscore(
        &self, key: &str, min: ScoreBound, max: ScoreBound,
    ) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut e) => match &mut e.value {
                Value::ZSet(z) => Ok(z.zremrangebyscore(min, max)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }
}

impl Default for Store {
    fn default() -> Self { Self::new() }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_del() {
        let s = Store::new();
        s.set("k".into(), Value::String(Bytes::from("v")), None);
        assert!(matches!(s.get("k"), Some(Value::String(b)) if b == "v"));
        assert_eq!(s.del(&["k".to_string()]), 1);
        assert!(s.get("k").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let s = Store::new();
        // expire_at is an absolute timestamp already in the past by the
        // time get() checks it, so this is deterministic given `now_ms()`
        // read once here -- no reliance on a store-internal clock read at
        // set-time (there isn't one anymore).
        s.set("k".into(), Value::String(Bytes::from("v")), Some(now_ms()));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(s.get("k").is_none());
    }

    #[test]
    fn test_expire_at_is_explicit_not_recomputed_per_replica() {
        // Two independent stores ("replicas") applying the same Set command
        // with the same absolute expire_at must agree on remaining TTL. The
        // old Instant-based design would have each replica compute its own
        // expiry relative to its own clock read at apply-time, which is
        // exactly the nondeterminism S5 (docs/invariants.md) forbids.
        let expire_at = now_ms() + 10_000;
        let a = Store::new();
        let b = Store::new();
        a.set("k".into(), Value::String(Bytes::from("v")), Some(expire_at));
        b.set("k".into(), Value::String(Bytes::from("v")), Some(expire_at));
        let diff = (a.pttl("k") - b.pttl("k")).abs();
        assert!(diff < 50, "pttl diverged by {diff}ms between replicas given identical expire_at");
    }

    #[test]
    fn test_incr() {
        let s = Store::new();
        assert_eq!(s.incrby("c".into(), 1).unwrap(), 1);
        assert_eq!(s.incrby("c".into(), 5).unwrap(), 6);
        assert_eq!(s.incrby("c".into(), -2).unwrap(), 4);
    }

    #[test]
    fn test_list_lpush_lrange() {
        let s = Store::new();
        s.lpush("l".into(), vec![Bytes::from("a")]).unwrap();
        s.lpush("l".into(), vec![Bytes::from("b"), Bytes::from("c")]).unwrap();
        let r = s.lrange("l", 0, -1).unwrap();
        assert_eq!(r[0], "c");
        assert_eq!(r[2], "a");
    }

    #[test]
    fn test_lset_linsert_lrem_ltrim() {
        let s = Store::new();
        s.rpush("l".into(), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        assert!(s.lset("l", 1, Bytes::from("B")).unwrap());
        assert_eq!(s.lindex("l", 1).unwrap(), Some(Bytes::from("B")));

        s.linsert("l", true, Bytes::from("B"), Bytes::from("X")).unwrap();
        assert_eq!(s.lrange("l", 0, -1).unwrap().len(), 4);

        s.ltrim("l", 1, -1).unwrap();
        assert_eq!(s.lrange("l", 0, 0).unwrap(), vec![Bytes::from("X")]);
    }

    #[test]
    fn test_lmove() {
        let s = Store::new();
        s.rpush("src".into(), vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let moved = s.lmove("src", "dst", true, false).unwrap();
        assert_eq!(moved, Some(Bytes::from("a")));
        assert_eq!(s.llen("src").unwrap(), 1);
        assert_eq!(s.llen("dst").unwrap(), 1);
    }

    #[test]
    fn test_hash_ops() {
        let s = Store::new();
        assert_eq!(s.hset("h".into(), Bytes::from("f1"), Bytes::from("v1")).unwrap(), 1);
        assert_eq!(s.hset("h".into(), Bytes::from("f1"), Bytes::from("v2")).unwrap(), 0);
        assert_eq!(s.hget("h", &Bytes::from("f1")).unwrap(), Some(Bytes::from("v2")));
        assert_eq!(s.hincrby("h".into(), Bytes::from("num"), 5).unwrap(), 5);
        assert_eq!(s.hincrby("h".into(), Bytes::from("num"), -2).unwrap(), 3);
    }

    #[test]
    fn test_set_algebra() {
        let s = Store::new();
        s.sadd("a".into(), vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]).unwrap();
        s.sadd("b".into(), vec![Bytes::from("2"), Bytes::from("3"), Bytes::from("4")]).unwrap();
        let mut u = s.sunion(&["a".to_string(), "b".to_string()]).unwrap();
        u.sort(); assert_eq!(u.len(), 4);
        let mut i = s.sinter(&["a".to_string(), "b".to_string()]).unwrap();
        i.sort(); assert_eq!(i.len(), 2);
        let d = s.sdiff(&["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn test_zset_basic() {
        let s = Store::new();
        s.zadd("z".into(), 1.0, Bytes::from("a"), false, false, false, false).unwrap();
        s.zadd("z".into(), 3.0, Bytes::from("c"), false, false, false, false).unwrap();
        s.zadd("z".into(), 2.0, Bytes::from("b"), false, false, false, false).unwrap();
        assert_eq!(s.zcard("z").unwrap(), 3);
        assert_eq!(s.zscore("z", &Bytes::from("b")).unwrap(), Some(2.0));
        assert_eq!(s.zrank("z", &Bytes::from("a")).unwrap(), Some(0));
        assert_eq!(s.zrevrank("z", &Bytes::from("a")).unwrap(), Some(2));
        let range = s.zrange("z", 0, -1, false).unwrap();
        assert_eq!(range[0].0, "a");
        assert_eq!(range[2].0, "c");
    }

    #[test]
    fn test_wrongtype_error() {
        let s = Store::new();
        s.set("k".into(), Value::String(Bytes::from("v")), None);
        assert!(s.lpush("k".into(), vec![Bytes::from("x")]).is_err());
        assert!(s.sadd("k".into(), vec![Bytes::from("x")]).is_err());
        assert!(s.zadd("k".into(), 1.0, Bytes::from("x"), false, false, false, false).is_err());
    }
}
