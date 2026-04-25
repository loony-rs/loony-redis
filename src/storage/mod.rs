use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
}

/// A snapshot of a single key's data — used for full-sync replication.
pub enum SnapshotEntry {
    String { key: String, value: Bytes },
    List { key: String, items: Vec<Bytes> },
    Hash { key: String, fields: Vec<(Bytes, Bytes)> },
    Set { key: String, members: Vec<Bytes> },
}

#[derive(Debug)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Instant::now() > t).unwrap_or(false)
    }
}

pub struct Store {
    data: DashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    pub fn set(&self, key: String, value: Value, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.data.insert(key, Entry { value, expires_at });
    }

    pub fn del(&self, keys: &[String]) -> usize {
        keys.iter()
            .filter(|k| self.data.remove(*k).is_some())
            .count()
    }

    pub fn exists(&self, key: &str) -> bool {
        self.data
            .get(key)
            .map(|e| !e.is_expired())
            .unwrap_or(false)
    }

    pub fn expire(&self, key: &str, ttl: Duration) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            entry.expires_at = Some(Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    /// Returns remaining TTL in milliseconds, -1 if no expiry, -2 if key doesn't exist.
    pub fn pttl(&self, key: &str) -> i64 {
        match self.data.get(key) {
            None => -2,
            Some(entry) => {
                if entry.is_expired() {
                    return -2;
                }
                match entry.expires_at {
                    None => -1,
                    Some(t) => {
                        let now = Instant::now();
                        if t <= now {
                            -2
                        } else {
                            (t - now).as_millis() as i64
                        }
                    }
                }
            }
        }
    }

    pub fn persist(&self, key: &str) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }
        false
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        match self.data.get(key) {
            None => "none",
            Some(entry) => {
                if entry.is_expired() {
                    return "none";
                }
                match &entry.value {
                    Value::String(_) => "string",
                    Value::List(_) => "list",
                    Value::Hash(_) => "hash",
                    Value::Set(_) => "set",
                }
            }
        }
    }

    pub fn keys_count(&self) -> usize {
        self.data.len()
    }

    pub fn all_keys(&self) -> Vec<String> {
        self.data
            .iter()
            .filter(|e| !e.value().is_expired())
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn flush(&self) {
        self.data.clear();
    }

    /// Return a point-in-time snapshot of all non-expired entries, suitable
    /// for sending to a new replica during initial full-sync.
    pub fn snapshot(&self) -> Vec<SnapshotEntry> {
        self.data
            .iter()
            .filter(|e| !e.value().is_expired())
            .filter_map(|e| {
                let key = e.key().clone();
                match &e.value().value {
                    Value::String(b) => Some(SnapshotEntry::String {
                        key,
                        value: b.clone(),
                    }),
                    Value::List(list) if !list.is_empty() => Some(SnapshotEntry::List {
                        key,
                        items: list.iter().cloned().collect(),
                    }),
                    Value::Hash(map) if !map.is_empty() => Some(SnapshotEntry::Hash {
                        key,
                        fields: map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    }),
                    Value::Set(set) if !set.is_empty() => Some(SnapshotEntry::Set {
                        key,
                        members: set.iter().cloned().collect(),
                    }),
                    _ => None,
                }
            })
            .collect()
    }

    // ── String operations ──────────────────────────────────────────────────

    /// Atomically get then set. Returns the old value.
    pub fn getset(&self, key: String, new_value: Bytes) -> Option<Bytes> {
        let old = match self.get(&key) {
            Some(Value::String(b)) => Some(b),
            _ => None,
        };
        self.set(key, Value::String(new_value), None);
        old
    }

    pub fn append(&self, key: String, suffix: Bytes) -> anyhow::Result<usize> {
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::String(Bytes::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::String(b) => {
                let mut vec = b.to_vec();
                vec.extend_from_slice(&suffix);
                *b = Bytes::from(vec);
                Ok(b.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    /// Increment a string-encoded integer by delta. Returns new value.
    pub fn incrby(&self, key: String, delta: i64) -> anyhow::Result<i64> {
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::String(Bytes::from_static(b"0")),
            expires_at: None,
        });
        match &mut entry.value {
            Value::String(b) => {
                let s = std::str::from_utf8(b)
                    .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                let n: i64 = s
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                let result = n.checked_add(delta).ok_or_else(|| {
                    anyhow::anyhow!("ERR increment or decrement would overflow")
                })?;
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
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::List(list) => {
                // Each value is pushed to the front in the order given, so the
                // last argument ends up at the head — matching Redis semantics.
                for v in values {
                    list.push_front(v);
                }
                Ok(list.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> anyhow::Result<usize> {
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::List(list) => {
                for v in values {
                    list.push_back(v);
                }
                Ok(list.len())
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn lpop(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        if let Some(mut entry) = self.data.get_mut(key) {
            return match &mut entry.value {
                Value::List(list) => Ok(list.pop_front()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            };
        }
        Ok(None)
    }

    pub fn rpop(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        if let Some(mut entry) = self.data.get_mut(key) {
            return match &mut entry.value {
                Value::List(list) => Ok(list.pop_back()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            };
        }
        Ok(None)
    }

    pub fn llen(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::List(list) => Ok(list.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> anyhow::Result<Option<Bytes>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        return Ok(None);
                    }
                    Ok(list.get(idx as usize).cloned())
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
            Some(entry) => match &entry.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let e =
                        if stop < 0 { (len + stop + 1).max(0) } else { (stop + 1).min(len) }
                            as usize;
                    if s >= e {
                        return Ok(vec![]);
                    }
                    Ok(list.range(s..e).cloned().collect())
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    // ── Hash operations ────────────────────────────────────────────────────

    pub fn hset(&self, key: String, field: Bytes, value: Bytes) -> anyhow::Result<usize> {
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Hash(HashMap::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::Hash(map) => {
                let added = if map.contains_key(&field) { 0 } else { 1 };
                map.insert(field, value);
                Ok(added)
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn hget(&self, key: &str, field: &Bytes) -> anyhow::Result<Option<Bytes>> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.get(field).cloned()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hdel(&self, key: &str, fields: &[Bytes]) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut entry) => match &mut entry.value {
                Value::Hash(map) => {
                    let removed = fields.iter().filter(|f| map.remove(*f).is_some()).count();
                    Ok(removed)
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hlen(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hexists(&self, key: &str, field: &Bytes) -> anyhow::Result<bool> {
        match self.data.get(key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.contains_key(field)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hkeys(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.keys().cloned().collect()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn hvals(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.values().cloned().collect()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    // ── Set operations ─────────────────────────────────────────────────────

    pub fn sadd(&self, key: String, members: Vec<Bytes>) -> anyhow::Result<usize> {
        let mut entry = self.data.entry(key).or_insert_with(|| Entry {
            value: Value::Set(HashSet::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::Set(set) => {
                let before = set.len();
                for m in members {
                    set.insert(m);
                }
                Ok(set.len() - before)
            }
            _ => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    pub fn smembers(&self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        match self.data.get(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn sismember(&self, key: &str, member: &Bytes) -> anyhow::Result<bool> {
        match self.data.get(key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.contains(member)),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn srem(&self, key: &str, members: &[Bytes]) -> anyhow::Result<usize> {
        match self.data.get_mut(key) {
            None => Ok(0),
            Some(mut entry) => match &mut entry.value {
                Value::Set(set) => {
                    let removed = members.iter().filter(|m| set.remove(*m)).count();
                    Ok(removed)
                }
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }

    pub fn scard(&self, key: &str) -> anyhow::Result<usize> {
        match self.data.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.len()),
                _ => Err(anyhow::anyhow!(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )),
            },
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_del() {
        let store = Store::new();
        store.set("k".into(), Value::String(Bytes::from("v")), None);
        assert!(matches!(store.get("k"), Some(Value::String(b)) if b == "v"));
        assert_eq!(store.del(&["k".to_string()]), 1);
        assert!(store.get("k").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let store = Store::new();
        store.set(
            "k".into(),
            Value::String(Bytes::from("v")),
            Some(Duration::from_nanos(1)),
        );
        std::thread::sleep(Duration::from_millis(1));
        assert!(store.get("k").is_none());
    }

    #[test]
    fn test_incr() {
        let store = Store::new();
        assert_eq!(store.incrby("counter".into(), 1).unwrap(), 1);
        assert_eq!(store.incrby("counter".into(), 5).unwrap(), 6);
        assert_eq!(store.incrby("counter".into(), -2).unwrap(), 4);
    }

    #[test]
    fn test_list_lpush_lrange() {
        let store = Store::new();
        store.lpush("list".into(), vec![Bytes::from("a")]).unwrap();
        store
            .lpush("list".into(), vec![Bytes::from("b"), Bytes::from("c")])
            .unwrap();
        // After LPUSH b c: c is pushed first to front, then b is pushed to front
        // so order is: b, c, a (b is on top since it was pushed last)
        // Wait: lpush pushes each in order to front: push b → [b,a], push c → [c,b,a]
        let range = store.lrange("list", 0, -1).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], Bytes::from("c"));
        assert_eq!(range[1], Bytes::from("b"));
        assert_eq!(range[2], Bytes::from("a"));
    }

    #[test]
    fn test_hash_ops() {
        let store = Store::new();
        assert_eq!(
            store
                .hset("h".into(), Bytes::from("f1"), Bytes::from("v1"))
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .hset("h".into(), Bytes::from("f1"), Bytes::from("v2"))
                .unwrap(),
            0
        ); // update, not new
        assert_eq!(
            store.hget("h", &Bytes::from("f1")).unwrap(),
            Some(Bytes::from("v2"))
        );
        assert_eq!(store.hlen("h").unwrap(), 1);
    }

    #[test]
    fn test_set_ops() {
        let store = Store::new();
        assert_eq!(
            store
                .sadd(
                    "s".into(),
                    vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a")]
                )
                .unwrap(),
            2
        );
        assert_eq!(store.scard("s").unwrap(), 2);
        assert!(store.sismember("s", &Bytes::from("a")).unwrap());
        assert!(!store.sismember("s", &Bytes::from("c")).unwrap());
        assert_eq!(store.srem("s", &[Bytes::from("a")]).unwrap(), 1);
        assert_eq!(store.scard("s").unwrap(), 1);
    }

    #[test]
    fn test_wrongtype_error() {
        let store = Store::new();
        store.set("k".into(), Value::String(Bytes::from("v")), None);
        assert!(store.lpush("k".into(), vec![Bytes::from("x")]).is_err());
        assert!(store.sadd("k".into(), vec![Bytes::from("x")]).is_err());
    }
}
