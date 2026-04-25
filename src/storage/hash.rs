use bytes::Bytes;
use std::collections::HashMap;

const MAX_LISTPACK_ENTRIES: usize = 128;
const MAX_ELEMENT_BYTES: usize = 64;

/// Dual-encoding hash: flat `Vec` of pairs for small hashes (O(n) but
/// cache-friendly and allocation-cheap), `HashMap` once thresholds are crossed.
#[derive(Debug, Clone)]
pub enum Hash {
    Small(Vec<(Bytes, Bytes)>),
    Large(HashMap<Bytes, Bytes>),
}

impl Hash {
    pub fn new() -> Self {
        Hash::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Hash::Small(v) => v.len(),
            Hash::Large(m) => m.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn needs_promote(&self, field: &Bytes, value: &Bytes) -> bool {
        match self {
            Hash::Small(v) => {
                v.len() >= MAX_LISTPACK_ENTRIES
                    || field.len() > MAX_ELEMENT_BYTES
                    || value.len() > MAX_ELEMENT_BYTES
            }
            Hash::Large(_) => false,
        }
    }

    fn promote(&mut self) {
        if let Hash::Small(v) = self {
            let map: HashMap<Bytes, Bytes> = v.drain(..).collect();
            *self = Hash::Large(map);
        }
    }

    /// Set field. Returns 1 if new field was added, 0 if existing was updated.
    pub fn hset(&mut self, field: Bytes, value: Bytes) -> usize {
        if self.needs_promote(&field, &value) {
            self.promote();
        }
        match self {
            Hash::Small(v) => {
                if let Some(pair) = v.iter_mut().find(|(k, _)| k == &field) {
                    pair.1 = value;
                    0
                } else {
                    v.push((field, value));
                    1
                }
            }
            Hash::Large(m) => {
                let is_new = !m.contains_key(&field);
                m.insert(field, value);
                if is_new { 1 } else { 0 }
            }
        }
    }

    /// HSETNX: set only if field does not exist. Returns true if set.
    pub fn hsetnx(&mut self, field: Bytes, value: Bytes) -> bool {
        if self.contains(&field) {
            return false;
        }
        self.hset(field, value);
        true
    }

    pub fn get(&self, field: &Bytes) -> Option<&Bytes> {
        match self {
            Hash::Small(v) => v.iter().find(|(k, _)| k == field).map(|(_, v)| v),
            Hash::Large(m) => m.get(field),
        }
    }

    pub fn get_mut(&mut self, field: &Bytes) -> Option<&mut Bytes> {
        match self {
            Hash::Small(v) => v.iter_mut().find(|(k, _)| k == field).map(|(_, v)| v),
            Hash::Large(m) => m.get_mut(field),
        }
    }

    pub fn contains(&self, field: &Bytes) -> bool {
        match self {
            Hash::Small(v) => v.iter().any(|(k, _)| k == field),
            Hash::Large(m) => m.contains_key(field),
        }
    }

    pub fn hdel(&mut self, fields: &[Bytes]) -> usize {
        match self {
            Hash::Small(v) => {
                let before = v.len();
                v.retain(|(k, _)| !fields.contains(k));
                before - v.len()
            }
            Hash::Large(m) => fields.iter().filter(|f| m.remove(*f).is_some()).count(),
        }
    }

    pub fn all_pairs(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            Hash::Small(v) => v.clone(),
            Hash::Large(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    pub fn keys(&self) -> Vec<Bytes> {
        match self {
            Hash::Small(v) => v.iter().map(|(k, _)| k.clone()).collect(),
            Hash::Large(m) => m.keys().cloned().collect(),
        }
    }

    pub fn values(&self) -> Vec<Bytes> {
        match self {
            Hash::Small(v) => v.iter().map(|(_, v)| v.clone()).collect(),
            Hash::Large(m) => m.values().cloned().collect(),
        }
    }
}

impl Default for Hash {
    fn default() -> Self { Hash::new() }
}
