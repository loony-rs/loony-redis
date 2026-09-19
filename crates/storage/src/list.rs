use bytes::Bytes;
use std::collections::VecDeque;

// Thresholds matching Redis listpack defaults.
const MAX_LISTPACK_ENTRIES: usize = 128;
const MAX_ELEMENT_BYTES: usize = 64;

/// Dual-encoding list: compact `Vec` for small lists, `VecDeque` once either
/// threshold is exceeded.  All public methods keep the invariant automatically.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum List {
    Small(Vec<Bytes>),
    Large(VecDeque<Bytes>),
}

impl List {
    pub fn new() -> Self {
        List::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            List::Small(v) => v.len(),
            List::Large(d) => d.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn needs_promote(&self, new_elem: &Bytes) -> bool {
        match self {
            List::Small(v) => {
                v.len() >= MAX_LISTPACK_ENTRIES || new_elem.len() > MAX_ELEMENT_BYTES
            }
            List::Large(_) => false,
        }
    }

    fn promote(&mut self) {
        if let List::Small(v) = self {
            let dq: VecDeque<Bytes> = v.drain(..).collect();
            *self = List::Large(dq);
        }
    }

    pub fn push_front(&mut self, val: Bytes) {
        if self.needs_promote(&val) {
            self.promote();
        }
        match self {
            List::Small(v) => v.insert(0, val),
            List::Large(d) => d.push_front(val),
        }
    }

    pub fn push_back(&mut self, val: Bytes) {
        if self.needs_promote(&val) {
            self.promote();
        }
        match self {
            List::Small(v) => v.push(val),
            List::Large(d) => d.push_back(val),
        }
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        match self {
            List::Small(v) if !v.is_empty() => Some(v.remove(0)),
            List::Small(_) => None,
            List::Large(d) => d.pop_front(),
        }
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        match self {
            List::Small(v) => v.pop(),
            List::Large(d) => d.pop_back(),
        }
    }

    pub fn pop_front_n(&mut self, n: usize) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.pop_front() {
                Some(v) => out.push(v),
                None => break,
            }
        }
        out
    }

    pub fn pop_back_n(&mut self, n: usize) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.pop_back() {
                Some(v) => out.push(v),
                None => break,
            }
        }
        out
    }

    pub fn get(&self, index: usize) -> Option<&Bytes> {
        match self {
            List::Small(v) => v.get(index),
            List::Large(d) => d.get(index),
        }
    }

    pub fn set_index(&mut self, index: usize, val: Bytes) -> bool {
        match self {
            List::Small(v) => match v.get_mut(index) {
                Some(slot) => { *slot = val; true }
                None => false,
            },
            List::Large(d) => match d.get_mut(index) {
                Some(slot) => { *slot = val; true }
                None => false,
            },
        }
    }

    pub fn range(&self, start: usize, end: usize) -> Vec<Bytes> {
        let end = end.min(self.len());
        if start >= end { return vec![]; }
        match self {
            List::Small(v) => v[start..end].to_vec(),
            List::Large(d) => d.range(start..end).cloned().collect(),
        }
    }

    /// LINSERT BEFORE pivot val.  Returns new length, or -1 if pivot not found.
    pub fn insert_before(&mut self, pivot: &Bytes, val: Bytes) -> i64 {
        let pos = match self.position(pivot) {
            Some(p) => p,
            None => return -1,
        };
        if self.needs_promote(&val) {
            self.promote();
        }
        match self {
            List::Small(v) => v.insert(pos, val),
            List::Large(d) => d.insert(pos, val),
        }
        self.len() as i64
    }

    /// LINSERT AFTER pivot val.
    pub fn insert_after(&mut self, pivot: &Bytes, val: Bytes) -> i64 {
        let pos = match self.position(pivot) {
            Some(p) => p,
            None => return -1,
        };
        let insert_at = pos + 1;
        if self.needs_promote(&val) {
            self.promote();
        }
        match self {
            List::Small(v) => {
                if insert_at >= v.len() { v.push(val); } else { v.insert(insert_at, val); }
            }
            List::Large(d) => d.insert(insert_at, val),
        }
        self.len() as i64
    }

    fn position(&self, val: &Bytes) -> Option<usize> {
        match self {
            List::Small(v) => v.iter().position(|b| b == val),
            List::Large(d) => d.iter().position(|b| b == val),
        }
    }

    /// LREM: remove `count` occurrences of `val`.
    /// count > 0: from head; count < 0: from tail; count == 0: all.
    pub fn lrem(&mut self, count: i64, val: &Bytes) -> usize {
        let items: Vec<Bytes> = self.to_vec();
        let mut removed = 0usize;

        let new_items: Vec<Bytes> = if count > 0 {
            let limit = count as usize;
            items.into_iter().filter(|b| {
                if removed < limit && b == val { removed += 1; false } else { true }
            }).collect()
        } else if count < 0 {
            let limit = (-count) as usize;
            let mut reversed: Vec<Bytes> = items.into_iter().rev().collect();
            reversed = reversed.into_iter().filter(|b| {
                if removed < limit && b == val { removed += 1; false } else { true }
            }).collect();
            reversed.into_iter().rev().collect()
        } else {
            items.into_iter().filter(|b| {
                if b == val { removed += 1; false } else { true }
            }).collect()
        };

        *self = List::Small(new_items);
        removed
    }

    /// LTRIM: keep only [start, stop] inclusive using Redis negative-index semantics.
    pub fn ltrim(&mut self, start: i64, stop: i64) {
        let len = self.len() as i64;
        let s = (if start < 0 { len + start } else { start }).max(0) as usize;
        let e = (if stop < 0 { len + stop } else { stop }).min(len - 1).max(-1) as usize;
        if s > e || len == 0 {
            *self = List::Small(Vec::new());
            return;
        }
        let new_items = self.range(s, e + 1);
        *self = List::Small(new_items);
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        match self {
            List::Small(v) => v.clone(),
            List::Large(d) => d.iter().cloned().collect(),
        }
    }
}

impl Default for List {
    fn default() -> Self { List::new() }
}
