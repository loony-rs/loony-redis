use bytes::Bytes;
use rand::seq::SliceRandom;
use std::collections::HashSet;

const MAX_INTSET_ENTRIES: usize = 128;
const MAX_ELEMENT_BYTES: usize = 64;

/// Dual-encoding set: compact `Vec` for small sets, `HashSet` once thresholds
/// are exceeded.
#[derive(Debug, Clone)]
pub enum Set {
    Small(Vec<Bytes>),
    Large(HashSet<Bytes>),
}

impl Set {
    pub fn new() -> Self {
        Set::Small(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Set::Small(v) => v.len(),
            Set::Large(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn needs_promote(&self, member: &Bytes) -> bool {
        match self {
            Set::Small(v) => {
                v.len() >= MAX_INTSET_ENTRIES || member.len() > MAX_ELEMENT_BYTES
            }
            Set::Large(_) => false,
        }
    }

    fn promote(&mut self) {
        if let Set::Small(v) = self {
            let set: HashSet<Bytes> = v.drain(..).collect();
            *self = Set::Large(set);
        }
    }

    /// SADD: returns count of newly added members.
    pub fn sadd(&mut self, members: impl IntoIterator<Item = Bytes>) -> usize {
        let mut added = 0usize;
        for m in members {
            if self.needs_promote(&m) {
                self.promote();
            }
            match self {
                Set::Small(v) => {
                    if !v.contains(&m) {
                        v.push(m);
                        added += 1;
                    }
                }
                Set::Large(s) => {
                    if s.insert(m) {
                        added += 1;
                    }
                }
            }
        }
        added
    }

    pub fn contains(&self, member: &Bytes) -> bool {
        match self {
            Set::Small(v) => v.contains(member),
            Set::Large(s) => s.contains(member),
        }
    }

    pub fn srem(&mut self, members: &[Bytes]) -> usize {
        match self {
            Set::Small(v) => {
                let before = v.len();
                v.retain(|m| !members.contains(m));
                before - v.len()
            }
            Set::Large(s) => members.iter().filter(|m| s.remove(*m)).count(),
        }
    }

    /// Remove a single specific member. Returns whether it existed.
    pub fn remove_one(&mut self, member: &Bytes) -> bool {
        match self {
            Set::Small(v) => match v.iter().position(|m| m == member) {
                Some(i) => { v.swap_remove(i); true }
                None => false,
            },
            Set::Large(s) => s.remove(member),
        }
    }

    pub fn members(&self) -> Vec<Bytes> {
        match self {
            Set::Small(v) => v.clone(),
            Set::Large(s) => s.iter().cloned().collect(),
        }
    }

    pub fn to_hashset(&self) -> HashSet<Bytes> {
        match self {
            Set::Small(v) => v.iter().cloned().collect(),
            Set::Large(s) => s.clone(),
        }
    }

    // ── Set algebra ───────────────────────────────────────────────────────────

    pub fn union_with(&self, others: &[&Set]) -> HashSet<Bytes> {
        let mut result = self.to_hashset();
        for other in others {
            result.extend(other.members());
        }
        result
    }

    pub fn intersect_with(&self, others: &[&Set]) -> HashSet<Bytes> {
        let mut result = self.to_hashset();
        for other in others {
            let rhs = other.to_hashset();
            result.retain(|m| rhs.contains(m));
        }
        result
    }

    pub fn diff_with(&self, others: &[&Set]) -> HashSet<Bytes> {
        let mut result = self.to_hashset();
        for other in others {
            for m in other.members() {
                result.remove(&m);
            }
        }
        result
    }

    // ── Random access ─────────────────────────────────────────────────────────

    /// SRANDMEMBER:
    /// - count >= 0: up to `count` distinct members (no duplicates).
    /// - count < 0: exactly `|count|` members, duplicates allowed.
    pub fn random_members(&self, count: i64) -> Vec<Bytes> {
        let members = self.members();
        if members.is_empty() {
            return vec![];
        }
        let mut rng = rand::thread_rng();
        if count >= 0 {
            let n = (count as usize).min(members.len());
            members.choose_multiple(&mut rng, n).cloned().collect()
        } else {
            let n = (-count) as usize;
            (0..n).map(|_| members.choose(&mut rng).unwrap().clone()).collect()
        }
    }

    /// SPOP: remove and return `count` random members.
    pub fn spop(&mut self, count: usize) -> Vec<Bytes> {
        let mut members = self.members();
        if members.is_empty() {
            return vec![];
        }
        let mut rng = rand::thread_rng();
        members.shuffle(&mut rng);
        let n = count.min(members.len());
        let popped: Vec<Bytes> = members[..n].to_vec();
        for m in &popped {
            self.remove_one(m);
        }
        popped
    }
}

impl Default for Set {
    fn default() -> Self { Set::new() }
}
