use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

// ── Score ordering ─────────────────────────────────────────────────────────
//
// f64 doesn't implement Ord, so we reinterpret bits as u64 using the standard
// sign-magnitude → two's-complement trick so that the u64 ordering matches the
// float ordering:
//
//   positive (sign=0): set sign bit → 1xxx…  (now sorts after negatives)
//   negative (sign=1): flip all bits → 0yyy… (reverses the negative sub-range)
//
// Result: -inf < negative floats < -0.0 < +0.0 < positive floats < +inf.
// NaN is rejected at the command layer; it never enters a ZSet.

fn encode_score(score: f64) -> u64 {
    let bits = score.to_bits();
    if bits >> 63 == 0 { bits | (1u64 << 63) } else { !bits }
}

fn decode_score(encoded: u64) -> f64 {
    let bits = if encoded >> 63 != 0 {
        encoded ^ (1u64 << 63)
    } else {
        !encoded
    };
    f64::from_bits(bits)
}

/// BTreeMap key: (encoded_score, member).  Enables O(log n) rank and range queries.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct ScoreKey {
    encoded: u64,
    member: Bytes,
}

impl ScoreKey {
    fn new(score: f64, member: Bytes) -> Self {
        ScoreKey { encoded: encode_score(score), member }
    }
    fn score(&self) -> f64 {
        decode_score(self.encoded)
    }
    /// A lower-bound key at `score` that sorts before any real member at that score
    /// (empty Bytes sorts before any non-empty Bytes).
    fn lower(score: f64) -> Self {
        ScoreKey { encoded: encode_score(score), member: Bytes::new() }
    }
}

// ── Score range bounds ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub enum ScoreBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

impl ScoreBound {
    /// Parse a ZRANGEBYSCORE min/max token.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "-inf" => Some(ScoreBound::NegInf),
            "+inf" | "inf" => Some(ScoreBound::PosInf),
            s if s.starts_with('(') => s[1..].parse::<f64>().ok().map(ScoreBound::Exclusive),
            s => s.parse::<f64>().ok().map(ScoreBound::Inclusive),
        }
    }

    /// Does `score` satisfy this bound when used as a lower bound (min)?
    pub fn satisfies_lower(self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => score == f64::INFINITY,
            ScoreBound::Inclusive(min) => score >= min,
            ScoreBound::Exclusive(min) => score > min,
        }
    }

    /// Does `score` satisfy this bound when used as an upper bound (max)?
    pub fn satisfies_upper(self, score: f64) -> bool {
        match self {
            ScoreBound::PosInf => true,
            ScoreBound::NegInf => score == f64::NEG_INFINITY,
            ScoreBound::Inclusive(max) => score <= max,
            ScoreBound::Exclusive(max) => score < max,
        }
    }
}

// ── ZSet ───────────────────────────────────────────────────────────────────

/// Sorted set with dual-index:
///   `scores`   – O(1) member → score lookup
///   `by_score` – ordered BTree for rank/range queries, key = (encoded_score, member)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ZSet {
    scores: HashMap<Bytes, f64>,
    by_score: BTreeMap<ScoreKey, ()>,
}

impl ZSet {
    pub fn new() -> Self {
        ZSet { scores: HashMap::new(), by_score: BTreeMap::new() }
    }

    pub fn len(&self) -> usize { self.scores.len() }
    pub fn is_empty(&self) -> bool { self.scores.is_empty() }

    // ── ZADD ─────────────────────────────────────────────────────────────────

    /// Flags: nx=only add, xx=only update, gt/lt=conditional update.
    /// Returns (added_count, changed_count).
    pub fn zadd(
        &mut self,
        score: f64,
        member: Bytes,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> (usize, usize) {
        if score.is_nan() {
            return (0, 0);
        }
        if let Some(&old) = self.scores.get(&member) {
            if nx {
                return (0, 0);
            }
            let new_score = if gt {
                if score > old { score } else { old }
            } else if lt {
                if score < old { score } else { old }
            } else {
                score
            };
            if (new_score - old).abs() < f64::EPSILON * old.abs().max(1.0)
                && new_score == old
            {
                return (0, 0);
            }
            self.by_score.remove(&ScoreKey::new(old, member.clone()));
            self.scores.insert(member.clone(), new_score);
            self.by_score.insert(ScoreKey::new(new_score, member), ());
            (0, 1)
        } else {
            if xx {
                return (0, 0);
            }
            self.scores.insert(member.clone(), score);
            self.by_score.insert(ScoreKey::new(score, member), ());
            (1, 1)
        }
    }

    // ── Score / rank lookup ───────────────────────────────────────────────────

    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn zrank(&self, member: &Bytes) -> Option<usize> {
        let score = self.scores.get(member)?;
        let key = ScoreKey::new(*score, member.clone());
        Some(self.by_score.range(..=key).count() - 1)
    }

    pub fn zrevrank(&self, member: &Bytes) -> Option<usize> {
        let rank = self.zrank(member)?;
        Some(self.len() - 1 - rank)
    }

    // ── ZINCRBY ──────────────────────────────────────────────────────────────

    pub fn zincrby(&mut self, delta: f64, member: Bytes) -> f64 {
        let old = self.scores.get(&member).copied().unwrap_or(0.0);
        let new_score = old + delta;
        if self.scores.contains_key(&member) {
            self.by_score.remove(&ScoreKey::new(old, member.clone()));
        }
        self.scores.insert(member.clone(), new_score);
        self.by_score.insert(ScoreKey::new(new_score, member), ());
        new_score
    }

    // ── ZREM ─────────────────────────────────────────────────────────────────

    pub fn zrem(&mut self, members: &[Bytes]) -> usize {
        let mut n = 0;
        for m in members {
            if let Some(score) = self.scores.remove(m) {
                self.by_score.remove(&ScoreKey::new(score, m.clone()));
                n += 1;
            }
        }
        n
    }

    // ── Range by rank ─────────────────────────────────────────────────────────

    /// ZRANGE start stop [REV] [WITHSCORES] — by rank.
    pub fn zrange_by_rank(&self, start: i64, stop: i64, rev: bool) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        let s = (if start < 0 { len + start } else { start }).max(0) as usize;
        let e = (if stop < 0 { len + stop + 1 } else { stop + 1 }).min(len).max(0) as usize;
        if s >= e {
            return vec![];
        }
        let items: Vec<(Bytes, f64)> = self.by_score.keys()
            .skip(s)
            .take(e - s)
            .map(|k| (k.member.clone(), k.score()))
            .collect();
        if rev { items.into_iter().rev().collect() } else { items }
    }

    // ── Range by score ────────────────────────────────────────────────────────

    pub fn zrangebyscore(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        offset: usize,
        limit: Option<usize>,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        let mut items: Vec<(Bytes, f64)> = self.by_score.keys()
            .filter(|k| {
                let s = k.score();
                min.satisfies_lower(s) && max.satisfies_upper(s)
            })
            .map(|k| (k.member.clone(), k.score()))
            .collect();
        if rev {
            items.reverse();
        }
        let items: Vec<_> = items.into_iter().skip(offset).collect();
        match limit {
            Some(n) => items.into_iter().take(n).collect(),
            None => items,
        }
    }

    // ── ZCOUNT ───────────────────────────────────────────────────────────────

    pub fn zcount(&self, min: ScoreBound, max: ScoreBound) -> usize {
        self.by_score.keys()
            .filter(|k| {
                let s = k.score();
                min.satisfies_lower(s) && max.satisfies_upper(s)
            })
            .count()
    }

    // ── ZPOPMIN / ZPOPMAX ─────────────────────────────────────────────────────

    pub fn zpopmin(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            match self.by_score.keys().next().cloned() {
                Some(key) => {
                    let score = key.score();
                    let member = key.member.clone();
                    self.by_score.remove(&key);
                    self.scores.remove(&member);
                    result.push((member, score));
                }
                None => break,
            }
        }
        result
    }

    pub fn zpopmax(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            match self.by_score.keys().next_back().cloned() {
                Some(key) => {
                    let score = key.score();
                    let member = key.member.clone();
                    self.by_score.remove(&key);
                    self.scores.remove(&member);
                    result.push((member, score));
                }
                None => break,
            }
        }
        result
    }

    // ── ZREMRANGE ────────────────────────────────────────────────────────────

    pub fn zremrangebyrank(&mut self, start: i64, stop: i64) -> usize {
        let to_remove: Vec<Bytes> = self.zrange_by_rank(start, stop, false)
            .into_iter().map(|(m, _)| m).collect();
        self.zrem(&to_remove)
    }

    pub fn zremrangebyscore(&mut self, min: ScoreBound, max: ScoreBound) -> usize {
        let to_remove: Vec<Bytes> = self.by_score.keys()
            .filter(|k| {
                let s = k.score();
                min.satisfies_lower(s) && max.satisfies_upper(s)
            })
            .map(|k| k.member.clone())
            .collect();
        let count = to_remove.len();
        for member in &to_remove {
            if let Some(score) = self.scores.remove(member) {
                self.by_score.remove(&ScoreKey::new(score, member.clone()));
            }
        }
        count
    }

    // ── Snapshot / migration helper ───────────────────────────────────────────

    pub fn all_with_scores(&self) -> Vec<(Bytes, f64)> {
        self.by_score.keys()
            .map(|k| (k.member.clone(), k.score()))
            .collect()
    }
}

impl Default for ZSet {
    fn default() -> Self { ZSet::new() }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_ordering() {
        let pairs = vec![
            (f64::NEG_INFINITY, "a"),
            (-100.0, "b"),
            (-1.0, "c"),
            (0.0, "d"),
            (1.0, "e"),
            (100.0, "f"),
            (f64::INFINITY, "g"),
        ];
        let mut zs = ZSet::new();
        for (score, m) in &pairs {
            zs.zadd(*score, Bytes::from(m.to_string()), false, false, false, false);
        }
        let result = zs.zrange_by_rank(0, -1, false);
        let scores: Vec<f64> = result.iter().map(|(_, s)| *s).collect();
        for i in 1..scores.len() {
            assert!(scores[i - 1] <= scores[i], "{} > {}", scores[i-1], scores[i]);
        }
    }

    #[test]
    fn rank_and_rev_rank() {
        let mut zs = ZSet::new();
        zs.zadd(1.0, Bytes::from("a"), false, false, false, false);
        zs.zadd(2.0, Bytes::from("b"), false, false, false, false);
        zs.zadd(3.0, Bytes::from("c"), false, false, false, false);
        assert_eq!(zs.zrank(&Bytes::from("a")), Some(0));
        assert_eq!(zs.zrank(&Bytes::from("c")), Some(2));
        assert_eq!(zs.zrevrank(&Bytes::from("a")), Some(2));
        assert_eq!(zs.zrevrank(&Bytes::from("c")), Some(0));
    }

    #[test]
    fn zrangebyscore_with_bounds() {
        let mut zs = ZSet::new();
        for i in 0..5i64 {
            zs.zadd(i as f64, Bytes::from(format!("m{i}")), false, false, false, false);
        }
        let r = zs.zrangebyscore(
            ScoreBound::Inclusive(1.0),
            ScoreBound::Exclusive(4.0),
            0, None, false,
        );
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].1, 1.0);
        assert_eq!(r[2].1, 3.0);
    }

    #[test]
    fn zpopmin_max() {
        let mut zs = ZSet::new();
        zs.zadd(5.0, Bytes::from("a"), false, false, false, false);
        zs.zadd(1.0, Bytes::from("b"), false, false, false, false);
        zs.zadd(3.0, Bytes::from("c"), false, false, false, false);
        let min = zs.zpopmin(1);
        assert_eq!(min[0].1, 1.0);
        let max = zs.zpopmax(1);
        assert_eq!(max[0].1, 5.0);
        assert_eq!(zs.len(), 1);
    }
}
