//! Node identity (docs/membership.md): a stable identifier generated once
//! on first start with an empty data directory, persisted, and never
//! re-derived from a network address. A node that loses its identity
//! file must rejoin as a brand-new node, never reuse an old identity
//! against different disk contents (docs/membership.md is explicit about
//! this) -- so `load_or_create` only ever *creates* a fresh identity when
//! the file is entirely absent, and propagates a hard error if the file
//! exists but is corrupt, rather than silently generating a replacement.

use rand::RngCore;
use std::path::Path;

/// A 128-bit random identifier, independent of network address. Stable
/// across restarts and address changes; only lost if the identity file
/// itself is lost.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct NodeIdentity([u8; 16]);

impl NodeIdentity {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        NodeIdentity(bytes)
    }

    /// Deterministic derivation of an `openraft` `NodeId` (a plain `u64`)
    /// from this identity, for use as this node's Raft-level id in any
    /// group it participates in (the metadata group now; shard groups
    /// once Phase 8's replica placement assigns them). Derived from the
    /// persisted identity, never from an address -- see docs/membership.md.
    pub fn raft_id(&self) -> raft::NodeId {
        let mut acc = 0xcbf29ce484222325u64; // FNV-1a offset basis
        for &b in &self.0 {
            acc ^= b as u64;
            acc = acc.wrapping_mul(0x100000001b3); // FNV-1a prime
        }
        acc
    }
}

impl std::fmt::Display for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Load the identity persisted at `<dir>/identity`, or create and persist
/// a fresh one if `dir` has none yet. Returns an error (does not
/// generate a replacement) if the file exists but fails to load --
/// e.g. corrupted -- since silently fabricating a new identity over
/// existing disk contents could let a corrupted/foreign disk masquerade
/// as a known cluster member.
pub fn load_or_create(dir: &Path) -> anyhow::Result<NodeIdentity> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("identity");
    if path.exists() {
        return raft::blob::load::<NodeIdentity>(&path)?.ok_or_else(|| {
            anyhow::anyhow!("identity file at {path:?} exists but is unreadable/corrupt")
        });
    }
    let identity = NodeIdentity::generate();
    raft::blob::save(&path, &identity)?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_is_random() {
        assert_ne!(NodeIdentity::generate(), NodeIdentity::generate());
    }

    #[test]
    fn test_raft_id_is_deterministic() {
        let id = NodeIdentity::generate();
        assert_eq!(id.raft_id(), id.raft_id());
    }

    #[test]
    fn test_load_or_create_persists_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let a = load_or_create(dir.path()).unwrap();
        let b = load_or_create(dir.path()).unwrap();
        assert_eq!(
            a, b,
            "second call must load the same identity, not generate a new one"
        );
    }

    #[test]
    fn test_corrupt_identity_file_is_a_hard_error_not_silently_replaced() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("identity"), b"not a valid identity blob").unwrap();
        assert!(load_or_create(dir.path()).is_err());
    }
}
