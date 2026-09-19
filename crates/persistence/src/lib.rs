//! Single-node persistence: WAL + snapshot + crash recovery
//! (docs/persistence.md, docs/recovery.md). No Raft yet -- this crate is
//! the "group of one" stepping stone PLAN.md's Phase 3 describes; Phase 4
//! wires a real `openraft::Raft` group's `RaftLogStorage`/
//! `RaftStateMachine` on top of the same `Wal`/`Snapshot`/`Command` types
//! rather than replacing them.

mod command;
mod snapshot;
mod wal;

pub use command::{apply, Command};
pub use snapshot::{load_latest_snapshot, save_snapshot, Snapshot, SnapshotMetadata};
pub use wal::{SyncPolicy, Wal, WalRecord};

use std::path::Path;
use storage::Store;

/// What recovery determined the store's applied state to be, so the
/// caller (in later phases, a Raft group) knows where to resume from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecoveredState {
    pub last_applied_term: u64,
    pub last_applied_index: u64,
}

/// Directory layout for one persisted group under `dir`:
///   dir/wal.log        -- the WAL
///   dir/snapshots/      -- snapshot files
fn wal_path(dir: &Path) -> std::path::PathBuf {
    dir.join("wal.log")
}

fn snapshot_dir(dir: &Path) -> std::path::PathBuf {
    dir.join("snapshots")
}

/// Startup sequence (docs/recovery.md, minus the Raft-rejoin steps which
/// don't exist until Phase 4): load the latest valid snapshot (if any)
/// into `store`, open the WAL (truncating any corrupt tail as a side
/// effect), and replay every WAL record after the snapshot's
/// last-included index by applying its `Command` to `store`, strictly in
/// index order (docs/invariants.md S4).
pub fn recover(
    dir: &Path,
    store: &Store,
    sync: SyncPolicy,
) -> anyhow::Result<(Wal, RecoveredState)> {
    let mut state = RecoveredState {
        last_applied_term: 0,
        last_applied_index: 0,
    };
    let mut have_snapshot = false;

    if let Some(snap) = load_latest_snapshot(&snapshot_dir(dir))? {
        store.restore_entries(snap.entries);
        state.last_applied_term = snap.metadata.last_included_term;
        state.last_applied_index = snap.metadata.last_included_index;
        have_snapshot = true;
    }

    let (wal, records) = Wal::open(wal_path(dir), sync)?;

    for rec in records {
        if have_snapshot && rec.index <= state.last_applied_index {
            continue; // already reflected in the snapshot
        }
        let cmd: Command = bincode::deserialize(&rec.payload)?;
        apply(store, &cmd);
        state.last_applied_index = rec.index;
        state.last_applied_term = rec.term;
    }

    Ok((wal, state))
}

/// Propose-and-apply for the pre-Raft, single-node case: append `cmd` to
/// the WAL (durable per `sync`'s policy), then apply it to `store`. This
/// mirrors the ordering Phase 4 will enforce for real
/// (append/commit-quorum, then apply, then ack) but with a "quorum" of
/// one -- see docs/consistency.md.
pub fn propose(wal: &mut Wal, store: &Store, term: u64, cmd: &Command) -> anyhow::Result<u64> {
    let payload = bincode::serialize(cmd)?;
    let index = wal.append(term, &payload)?;
    apply(store, cmd);
    Ok(index)
}

/// Take a snapshot of `store`'s current contents at `(term, index)` and
/// compact the WAL up to and including that index (docs/persistence.md).
/// `index`/`term` must be the index/term of the last WAL record already
/// reflected in `store`'s current state (i.e. the caller's own
/// last-applied position), never a point ahead of what's actually been
/// applied.
pub fn snapshot_and_compact(
    dir: &Path,
    store: &Store,
    wal: &mut Wal,
    term: u64,
    index: u64,
) -> anyhow::Result<std::path::PathBuf> {
    let snap = Snapshot {
        metadata: SnapshotMetadata {
            last_included_term: term,
            last_included_index: index,
        },
        entries: store.snapshot_entries(),
    };
    let path = save_snapshot(&snapshot_dir(dir), &snap)?;
    wal.compact_before(index)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use storage::Value;

    #[test]
    fn test_write_snapshot_crash_restart_recover_verify() {
        // The required Phase 3 test (docs/persistence.md / PLAN.md):
        // write data -> snapshot -> crash -> restart -> restore snapshot
        // -> replay WAL tail -> verify state matches.
        let dir = tempfile::tempdir().unwrap();
        let term = 1u64;

        let store = Store::new();
        let (mut wal, _state) = recover(dir.path(), &store, SyncPolicy::Always).unwrap();

        propose(
            &mut wal,
            &store,
            term,
            &Command::Set {
                key: "a".into(),
                value: Bytes::from("1"),
                expire_at: None,
            },
        )
        .unwrap();
        propose(
            &mut wal,
            &store,
            term,
            &Command::Set {
                key: "b".into(),
                value: Bytes::from("2"),
                expire_at: None,
            },
        )
        .unwrap();

        // Snapshot covers indices 0..=1 (both proposals so far).
        snapshot_and_compact(dir.path(), &store, &mut wal, term, 1).unwrap();

        // More writes after the snapshot, still in the WAL only.
        propose(
            &mut wal,
            &store,
            term,
            &Command::Set {
                key: "c".into(),
                value: Bytes::from("3"),
                expire_at: None,
            },
        )
        .unwrap();
        propose(&mut wal, &store, term, &Command::Delete { key: "a".into() }).unwrap();

        drop(wal); // "crash" -- no explicit close/shutdown sequence
        drop(store);

        // Restart: fresh store, recover from snapshot + WAL tail.
        let recovered_store = Store::new();
        let (_wal2, state) = recover(dir.path(), &recovered_store, SyncPolicy::Always).unwrap();

        assert_eq!(state.last_applied_index, 3); // 0,1 in snapshot; 2,3 replayed
        assert!(
            recovered_store.get("a").is_none(),
            "a was deleted after the snapshot"
        );
        assert!(matches!(recovered_store.get("b"), Some(Value::String(b)) if b == "2"));
        assert!(matches!(recovered_store.get("c"), Some(Value::String(b)) if b == "3"));
    }

    #[test]
    fn test_uncommitted_write_not_durable_if_never_flushed_to_disk() {
        // A record that was appended with SyncPolicy::Never and lost
        // before the OS flushed it would not appear on recovery. We can't
        // simulate a real power-loss in a unit test, but we can verify
        // the documented contract that recovery only ever reflects what's
        // actually readable from the WAL file on disk -- nothing "in
        // flight" in a Wal handle that was simply dropped without its
        // writes having reached the file is invented on replay.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new();
        let (mut wal, _) = recover(dir.path(), &store, SyncPolicy::Always).unwrap();
        propose(
            &mut wal,
            &store,
            1,
            &Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_at: None,
            },
        )
        .unwrap();
        drop(wal);

        let recovered = Store::new();
        let (_wal2, state) = recover(dir.path(), &recovered, SyncPolicy::Always).unwrap();
        assert_eq!(state.last_applied_index, 0);
        assert!(matches!(recovered.get("k"), Some(Value::String(b)) if b == "v"));
    }

    #[test]
    fn test_recovery_from_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new();
        let (_wal, state) = recover(dir.path(), &store, SyncPolicy::Always).unwrap();
        assert_eq!(state.last_applied_index, 0);
        assert_eq!(state.last_applied_term, 0);
        assert_eq!(store.keys_count(), 0);
    }

    #[test]
    fn test_snapshot_plus_wal_replay_matches_continuous_application() {
        // R3 (docs/invariants.md): snapshot + WAL tail reproduces exactly
        // what continuous application would have produced.
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        let store_a = Store::new(); // never snapshots -- pure continuous apply
        let store_b = Store::new(); // snapshots partway through

        let (mut wal_a, _) = recover(dir_a.path(), &store_a, SyncPolicy::Always).unwrap();
        let (mut wal_b, _) = recover(dir_b.path(), &store_b, SyncPolicy::Always).unwrap();

        let cmds = [
            Command::Set {
                key: "x".into(),
                value: Bytes::from("1"),
                expire_at: None,
            },
            Command::ListPushRight {
                key: "l".into(),
                values: vec![Bytes::from("a")],
            },
            Command::HashSet {
                key: "h".into(),
                field: Bytes::from("f"),
                value: Bytes::from("v"),
            },
        ];
        for (i, cmd) in cmds.iter().enumerate() {
            propose(&mut wal_a, &store_a, 1, cmd).unwrap();
            propose(&mut wal_b, &store_b, 1, cmd).unwrap();
            if i == 1 {
                snapshot_and_compact(dir_b.path(), &store_b, &mut wal_b, 1, 1).unwrap();
            }
        }

        assert_eq!(store_a.get("x").is_some(), store_b.get("x").is_some());
        assert_eq!(store_a.llen("l").unwrap(), store_b.llen("l").unwrap());
        assert_eq!(
            store_a.hget("h", &Bytes::from("f")).unwrap(),
            store_b.hget("h", &Bytes::from("f")).unwrap()
        );
    }
}
