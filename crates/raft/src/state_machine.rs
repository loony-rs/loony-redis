//! `RaftStateMachine`/`RaftSnapshotBuilder` backed by `storage::Store`,
//! applying entries via `persistence::Command::apply` -- the same
//! deterministic apply function Phase 3 already built and tested. This
//! crate adds nothing to "what a command does"; it only adds the Raft
//! trait glue and snapshot persistence around it.
//!
//! Traits are implemented on `Arc<StateMachineStore>` rather than
//! `StateMachineStore` directly (matching openraft's own reference
//! example) so that `Raft::new` can own one clone of the `Arc` while the
//! rest of the application (the RESP server, in a later phase) keeps
//! another clone for reading `.store` directly without going through
//! Raft at all -- exactly the "leader reads" path in docs/consistency.md.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use tokio::sync::RwLock;

use crate::{blob, Node, NodeId, TypeConfig};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Node>,
    /// bincode-serialized `Vec<storage::KeyEntry>` -- the same
    /// full-fidelity, TTL-preserving representation `crates/persistence`
    /// uses for its own snapshots.
    data: Vec<u8>,
}

struct AppliedState {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, Node>,
}

pub struct StateMachineStore {
    pub store: Arc<storage::Store>,
    state: RwLock<AppliedState>,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_path: PathBuf,
    snapshot_idx: AtomicU64,
}

impl StateMachineStore {
    pub async fn open(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let snapshot_path = dir.join("snapshot.bin");
        let store = Arc::new(storage::Store::new());

        let mut last_applied = None;
        let mut last_membership = StoredMembership::default();
        let mut current_snapshot = None;

        if let Some(snap) = blob::load::<StoredSnapshot>(&snapshot_path)? {
            let entries: Vec<storage::KeyEntry> = bincode::deserialize(&snap.data)?;
            store.restore_entries(entries);
            last_applied = snap.meta.last_log_id;
            last_membership = snap.meta.last_membership.clone();
            current_snapshot = Some(snap);
        }

        Ok(StateMachineStore {
            store,
            state: RwLock::new(AppliedState {
                last_applied,
                last_membership,
            }),
            current_snapshot: RwLock::new(current_snapshot),
            snapshot_path,
            snapshot_idx: AtomicU64::new(0),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let state = self.state.read().await;
            (state.last_applied, state.last_membership.clone())
        };

        let entries = self.store.snapshot_entries();
        let data =
            bincode::serialize(&entries).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied {
            Some(id) => format!("{}-{}-{}", id.leader_id, id.index, idx),
            None => format!("--{idx}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        blob::save(&self.snapshot_path, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        *self.current_snapshot.write().await = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let state = self.state.read().await;
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut res = Vec::new();
        let mut state = self.state.write().await;
        for entry in entries {
            state.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(ref cmd) => {
                    persistence::apply(&self.store, cmd);
                }
                EntryPayload::Membership(ref mem) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                }
            }
            res.push(());
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let entries: Vec<storage::KeyEntry> = bincode::deserialize(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        self.store.restore_entries(entries);

        {
            let mut state = self.state.write().await;
            state.last_applied = meta.last_log_id;
            state.last_membership = meta.last_membership.clone();
        }

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        blob::save(&self.snapshot_path, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{EntryPayload, LeaderId};

    fn entry(index: u64, cmd: persistence::Command) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn set(key: &str, value: &str) -> persistence::Command {
        persistence::Command::Set {
            key: key.into(),
            value: bytes::Bytes::from(value.to_string()),
            expire_at: None,
        }
    }

    #[tokio::test]
    async fn test_apply_updates_store_and_applied_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachineStore::open(dir.path()).await.unwrap());

        sm.apply(vec![entry(0, set("a", "1")), entry(1, set("b", "2"))])
            .await
            .unwrap();

        assert!(matches!(sm.store.get("a"), Some(storage::Value::String(b)) if b == "1"));
        let (last_applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_snapshot_build_install_round_trip_on_fresh_store() {
        let dir_a = tempfile::tempdir().unwrap();
        let mut sm_a = Arc::new(StateMachineStore::open(dir_a.path()).await.unwrap());
        sm_a.apply(vec![entry(0, set("a", "1")), entry(1, set("b", "2"))])
            .await
            .unwrap();

        let snapshot = sm_a.build_snapshot().await.unwrap();

        let dir_b = tempfile::tempdir().unwrap();
        let mut sm_b = Arc::new(StateMachineStore::open(dir_b.path()).await.unwrap());
        sm_b.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert!(matches!(sm_b.store.get("a"), Some(storage::Value::String(b)) if b == "1"));
        assert!(matches!(sm_b.store.get("b"), Some(storage::Value::String(b)) if b == "2"));
        let (last_applied, _) = sm_b.applied_state().await.unwrap();
        assert_eq!(last_applied.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_snapshot_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sm = Arc::new(StateMachineStore::open(dir.path()).await.unwrap());
            sm.apply(vec![entry(0, set("a", "1"))]).await.unwrap();
            sm.build_snapshot().await.unwrap();
        }

        let reopened = StateMachineStore::open(dir.path()).await.unwrap();
        assert!(matches!(reopened.store.get("a"), Some(storage::Value::String(b)) if b == "1"));
    }
}
