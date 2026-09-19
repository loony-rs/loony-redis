//! `ClusterState` (the metadata group's replicated data: node identities/
//! addresses, slot ownership -- decision 0002) and its
//! `RaftStateMachine`/`RaftSnapshotBuilder` wiring, mirroring
//! `raft::state_machine`'s pattern for shard data but far simpler: no
//! WAL-scale key/value store, just a small map and a slot table, both
//! cheap to snapshot wholesale every time.
//!
//! On `Arc<MetaStateMachineStore>` rather than the bare struct, same
//! reasoning as `raft::StateMachineStore`: `Raft::new` owns one clone,
//! the rest of the application (the cluster router, once it stops using
//! a synthetic `SlotTable` in tests) keeps another for direct reads.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use raft::{Node, NodeId};
use tokio::sync::RwLock;

use crate::identity::NodeIdentity;
use crate::{command, MetaTypeConfig};

/// The metadata group's replicated data (decision 0002): which nodes
/// exist and their addresses, plus slot ownership. Never key/value data.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ClusterState {
    pub nodes: BTreeMap<NodeIdentity, String>,
    pub slots: cluster::SlotTable,
    /// In-flight slot migrations (docs/resharding.md), keyed implicitly
    /// by their `(start, end)` range -- see `MetaCommand`'s migration
    /// variants for how these are created/advanced/removed.
    pub migrations: Vec<cluster::SlotMigration>,
}

impl ClusterState {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Node>,
    data: Vec<u8>, // bincode-serialized ClusterState
}

struct AppliedState {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, Node>,
}

pub struct MetaStateMachineStore {
    pub state: Arc<RwLock<ClusterState>>,
    applied: RwLock<AppliedState>,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    snapshot_path: PathBuf,
    snapshot_idx: AtomicU64,
}

impl MetaStateMachineStore {
    pub async fn open(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let snapshot_path = dir.join("meta-snapshot.bin");

        let mut cluster_state = ClusterState::new();
        let mut last_applied = None;
        let mut last_membership = StoredMembership::default();
        let mut current_snapshot = None;

        if let Some(snap) = raft::blob::load::<StoredSnapshot>(&snapshot_path)? {
            cluster_state = bincode::deserialize(&snap.data)?;
            last_applied = snap.meta.last_log_id;
            last_membership = snap.meta.last_membership.clone();
            current_snapshot = Some(snap);
        }

        Ok(MetaStateMachineStore {
            state: Arc::new(RwLock::new(cluster_state)),
            applied: RwLock::new(AppliedState {
                last_applied,
                last_membership,
            }),
            current_snapshot: RwLock::new(current_snapshot),
            snapshot_path,
            snapshot_idx: AtomicU64::new(0),
        })
    }
}

impl RaftSnapshotBuilder<MetaTypeConfig> for Arc<MetaStateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let applied = self.applied.read().await;
            (applied.last_applied, applied.last_membership.clone())
        };

        let data = {
            let state = self.state.read().await;
            bincode::serialize(&*state).map_err(|e| StorageIOError::read_state_machine(&e))?
        };

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
        raft::blob::save(&self.snapshot_path, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        *self.current_snapshot.write().await = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<MetaTypeConfig> for Arc<MetaStateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let applied = self.applied.read().await;
        Ok((applied.last_applied, applied.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<MetaTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut res = Vec::new();
        let mut applied = self.applied.write().await;
        let mut state = self.state.write().await;
        for entry in entries {
            applied.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(ref cmd) => command::apply(&mut state, cmd),
                EntryPayload::Membership(ref mem) => {
                    applied.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
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
    ) -> Result<Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let new_state: ClusterState = bincode::deserialize(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        *self.state.write().await = new_state;
        {
            let mut applied = self.applied.write().await;
            applied.last_applied = meta.last_log_id;
            applied.last_membership = meta.last_membership.clone();
        }

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        raft::blob::save(&self.snapshot_path, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        *self.current_snapshot.write().await = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, StorageError<NodeId>> {
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
    use crate::MetaCommand;
    use openraft::LeaderId;

    fn entry(index: u64, cmd: MetaCommand) -> openraft::Entry<MetaTypeConfig> {
        openraft::Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn test_apply_and_snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(MetaStateMachineStore::open(dir.path()).await.unwrap());

        let node = NodeIdentity::generate();
        sm.apply(vec![entry(
            0,
            MetaCommand::AddNode {
                node,
                address: "127.0.0.1:7001".into(),
            },
        )])
        .await
        .unwrap();

        assert_eq!(
            sm.state.read().await.nodes.get(&node),
            Some(&"127.0.0.1:7001".to_string())
        );

        let snapshot = sm.build_snapshot().await.unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let mut sm2 = Arc::new(MetaStateMachineStore::open(dir2.path()).await.unwrap());
        sm2.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert_eq!(
            sm2.state.read().await.nodes.get(&node),
            Some(&"127.0.0.1:7001".to_string())
        );
    }

    #[tokio::test]
    async fn test_snapshot_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let node = NodeIdentity::generate();
        {
            let mut sm = Arc::new(MetaStateMachineStore::open(dir.path()).await.unwrap());
            sm.apply(vec![entry(
                0,
                MetaCommand::AddNode {
                    node,
                    address: "a:1".into(),
                },
            )])
            .await
            .unwrap();
            sm.build_snapshot().await.unwrap();
        }

        let reopened = MetaStateMachineStore::open(dir.path()).await.unwrap();
        assert_eq!(
            reopened.state.read().await.nodes.get(&node),
            Some(&"a:1".to_string())
        );
    }
}
