//! `RaftLogStorage`/`RaftLogReader` backed by `persistence::Wal`, generic
//! over any `RaftTypeConfig` that fixes `NodeId`/`Node` to this crate's
//! `NodeId`/`Node` aliases -- which every group in this project uses,
//! whether it's a shard's data group (`crate::TypeConfig`) or the
//! metadata group (`membership`'s own type config, Phase 7). The WAL,
//! vote, and purge-boundary handling below don't care what `D`/`R`/
//! `Entry` a group's commands are; only `state_machine.rs` is specific to
//! shard data.
//!
//! Reads are served from an in-memory index (`entries`, keyed by log
//! index) rather than re-scanning the WAL file on every call -- the WAL
//! itself remains the durable source of truth and is what's replayed to
//! rebuild `entries` on `open`. `vote` and the purged-prefix boundary
//! each get their own tiny checksummed file (`crate::blob`) since the WAL
//! record format has no slot for them.

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{
    LogId, OptionalSend, RaftLogId, RaftTypeConfig, StorageError, StorageIOError, Vote,
};
use tokio::sync::{Mutex, RwLock};

use crate::{blob, Node, NodeId};

pub struct LogStore<C> {
    inner: Arc<Inner>,
    _config: PhantomData<C>,
}

impl<C> Clone for LogStore<C> {
    fn clone(&self) -> Self {
        LogStore {
            inner: self.inner.clone(),
            _config: PhantomData,
        }
    }
}

struct Inner {
    wal: Mutex<persistence::Wal>,
    entries: RwLock<BTreeMap<u64, Vec<u8>>>,
    vote: RwLock<Option<Vote<NodeId>>>,
    last_purged: RwLock<Option<LogId<NodeId>>>,
    vote_path: PathBuf,
    purged_path: PathBuf,
}

impl<C> LogStore<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    pub async fn open(dir: &Path, sync: persistence::SyncPolicy) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join("raft-log.wal");
        let (wal, records) = persistence::Wal::open(wal_path, sync)?;

        let mut entries = BTreeMap::new();
        for rec in records {
            entries.insert(rec.index, rec.payload);
        }

        let vote_path = dir.join("vote.bin");
        let purged_path = dir.join("purged.bin");
        let vote = blob::load(&vote_path)?;
        let last_purged = blob::load(&purged_path)?;

        Ok(LogStore {
            inner: Arc::new(Inner {
                wal: Mutex::new(wal),
                entries: RwLock::new(entries),
                vote: RwLock::new(vote),
                last_purged: RwLock::new(last_purged),
                vote_path,
                purged_path,
            }),
            _config: PhantomData,
        })
    }

    /// This group's WAL byte/fsync counters (docs/observability.md's
    /// `wal_bytes_written`/`wal_fsync_duration`) -- the caller attaches
    /// its own `group_id` label, since this store doesn't know it.
    pub async fn wal_stats(&self) -> persistence::WalStats {
        self.inner.wal.lock().await.stats()
    }

    async fn append_inner(
        &self,
        entries: impl IntoIterator<Item = openraft::Entry<C>>,
    ) -> io::Result<()> {
        let mut wal = self.inner.wal.lock().await;
        let mut entries_map = self.inner.entries.write().await;
        for entry in entries {
            let log_id = *entry.get_log_id();
            let payload = bincode::serialize(&entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            wal.append(log_id.leader_id.term, &payload)?;
            entries_map.insert(log_id.index, payload);
        }
        wal.flush()?;
        Ok(())
    }
}

impl<C> RaftLogReader<C> for LogStore<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<C>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let entries = self.inner.entries.read().await;
        let mut result = Vec::new();
        for payload in entries.range(range).map(|(_, v)| v) {
            let entry: openraft::Entry<C> =
                bincode::deserialize(payload).map_err(|e| StorageIOError::read_logs(&e))?;
            result.push(entry);
        }
        Ok(result)
    }
}

impl<C> RaftLogStorage<C> for LogStore<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    type LogReader = LogStore<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<NodeId>> {
        let entries = self.inner.entries.read().await;
        let last_purged = *self.inner.last_purged.read().await;
        let last_log_id = match entries.iter().next_back() {
            None => last_purged,
            Some((_, payload)) => {
                let entry: openraft::Entry<C> =
                    bincode::deserialize(payload).map_err(|e| StorageIOError::read_logs(&e))?;
                Some(*entry.get_log_id())
            }
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        blob::save(&self.inner.vote_path, vote).map_err(|e| StorageIOError::write_vote(&e))?;
        *self.inner.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.inner.vote.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result = self.append_inner(entries).await;
        callback.log_io_completed(result);
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut entries = self.inner.entries.write().await;
            entries.retain(|&idx, _| idx < log_id.index);
        }
        let mut wal = self.inner.wal.lock().await;
        wal.truncate_from(log_id.index)
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut entries = self.inner.entries.write().await;
            entries.retain(|&idx, _| idx > log_id.index);
        }
        {
            let mut wal = self.inner.wal.lock().await;
            wal.compact_before(log_id.index)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        *self.inner.last_purged.write().await = Some(log_id);
        blob::save(&self.inner.purged_path, &log_id).map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TypeConfig;
    use openraft::{EntryPayload, LeaderId};

    fn entry(term: u64, index: u64, cmd: persistence::Command) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(LeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn set(key: &str) -> persistence::Command {
        persistence::Command::Set {
            key: key.into(),
            value: bytes::Bytes::from("v"),
            expire_at: None,
        }
    }

    #[tokio::test]
    async fn test_append_read_and_reopen_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut store: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();

        store
            .append_inner(vec![entry(1, 0, set("a")), entry(1, 1, set("b"))])
            .await
            .unwrap();

        let got = store.try_get_log_entries(0..2).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 0);
        assert_eq!(got[1].log_id.index, 1);

        // Reopen against the same directory -- entries must survive.
        let mut reopened: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();
        let got2 = reopened.try_get_log_entries(0..2).await.unwrap();
        assert_eq!(got2.len(), 2);
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_truncate_removes_conflicting_suffix_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut store: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();

        store
            .append_inner(vec![
                entry(1, 0, set("a")),
                entry(1, 1, set("b")),
                entry(1, 2, set("c")),
            ])
            .await
            .unwrap();

        store
            .truncate(LogId::new(LeaderId::new(1, 1), 1))
            .await
            .unwrap();

        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].log_id.index, 0);
    }

    #[tokio::test]
    async fn test_purge_discards_prefix_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut store: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();

        store
            .append_inner(vec![
                entry(1, 0, set("a")),
                entry(1, 1, set("b")),
                entry(1, 2, set("c")),
            ])
            .await
            .unwrap();

        store
            .purge(LogId::new(LeaderId::new(1, 1), 0))
            .await
            .unwrap();
        let got = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2);

        let mut reopened: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 0);
        let got2 = reopened.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got2.len(), 2);
    }

    #[tokio::test]
    async fn test_vote_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut store: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();
        let vote = Vote::new(3, 1);
        store.save_vote(&vote).await.unwrap();

        let mut reopened: LogStore<TypeConfig> =
            LogStore::open(dir.path(), persistence::SyncPolicy::Always)
                .await
                .unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
    }
}
