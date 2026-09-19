//! Drives one slot migration through docs/resharding.md's state machine
//! (PREPARING -> TRANSFERRING -> CATCHING_UP -> CUTOVER -> COMPLETED),
//! performing the actual data movement between two real shard Raft
//! groups.
//!
//! Per docs/resharding.md, any node can pick up this role by reading
//! `ClusterState` -- this function doesn't implement dynamic
//! coordinator-election (a documented simplification: *which* node runs
//! it is decided by the caller, not negotiated at runtime), but it *is*
//! resilient to the underlying Raft groups' leadership changing or a
//! replica dying mid-migration: every step retries against the full set
//! of known replica handles until one succeeds, which is exactly what a
//! well-behaved client does when a leader dies and a new one is elected.
//!
//! Data transfer is a repeated full-range diff-and-sync rather than
//! tailing the source's committed log: `sync_once` fetches the source's
//! current range snapshot (`rpc::fetch_range_snapshot`, safe to serve
//! from any replica, per that module's doc comment) and the target's own
//! current range snapshot, then proposes `Set`/`Delete` commands through
//! the target's Raft group for every difference. This is a deliberate
//! simplification versus tailing the source's log (which would be more
//! efficient for large, low-churn ranges) but is fully correct: repeating
//! it until a pass finds no differences is exactly "TRANSFERRING does
//! the bulk copy, CATCHING_UP closes the gap against ongoing writes."
//! Only `storage::Value::String` values are migrated; other types are
//! skipped with a warning -- a documented limitation, not a silent gap.

use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use storage::{KeyEntry, Value};

const RETRY_TIMEOUT: Duration = Duration::from_secs(20);
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

fn in_range(key: &str, start: u16, end: u16) -> bool {
    let slot = cluster::slot_for_key(key.as_bytes());
    slot >= start && slot <= end
}

/// Try `f` against each of `rafts` in turn (repeatedly, since which one
/// is leader can change) until one succeeds or `RETRY_TIMEOUT` elapses.
async fn propose_meta(
    rafts: &[membership::MetaRaft],
    cmd: membership::MetaCommand,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + RETRY_TIMEOUT;
    loop {
        for r in rafts {
            if r.client_write(cmd.clone()).await.is_ok() {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "propose_meta({cmd:?}) timed out against {} replicas",
                rafts.len()
            );
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

async fn propose_shard(rafts: &[raft::Raft], cmd: persistence::Command) -> anyhow::Result<()> {
    let deadline = Instant::now() + RETRY_TIMEOUT;
    loop {
        for r in rafts {
            if r.client_write(cmd.clone()).await.is_ok() {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "propose_shard({cmd:?}) timed out against {} replicas",
                rafts.len()
            );
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

async fn fetch_from_any(addrs: &[String], start: u16, end: u16) -> anyhow::Result<Vec<KeyEntry>> {
    let deadline = Instant::now() + RETRY_TIMEOUT;
    loop {
        for addr in addrs {
            if let Ok(entries) = crate::rpc::fetch_range_snapshot(addr, start, end).await {
                return Ok(entries);
            }
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "fetch_from_any timed out against {} source addresses",
                addrs.len()
            );
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

/// Fetch source's and target's current range contents and propose
/// whatever commands close the gap. Returns whether anything changed
/// (i.e. whether the two sides were already in sync).
///
/// `delete_target_only`: whether a key present on target but absent from
/// source should be deleted. True during `Transferring`/`CatchingUp`,
/// where target shouldn't have diverged from source at all yet -- a
/// target-only key there means a delete happened at source since the
/// last pass. **False** for the pass taken during/after `Cutover`: by
/// then an ASK-aware client may already be writing straight to target
/// for this range (source is telling callers to go there), so a
/// target-only key at that point is a legitimate new write, not
/// leftover garbage -- deleting it would destroy real data.
async fn sync_once(
    source_addrs: &[String],
    target_rafts: &[raft::Raft],
    target_sm: &raft::StateMachineStore,
    start: u16,
    end: u16,
    delete_target_only: bool,
) -> anyhow::Result<bool> {
    let source_entries = fetch_from_any(source_addrs, start, end).await?;
    let source_map: HashMap<String, KeyEntry> = source_entries
        .into_iter()
        .map(|e| (e.key.clone(), e))
        .collect();

    let target_map: HashMap<String, KeyEntry> = target_sm
        .store
        .snapshot_entries()
        .into_iter()
        .filter(|e| in_range(&e.key, start, end))
        .map(|e| (e.key.clone(), e))
        .collect();

    let mut changed = false;

    for (key, entry) in &source_map {
        let value = match &entry.value {
            Value::String(b) => b,
            other => {
                tracing::warn!("skipping migration of key {key:?}: only String values are migrated, found {other:?}");
                continue;
            }
        };
        let matches = matches!(target_map.get(key), Some(t) if matches!(&t.value, Value::String(tb) if tb == value) && t.expires_at == entry.expires_at);
        if !matches {
            changed = true;
            propose_shard(
                target_rafts,
                persistence::Command::Set {
                    key: key.clone(),
                    value: Bytes::from(value.to_vec()),
                    expire_at: entry.expires_at,
                },
            )
            .await?;
        }
    }

    if delete_target_only {
        for key in target_map.keys() {
            if !source_map.contains_key(key) {
                changed = true;
                propose_shard(
                    target_rafts,
                    persistence::Command::Delete { key: key.clone() },
                )
                .await?;
            }
        }
    }

    Ok(changed)
}

/// The (start, end, source, target) identity of one migration, grouped
/// into its own type purely to keep `run_migration`'s signature down to
/// a readable number of parameters.
#[derive(Debug, Clone)]
pub struct MigrationSpec {
    pub start: u16,
    pub end: u16,
    pub source: cluster::ShardId,
    pub target: cluster::ShardId,
    pub target_leader_addr: String,
}

/// Drive `spec`'s migration to completion, per docs/resharding.md.
/// `source_rpc_addrs` are `resharding::rpc::serve` addresses for the
/// source shard's replicas; `target_rafts`/`target_sm` are the target
/// shard's replica handles (any of them -- `target_sm` is read from
/// directly since this coordinator is assumed to run somewhere with
/// local access to it, per the module doc comment on coordinator
/// placement).
pub async fn run_migration(
    meta_rafts: &[membership::MetaRaft],
    spec: &MigrationSpec,
    source_rpc_addrs: &[String],
    target_rafts: &[raft::Raft],
    target_sm: &raft::StateMachineStore,
) -> anyhow::Result<()> {
    let (start, end) = (spec.start, spec.end);

    propose_meta(
        meta_rafts,
        membership::MetaCommand::StartMigration {
            start,
            end,
            source: spec.source,
            target: spec.target,
            target_leader_addr: spec.target_leader_addr.clone(),
        },
    )
    .await?;

    propose_meta(
        meta_rafts,
        membership::MetaCommand::AdvanceMigration {
            start,
            end,
            state: cluster::MigrationState::Transferring,
        },
    )
    .await?;
    sync_once(source_rpc_addrs, target_rafts, target_sm, start, end, true).await?; // bulk transfer

    propose_meta(
        meta_rafts,
        membership::MetaCommand::AdvanceMigration {
            start,
            end,
            state: cluster::MigrationState::CatchingUp,
        },
    )
    .await?;
    loop {
        let changed =
            sync_once(source_rpc_addrs, target_rafts, target_sm, start, end, true).await?;
        if !changed {
            break;
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }

    propose_meta(
        meta_rafts,
        membership::MetaCommand::AdvanceMigration {
            start,
            end,
            state: cluster::MigrationState::Cutover,
        },
    )
    .await?;
    // One more pass to catch anything committed at source between the
    // last CatchingUp pass and routers actually seeing Cutover. Not
    // delete_target_only: a cluster-aware client may already be writing
    // straight to target for this range once it sees -ASK, and those
    // writes must not be treated as garbage just because source doesn't
    // have them (source never will -- they were never sent there).
    sync_once(source_rpc_addrs, target_rafts, target_sm, start, end, false).await?;

    propose_meta(
        meta_rafts,
        membership::MetaCommand::CompleteMigration { start, end },
    )
    .await?;
    Ok(())
}
