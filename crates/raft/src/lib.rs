//! Per-shard Raft group wiring on top of `openraft` (decision 0001,
//! docs/raft.md). This crate implements the trait glue only: the actual
//! WAL, snapshot, and deterministic-apply logic already exist in
//! `crates/persistence` and are reused here unchanged.
//!
//! Phase 4 scope: a single Raft group, wired for real over TCP, tested
//! with a 3-node cluster including leader crash and network partition.
//! Per-shard multi-group composition is Phase 6.

pub mod blob;
pub mod log_store;
pub mod metrics;
pub mod network;
mod state_machine;

pub use network::PartitionControl;
pub use state_machine::StateMachineStore;

use std::io::Cursor;
use std::sync::Arc;

pub type NodeId = u64;
pub type Node = openraft::BasicNode;

/// `LogStore`/`Network` are generic over any `RaftTypeConfig` fixing
/// `NodeId`/`Node` to the aliases above (see `log_store.rs`/`network.rs`)
/// -- both a shard's data group and the metadata group (Phase 7) use the
/// same WAL-backed log storage and the same TCP transport, differing only
/// in their command type. These aliases are the shard-data
/// instantiation; `membership`'s metadata group uses
/// `raft::log_store::LogStore<MetaTypeConfig>` /
/// `raft::network::Network<MetaTypeConfig>` directly.
pub type LogStore = log_store::LogStore<TypeConfig>;
pub type Network = network::Network<TypeConfig>;

openraft::declare_raft_types!(
    /// Type configuration for loony-redis's per-shard Raft groups.
    pub TypeConfig:
        D = persistence::Command,
        R = (),
        NodeId = NodeId,
        Node = Node,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, Node, RaftError<E>>;

/// Build and start a `Raft` node backed by real WAL/snapshot persistence
/// under `dir`, communicating with peers over TCP via `network`.
///
/// `dir` layout matches `crates/persistence`'s convention
/// (`dir/wal.log`, `dir/snapshots/`) plus this crate's own `vote.bin` /
/// `purged.bin`.
pub async fn start_node(
    node_id: NodeId,
    dir: &std::path::Path,
    config: Arc<openraft::Config>,
    network: Network,
) -> anyhow::Result<(Raft, LogStore, Arc<StateMachineStore>)> {
    let log_store = LogStore::open(dir, persistence::SyncPolicy::Always).await?;
    let state_machine = Arc::new(StateMachineStore::open(dir).await?);

    let raft = openraft::Raft::new(
        node_id,
        config,
        network,
        log_store.clone(),
        state_machine.clone(),
    )
    .await?;

    Ok((raft, log_store, state_machine))
}
