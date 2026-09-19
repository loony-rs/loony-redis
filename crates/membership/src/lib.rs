//! Cluster membership (docs/membership.md) and the metadata Raft group
//! (decision 0002): node identity, JOIN/LEAVE/REJOIN as replicated
//! commands, and the consensus-backed `ClusterState` (node addresses +
//! slot ownership) that replaces the prototype's gossip-only
//! `ClusterConfig`/`auto_heal`.
//!
//! The metadata group is a Raft group like any other in this project --
//! it reuses `raft::log_store::LogStore` and `raft::network::Network`
//! verbatim (Phase 7 is exactly why those were made generic), supplying
//! its own `RaftTypeConfig` (`D = MetaCommand`) and its own state machine
//! (`MetaStateMachineStore`, wrapping `ClusterState` instead of
//! `storage::Store`).

pub mod command;
pub mod identity;
mod state_machine;

pub use command::MetaCommand;
pub use identity::NodeIdentity;
pub use state_machine::{ClusterState, MetaStateMachineStore};

use raft::{Node, NodeId};
use std::io::Cursor;
use std::sync::Arc;

openraft::declare_raft_types!(
    /// Type configuration for the metadata Raft group.
    pub MetaTypeConfig:
        D = MetaCommand,
        R = (),
        NodeId = NodeId,
        Node = Node,
);

pub type MetaRaft = openraft::Raft<MetaTypeConfig>;
pub type MetaLogStore = raft::log_store::LogStore<MetaTypeConfig>;
pub type MetaNetwork = raft::network::Network<MetaTypeConfig>;

/// Build and start the metadata group's local `Raft` node, reusing
/// `crates/raft`'s generic WAL-backed log store and TCP network
/// transport (see the module doc comment).
pub async fn start_meta_node(
    node_id: NodeId,
    dir: &std::path::Path,
    config: Arc<openraft::Config>,
    network: MetaNetwork,
) -> anyhow::Result<(MetaRaft, MetaLogStore, Arc<MetaStateMachineStore>)> {
    let log_store: MetaLogStore =
        raft::log_store::LogStore::open(dir, persistence::SyncPolicy::Always).await?;
    let state_machine = Arc::new(MetaStateMachineStore::open(dir).await?);

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
