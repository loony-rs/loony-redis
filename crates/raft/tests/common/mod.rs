//! Shared test harness for spinning up real Raft clusters over real TCP
//! (localhost, ephemeral ports), each node with its own on-disk WAL/
//! snapshot directory. Used by both `cluster.rs` (Phase 4: a single
//! shard's critical distributed tests) and `multi_shard.rs` (Phase 6:
//! several independent shards in one process). Generalizing this harness
//! to take an explicit id list -- rather than hard-coding a 3-node
//! `1..=3` cluster -- is exactly the "generalize Phase 4's single-group
//! wiring to N groups" PLAN.md asks Phase 6 to do: the production code in
//! `crates/raft` was already group-agnostic (every `start_node` call is
//! fully independent), so what actually needed generalizing was the test
//! harness that spins clusters up.

#![allow(dead_code)]

use bytes::Bytes;
use openraft::Config;
use persistence::Command;
use raft::{Network, Node, NodeId, PartitionControl, Raft, StateMachineStore};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);

pub fn test_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

pub struct TestNode {
    pub id: NodeId,
    pub addr: String,
    pub _dir: TempDir,
    pub raft: Raft,
    pub sm: Arc<StateMachineStore>,
    serve_task: JoinHandle<()>,
}

impl TestNode {
    pub async fn spawn(id: NodeId, links: Arc<PartitionControl>) -> TestNode {
        Self::spawn_at(id, "127.0.0.1:0", None, links).await
    }

    /// Rejoin using the same on-disk directory and, per docs/membership.md
    /// (address changes require a propagated update, which doesn't exist
    /// until Phase 7), the same network address as before.
    pub async fn rejoin_at(
        id: NodeId,
        addr: &str,
        dir: TempDir,
        links: Arc<PartitionControl>,
    ) -> TestNode {
        Self::spawn_at(id, addr, Some(dir), links).await
    }

    async fn spawn_at(
        id: NodeId,
        addr: &str,
        existing_dir: Option<TempDir>,
        links: Arc<PartitionControl>,
    ) -> TestNode {
        let dir = existing_dir.unwrap_or_else(|| tempfile::tempdir().unwrap());
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let network = Network::new(id, links);
        let (raft_handle, _log_store, sm) =
            raft::start_node(id, dir.path(), test_config(), network)
                .await
                .unwrap();

        let serve_raft = raft_handle.clone();
        let serve_task = tokio::spawn(async move {
            let _ = raft::network::serve(listener, serve_raft).await;
        });

        TestNode {
            id,
            addr,
            _dir: dir,
            raft: raft_handle,
            sm,
            serve_task,
        }
    }

    /// Simulate a crash: stop accepting RPCs (peers see connection
    /// refused, matching a dead process) and shut down the local Raft
    /// core. Takes `&self` (not `self`) so the caller keeps ownership of
    /// the on-disk directory for a later rejoin.
    pub async fn kill(&self) {
        self.serve_task.abort();
        let _ = self.raft.shutdown().await;
    }
}

/// Spawn one Raft group ("shard") with a member for each id in `ids`,
/// initialize it as a single cluster, and wait for a leader. Different
/// shards should use disjoint `ids` (e.g. shard A: 1..=3, shard B:
/// 11..=13) purely so test assertions/logs stay unambiguous -- Raft
/// itself doesn't require it, since each group's state is independent.
pub async fn spawn_cluster(ids: &[NodeId], links: Arc<PartitionControl>) -> Vec<TestNode> {
    let mut nodes = Vec::new();
    for &id in ids {
        nodes.push(TestNode::spawn(id, links.clone()).await);
    }

    let mut members = std::collections::BTreeMap::new();
    for n in &nodes {
        members.insert(
            n.id,
            Node {
                addr: n.addr.clone(),
            },
        );
    }
    nodes[0].raft.initialize(members).await.unwrap();
    wait_for_leader(&nodes).await;
    nodes
}

pub async fn wait_for_leader(nodes: &[TestNode]) -> NodeId {
    wait_for_leader_excluding(nodes.iter(), &[]).await
}

/// Like `wait_for_leader`, but ignores a stale `current_leader` belief
/// that still names a node in `exclude` -- a follower's metrics don't
/// clear `current_leader` the instant the old leader dies, only once the
/// follower itself notices (via a failed heartbeat/election timeout) and
/// a new leader is actually elected.
pub async fn wait_for_leader_excluding<'a>(
    nodes: impl Iterator<Item = &'a TestNode> + Clone,
    exclude: &[NodeId],
) -> NodeId {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        for n in nodes.clone() {
            if let Some(leader) = n.raft.metrics().borrow().current_leader {
                if !exclude.contains(&leader) {
                    return leader;
                }
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no new leader elected within {TEST_TIMEOUT:?} (excluding {exclude:?})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn leader_node(nodes: &[TestNode], leader_id: NodeId) -> &TestNode {
    nodes
        .iter()
        .find(|n| n.id == leader_id)
        .expect("leader id must be one of the cluster's nodes")
}

pub fn set_cmd(key: &str, value: &str) -> Command {
    Command::Set {
        key: key.into(),
        value: Bytes::from(value.to_string()),
        expire_at: None,
    }
}

pub fn get_string(sm: &StateMachineStore, key: &str) -> Option<String> {
    match sm.store.get(key) {
        Some(storage::Value::String(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
        _ => None,
    }
}
