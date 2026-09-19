//! Phase 7 integration tests: a real 3-node metadata Raft group over real
//! TCP, proving membership changes (JOIN/LEAVE/REJOIN) are genuinely
//! consensus-backed -- visible cluster-wide only once committed, never a
//! unilateral local/gossip-style mutation, and impossible to commit from
//! an isolated minority. This is the same harness shape as
//! `crates/raft/tests/common` but over `membership::MetaRaft`/
//! `MetaStateMachineStore` -- small enough (and operating on different
//! types) that duplicating it here is clearer than trying to share test
//! code across crates.

use membership::{ClusterState, MetaCommand, MetaNetwork, MetaStateMachineStore, NodeIdentity};
use openraft::Config;
use raft::{Node, NodeId, PartitionControl};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const CLUSTER: [NodeId; 3] = [1, 2, 3];

fn test_config() -> Arc<Config> {
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

struct TestNode {
    id: NodeId,
    addr: String,
    _dir: TempDir,
    raft: membership::MetaRaft,
    sm: Arc<MetaStateMachineStore>,
    serve_task: JoinHandle<()>,
}

impl TestNode {
    async fn spawn(id: NodeId, links: Arc<PartitionControl>) -> TestNode {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let network: MetaNetwork = MetaNetwork::new(id, links);
        let (raft_handle, _log_store, sm) =
            membership::start_meta_node(id, dir.path(), test_config(), network)
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

    async fn kill(&self) {
        self.serve_task.abort();
        let _ = self.raft.shutdown().await;
    }
}

async fn spawn_cluster(ids: &[NodeId], links: Arc<PartitionControl>) -> Vec<TestNode> {
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

async fn wait_for_leader(nodes: &[TestNode]) -> NodeId {
    wait_for_leader_excluding(nodes.iter(), &[]).await
}

async fn wait_for_leader_excluding<'a>(
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
            panic!("no leader elected within {TEST_TIMEOUT:?} (excluding {exclude:?})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn leader_node(nodes: &[TestNode], leader_id: NodeId) -> &TestNode {
    nodes.iter().find(|n| n.id == leader_id).unwrap()
}

async fn cluster_state(sm: &MetaStateMachineStore) -> ClusterState {
    sm.state.read().await.clone()
}

#[tokio::test]
async fn test_join_is_visible_cluster_wide_via_committed_state() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&CLUSTER, links).await;
    let leader = leader_node(&nodes, wait_for_leader(&nodes).await);

    let new_node = NodeIdentity::generate();
    let idx = leader
        .raft
        .client_write(MetaCommand::AddNode {
            node: new_node,
            address: "127.0.0.1:9001".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;

    // Visible on every node, not just the one that proposed it.
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "AddNode replicates")
            .await
            .unwrap();
        let state = cluster_state(&n.sm).await;
        assert_eq!(
            state.nodes.get(&new_node),
            Some(&"127.0.0.1:9001".to_string()),
            "node {} does not see the joined node in its committed ClusterState",
            n.id
        );
    }
}

#[tokio::test]
async fn test_leave_removes_node_cluster_wide() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&CLUSTER, links).await;
    let leader = leader_node(&nodes, wait_for_leader(&nodes).await);

    let node = NodeIdentity::generate();
    let idx1 = leader
        .raft
        .client_write(MetaCommand::AddNode {
            node,
            address: "127.0.0.1:9001".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx1), "join replicates")
            .await
            .unwrap();
    }

    let idx2 = leader
        .raft
        .client_write(MetaCommand::RemoveNode { node })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx2), "leave replicates")
            .await
            .unwrap();
        let state = cluster_state(&n.sm).await;
        assert!(
            !state.nodes.contains_key(&node),
            "node {} still shows the departed node",
            n.id
        );
    }
}

#[tokio::test]
async fn test_rejoin_with_new_address_updates_cluster_wide() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&CLUSTER, links).await;
    let leader = leader_node(&nodes, wait_for_leader(&nodes).await);

    // A node that already has a persisted identity restarts with a
    // different address (docs/membership.md REJOIN) -- this must be an
    // address update on its existing identity, not a fresh identity.
    let node = NodeIdentity::generate();
    let idx1 = leader
        .raft
        .client_write(MetaCommand::AddNode {
            node,
            address: "127.0.0.1:9001".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx1), "initial join replicates")
            .await
            .unwrap();
    }

    let idx2 = leader
        .raft
        .client_write(MetaCommand::UpdateNodeAddress {
            node,
            address: "127.0.0.1:9099".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx2), "rejoin address update replicates")
            .await
            .unwrap();
        let state = cluster_state(&n.sm).await;
        assert_eq!(
            state.nodes.get(&node),
            Some(&"127.0.0.1:9099".to_string()),
            "node {} did not pick up the rejoined address",
            n.id
        );
        assert_eq!(
            state.nodes.len(),
            1,
            "rejoin must update the existing record, not add a second one"
        );
    }
}

#[tokio::test]
async fn test_leader_failure_does_not_unilaterally_change_membership() {
    // A leader disappearing (heartbeats stop) must never, by itself,
    // mutate ClusterState -- only a committed MetaCommand may. This is
    // the property that distinguishes this design from the prototype's
    // auto_heal (which rewrote routing after 3 missed heartbeats with no
    // consensus at all).
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&CLUSTER, links).await;
    let leader_id = wait_for_leader(&nodes).await;

    let node = NodeIdentity::generate();
    let idx = leader_node(&nodes, leader_id)
        .raft
        .client_write(MetaCommand::AddNode {
            node,
            address: "127.0.0.1:9001".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "join replicates")
            .await
            .unwrap();
    }
    let state_before = cluster_state(&nodes[0].sm).await;

    let pos = nodes.iter().position(|n| n.id == leader_id).unwrap();
    nodes[pos].kill().await;

    // Give the survivors time to notice and elect a new leader --
    // failure *detection* is allowed to happen; it must just never touch
    // ClusterState by itself.
    let survivors: Vec<&TestNode> = nodes.iter().filter(|n| n.id != leader_id).collect();
    let _new_leader = wait_for_leader_excluding(survivors.iter().copied(), &[leader_id]).await;

    for n in &survivors {
        let state_after = cluster_state(&n.sm).await;
        assert_eq!(
            state_after.nodes, state_before.nodes,
            "node {} mutated ClusterState without a committed command",
            n.id
        );
    }
}

#[tokio::test]
async fn test_minority_partition_cannot_commit_membership_change() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&CLUSTER, links.clone()).await;
    let leader_id = wait_for_leader(&nodes).await;
    let others: Vec<NodeId> = nodes
        .iter()
        .map(|n| n.id)
        .filter(|&id| id != leader_id)
        .collect();

    // Isolate the leader: A | B C.
    links.partition(leader_id, others[0]).await;
    links.partition(leader_id, others[1]).await;

    let isolated_node = NodeIdentity::generate();
    let leader = leader_node(&nodes, leader_id);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        leader.raft.client_write(MetaCommand::AddNode {
            node: isolated_node,
            address: "127.0.0.1:9999".into(),
        }),
    )
    .await;
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "an isolated minority must not be able to commit a membership change"
    );

    // The majority elects its own leader and CAN commit a membership
    // change, proving the failure above was quorum-specific, not a
    // general malfunction.
    let majority: Vec<&TestNode> = nodes.iter().filter(|n| n.id != leader_id).collect();
    let new_leader_id = wait_for_leader_excluding(majority.iter().copied(), &[leader_id]).await;
    let new_leader = nodes.iter().find(|n| n.id == new_leader_id).unwrap();

    let joined_node = NodeIdentity::generate();
    let idx = new_leader
        .raft
        .client_write(MetaCommand::AddNode {
            node: joined_node,
            address: "127.0.0.1:9001".into(),
        })
        .await
        .unwrap()
        .log_id
        .index;
    for n in &majority {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "majority commits its own join")
            .await
            .unwrap();
        let state = cluster_state(&n.sm).await;
        assert!(state.nodes.contains_key(&joined_node));
        assert!(
            !state.nodes.contains_key(&isolated_node),
            "the minority's uncommitted proposal must never appear"
        );
    }
}
