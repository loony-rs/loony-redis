//! Phase 4 integration tests: real 3-node Raft clusters over real TCP
//! (localhost, ephemeral ports), each node with its own on-disk WAL/
//! snapshot directory. These are the "critical distributed tests" from
//! docs/testing.md / PLAN.md Phase 4 acceptance: basic replication,
//! leader crash + election + continued writes + rejoin-catch-up, and
//! both partition scenarios from docs/failure-model.md.
//!
//! The harness itself (`TestNode`, `spawn_cluster`, ...) lives in
//! `tests/common/mod.rs`, shared with `multi_shard.rs` (Phase 6).

mod common;

use common::*;
use raft::{NodeId, PartitionControl};
use std::time::Duration;

const SHARD: [NodeId; 3] = [1, 2, 3];

#[tokio::test]
async fn test_three_node_replication() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&SHARD, links).await;

    let leader_id = wait_for_leader(&nodes).await;
    let leader = leader_node(&nodes, leader_id);
    let resp = leader.raft.client_write(set_cmd("k", "v1")).await.unwrap();
    let idx = resp.log_id.index;

    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "replicate SET")
            .await
            .unwrap();
        assert_eq!(
            get_string(&n.sm, "k"),
            Some("v1".to_string()),
            "node {} did not replicate the write",
            n.id
        );
    }
}

#[tokio::test]
async fn test_leader_crash_election_continues_and_rejoin_catches_up() {
    let links = PartitionControl::new();
    let mut nodes = spawn_cluster(&SHARD, links.clone()).await;

    let old_leader_id = wait_for_leader(&nodes).await;
    let idx1 = leader_node(&nodes, old_leader_id)
        .raft
        .client_write(set_cmd("before", "1"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx1), "replicate before-crash write")
            .await
            .unwrap();
    }

    // Kill the leader.
    let pos = nodes.iter().position(|n| n.id == old_leader_id).unwrap();
    let old_leader = nodes.remove(pos);
    old_leader.kill().await;
    let TestNode {
        addr: old_leader_addr,
        _dir: old_leader_dir,
        ..
    } = old_leader;

    // Remaining two must elect a new leader and keep serving writes.
    let new_leader_id = wait_for_leader_excluding(nodes.iter(), &[old_leader_id]).await;
    assert_ne!(
        new_leader_id, old_leader_id,
        "the dead node cannot still be reported as leader"
    );

    let idx2 = leader_node(&nodes, new_leader_id)
        .raft
        .client_write(set_cmd("after", "2"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx2), "replicate after-crash write")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "before"), Some("1".to_string()));
        assert_eq!(get_string(&n.sm, "after"), Some("2".to_string()));
    }

    // Restart the killed node against the SAME on-disk directory (kept
    // alive by TestNode holding the TempDir) under a fresh listener at
    // the SAME address, and confirm it catches up.
    let rejoined =
        TestNode::rejoin_at(old_leader_id, &old_leader_addr, old_leader_dir, links).await;
    assert_eq!(
        rejoined.addr, old_leader_addr,
        "must reuse the same address for peers to find it again"
    );
    nodes.push(rejoined);

    for n in &nodes {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx2), "rejoined node catches up")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "before"), Some("1".to_string()));
        assert_eq!(get_string(&n.sm, "after"), Some("2".to_string()));
    }
}

#[tokio::test]
async fn test_minority_partition_isolated_leader_cannot_commit() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&SHARD, links.clone()).await;

    let leader_id = wait_for_leader(&nodes).await;
    let others: Vec<NodeId> = nodes
        .iter()
        .map(|n| n.id)
        .filter(|&id| id != leader_id)
        .collect();
    assert_eq!(others.len(), 2);

    // Isolate the leader from both followers: A | B C.
    links.partition(leader_id, others[0]).await;
    links.partition(leader_id, others[1]).await;

    // The isolated leader cannot replicate to a quorum, so a write
    // through it must not succeed within a bounded time.
    let leader = leader_node(&nodes, leader_id);
    let write_result = tokio::time::timeout(
        Duration::from_secs(2),
        leader.raft.client_write(set_cmd("x", "1")),
    )
    .await;
    assert!(
        write_result.is_err() || write_result.unwrap().is_err(),
        "an isolated minority leader must not be able to commit a write"
    );

    // The majority side must elect a new leader among themselves and
    // keep serving writes. wait_for_leader_excluding also checks the
    // isolated leader's own metrics, but that's fine here: it's not
    // reachable to actually win an election with a higher term visible
    // to the others, and its stale self-belief (still `Some(leader_id)`)
    // is exactly what's being excluded.
    let majority: Vec<&TestNode> = nodes.iter().filter(|n| n.id != leader_id).collect();
    let new_leader_id = wait_for_leader_excluding(majority.iter().copied(), &[leader_id]).await;
    assert_ne!(new_leader_id, leader_id);

    let new_leader = nodes.iter().find(|n| n.id == new_leader_id).unwrap();
    let idx = new_leader
        .raft
        .client_write(set_cmd("y", "2"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &majority {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "majority continues writing")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "y"), Some("2".to_string()));
    }

    // Heal the partition; the old leader must catch up rather than
    // retain any stale claim to leadership.
    links.heal(leader_id, others[0]).await;
    links.heal(leader_id, others[1]).await;

    leader
        .raft
        .wait(Some(TEST_TIMEOUT))
        .applied_index_at_least(Some(idx), "healed node catches up to majority's writes")
        .await
        .unwrap();
    assert_eq!(get_string(&leader.sm, "y"), Some("2".to_string()));
}

#[tokio::test]
async fn test_majority_partition_continues_without_isolated_follower() {
    let links = PartitionControl::new();
    let nodes = spawn_cluster(&SHARD, links.clone()).await;

    let leader_id = wait_for_leader(&nodes).await;
    let followers: Vec<NodeId> = nodes
        .iter()
        .map(|n| n.id)
        .filter(|&id| id != leader_id)
        .collect();
    let isolated = followers[0];
    let other_follower = followers[1];

    // Isolate just one follower: leader+other_follower (majority) | isolated.
    links.partition(isolated, leader_id).await;
    links.partition(isolated, other_follower).await;

    // The majority side (still holding the original leader) must
    // continue committing writes without any new election.
    let leader = leader_node(&nodes, leader_id);
    let idx = leader
        .raft
        .client_write(set_cmd("k", "v"))
        .await
        .unwrap()
        .log_id
        .index;

    for n in nodes.iter().filter(|n| n.id != isolated) {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx), "majority side commits")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "k"), Some("v".to_string()));
    }
    assert_eq!(
        leader.raft.metrics().borrow().current_leader,
        Some(leader_id),
        "the majority side's leader must not have been displaced by an isolated single follower"
    );

    // The isolated follower must not have this write.
    let isolated_node = nodes.iter().find(|n| n.id == isolated).unwrap();
    assert_eq!(get_string(&isolated_node.sm, "k"), None);

    // Heal and confirm it catches up.
    links.heal(isolated, leader_id).await;
    links.heal(isolated, other_follower).await;
    isolated_node
        .raft
        .wait(Some(TEST_TIMEOUT))
        .applied_index_at_least(Some(idx), "isolated follower catches up after heal")
        .await
        .unwrap();
    assert_eq!(get_string(&isolated_node.sm, "k"), Some("v".to_string()));
}
