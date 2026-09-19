//! Phase 6 integration tests: multiple independent Raft groups ("shards")
//! running concurrently in one process, each its own real 3-node cluster
//! over real TCP with its own on-disk WAL/snapshot per node. Proves
//! docs/raft.md's "one Raft group per shard" claim for real: parallel
//! writes to different shards proceed independently, and a leader
//! failure in one shard has no effect on another shard's availability.
//!
//! Each shard uses a disjoint NodeId range (shard A: 1..=3, shard B:
//! 11..=13) purely to keep test assertions/logs unambiguous -- Raft
//! itself doesn't require this, since two groups' state is never
//! compared or shared.

mod common;

use common::*;
use raft::{NodeId, PartitionControl};

const SHARD_A: [NodeId; 3] = [1, 2, 3];
const SHARD_B: [NodeId; 3] = [11, 12, 13];

#[tokio::test]
async fn test_parallel_writes_to_independent_shards() {
    let links_a = PartitionControl::new();
    let links_b = PartitionControl::new();

    // Spin both shards up concurrently, not sequentially -- this alone
    // demonstrates they don't serialize on any shared state.
    let (nodes_a, nodes_b) = tokio::join!(
        spawn_cluster(&SHARD_A, links_a),
        spawn_cluster(&SHARD_B, links_b)
    );

    let leader_a = leader_node(&nodes_a, wait_for_leader(&nodes_a).await);
    let leader_b = leader_node(&nodes_b, wait_for_leader(&nodes_b).await);

    // Concurrent writes to each shard's leader.
    let (resp_a, resp_b) = tokio::join!(
        leader_a.raft.client_write(set_cmd("k", "shard-a-value")),
        leader_b.raft.client_write(set_cmd("k", "shard-b-value"))
    );
    let idx_a = resp_a.unwrap().log_id.index;
    let idx_b = resp_b.unwrap().log_id.index;

    for n in &nodes_a {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx_a), "shard A replicates")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "k"), Some("shard-a-value".to_string()));
    }
    for n in &nodes_b {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx_b), "shard B replicates")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "k"), Some("shard-b-value".to_string()));
    }

    // Same key, but each shard's state machine is entirely separate --
    // neither should ever see the other's value.
    for n in &nodes_a {
        assert_ne!(get_string(&n.sm, "k"), Some("shard-b-value".to_string()));
    }
    for n in &nodes_b {
        assert_ne!(get_string(&n.sm, "k"), Some("shard-a-value".to_string()));
    }
}

#[tokio::test]
async fn test_shard_leader_failure_does_not_affect_other_shard() {
    let links_a = PartitionControl::new();
    let links_b = PartitionControl::new();
    let (nodes_a, nodes_b) = tokio::join!(
        spawn_cluster(&SHARD_A, links_a),
        spawn_cluster(&SHARD_B, links_b)
    );

    let leader_a_id = wait_for_leader(&nodes_a).await;
    let leader_b_id = wait_for_leader(&nodes_b).await;

    // Establish a baseline write on shard B and note its leader/term
    // before touching shard A at all.
    let idx_before = leader_node(&nodes_b, leader_b_id)
        .raft
        .client_write(set_cmd("before", "1"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes_b {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx_before), "shard B baseline write")
            .await
            .unwrap();
    }
    let term_b_before = nodes_b[0].raft.metrics().borrow().current_term;

    // Kill shard A's leader. Shard B has no network link, no shared
    // directory, and no shared Raft state with shard A -- this must not
    // trigger an election or any observable change on shard B.
    let pos = nodes_a.iter().position(|n| n.id == leader_a_id).unwrap();
    nodes_a[pos].kill().await;

    // Immediately (no delay) confirm shard B is still fully available
    // and still led by the same node/term -- i.e. shard A's failure
    // never reached it.
    let idx_after = leader_node(&nodes_b, leader_b_id)
        .raft
        .client_write(set_cmd("after", "2"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &nodes_b {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx_after), "shard B unaffected write")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "after"), Some("2".to_string()));
    }
    assert_eq!(
        nodes_b[0].raft.metrics().borrow().current_leader,
        Some(leader_b_id),
        "shard B's leader must be unchanged by shard A's failure"
    );
    assert_eq!(
        nodes_b[0].raft.metrics().borrow().current_term,
        term_b_before,
        "shard B must not have run an election just because shard A did"
    );

    // Meanwhile shard A, in complete isolation from this, must still
    // independently recover: the remaining two nodes elect a new leader
    // and keep serving writes.
    let remaining_a: Vec<&TestNode> = nodes_a.iter().filter(|n| n.id != leader_a_id).collect();
    let new_leader_a_id =
        wait_for_leader_excluding(remaining_a.iter().copied(), &[leader_a_id]).await;
    assert_ne!(new_leader_a_id, leader_a_id);

    let new_leader_a = nodes_a.iter().find(|n| n.id == new_leader_a_id).unwrap();
    let idx_a = new_leader_a
        .raft
        .client_write(set_cmd("recovered", "yes"))
        .await
        .unwrap()
        .log_id
        .index;
    for n in &remaining_a {
        n.raft
            .wait(Some(TEST_TIMEOUT))
            .applied_index_at_least(Some(idx_a), "shard A recovers independently")
            .await
            .unwrap();
        assert_eq!(get_string(&n.sm, "recovered"), Some("yes".to_string()));
    }
}
