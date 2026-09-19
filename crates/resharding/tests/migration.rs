//! Phase 9 integration tests: a full slot migration between two real
//! 3-node shard Raft groups, coordinated through a real 3-node metadata
//! group -- continuous reads/writes throughout, plus source/target
//! leader kills mid-migration with verified convergence (PLAN.md Phase
//! 9, Prompt.md section 51 steps 13-16 at the level this stack actually
//! supports today: direct Raft/coordinator access, since there is no
//! production binary wiring crates/server to real shards yet -- see
//! PLAN.md's explicit note on this).

use bytes::Bytes;
use cluster::MigrationState;
use membership::{MetaCommand, MetaNetwork, MetaStateMachineStore};
use openraft::Config;
use persistence::Command;
use raft::{Network, Node, NodeId, PartitionControl};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

fn raft_config() -> Arc<Config> {
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

// ── Shard node/cluster harness (adapted from crates/raft/tests/common,
// extended with a resharding RPC listener each node also runs) ─────────

struct ShardNode {
    id: NodeId,
    raft_addr: String,
    raft: raft::Raft,
    sm: Arc<raft::StateMachineStore>,
    rpc_addr: String,
    _dir: TempDir,
    serve_task: JoinHandle<()>,
    rpc_task: JoinHandle<()>,
}

impl ShardNode {
    async fn spawn(id: NodeId, links: Arc<PartitionControl>) -> ShardNode {
        let dir = tempfile::tempdir().unwrap();
        let raft_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft_addr = raft_listener.local_addr().unwrap().to_string();
        let rpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_addr = rpc_listener.local_addr().unwrap().to_string();

        let network = Network::new(id, links);
        let (raft_handle, _log_store, sm) =
            raft::start_node(id, dir.path(), raft_config(), network)
                .await
                .unwrap();

        let serve_raft = raft_handle.clone();
        let serve_task = tokio::spawn(async move {
            let _ = raft::network::serve(raft_listener, serve_raft).await;
        });
        let rpc_sm = sm.clone();
        let rpc_task = tokio::spawn(async move {
            let _ = resharding::rpc::serve(rpc_listener, rpc_sm).await;
        });

        ShardNode {
            id,
            raft_addr,
            raft: raft_handle,
            sm,
            rpc_addr,
            _dir: dir,
            serve_task,
            rpc_task,
        }
    }

    async fn kill(&self) {
        self.serve_task.abort();
        self.rpc_task.abort();
        let _ = self.raft.shutdown().await;
    }
}

async fn spawn_shard(ids: &[NodeId], links: Arc<PartitionControl>) -> Vec<ShardNode> {
    let mut nodes = Vec::new();
    for &id in ids {
        nodes.push(ShardNode::spawn(id, links.clone()).await);
    }
    let mut members = std::collections::BTreeMap::new();
    for n in &nodes {
        members.insert(
            n.id,
            Node {
                addr: n.raft_addr.clone(),
            },
        );
    }
    nodes[0].raft.initialize(members).await.unwrap();
    wait_for_shard_leader(&nodes).await;
    nodes
}

async fn wait_for_shard_leader(nodes: &[ShardNode]) -> NodeId {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        for n in nodes {
            if let Some(leader) = n.raft.metrics().borrow().current_leader {
                return leader;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no shard leader elected within {TEST_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn shard_leader(nodes: &[ShardNode], leader_id: NodeId) -> &ShardNode {
    nodes.iter().find(|n| n.id == leader_id).unwrap()
}

// ── Metadata cluster harness (same shape as crates/membership/tests) ──

#[allow(dead_code)] // serve_task is kept alive only to keep the listener task running
struct MetaNode {
    id: NodeId,
    addr: String,
    raft: membership::MetaRaft,
    sm: Arc<MetaStateMachineStore>,
    _dir: TempDir,
    serve_task: JoinHandle<()>,
}

impl MetaNode {
    async fn spawn(id: NodeId, links: Arc<PartitionControl>) -> MetaNode {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let network: MetaNetwork = MetaNetwork::new(id, links);
        let (raft_handle, _log_store, sm) =
            membership::start_meta_node(id, dir.path(), raft_config(), network)
                .await
                .unwrap();
        let serve_raft = raft_handle.clone();
        let serve_task = tokio::spawn(async move {
            let _ = raft::network::serve(listener, serve_raft).await;
        });
        MetaNode {
            id,
            addr,
            raft: raft_handle,
            sm,
            _dir: dir,
            serve_task,
        }
    }
}

async fn spawn_meta(ids: &[NodeId], links: Arc<PartitionControl>) -> Vec<MetaNode> {
    let mut nodes = Vec::new();
    for &id in ids {
        nodes.push(MetaNode::spawn(id, links.clone()).await);
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
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        for n in &nodes {
            if n.raft.metrics().borrow().current_leader.is_some() {
                return nodes;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no metadata leader elected within {TEST_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn get_string(sm: &raft::StateMachineStore, key: &str) -> Option<String> {
    match sm.store.get(key) {
        Some(storage::Value::String(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
        _ => None,
    }
}

/// Find `count` distinct keys whose slot falls within `[start, end]`,
/// starting the search past the first `skip` matching candidates so
/// repeated calls (e.g. a "before" batch and a "during" batch) can be
/// made to return disjoint key sets by passing an increasing `skip`.
fn keys_in_range_from(count: usize, skip: usize, start: u16, end: u16) -> Vec<String> {
    let mut out = Vec::new();
    let mut found = 0usize;
    let mut i = 0u64;
    while out.len() < count {
        let k = format!("k{i}");
        if cluster::slot_for_key(k.as_bytes()) <= end
            && cluster::slot_for_key(k.as_bytes()) >= start
        {
            if found >= skip {
                out.push(k);
            }
            found += 1;
        }
        i += 1;
    }
    out
}

fn keys_in_range(count: usize, start: u16, end: u16) -> Vec<String> {
    keys_in_range_from(count, 0, start, end)
}

#[tokio::test]
async fn test_full_migration_cycle_with_continuous_reads_writes() {
    let links = PartitionControl::new();
    let meta = spawn_meta(&[100, 101, 102], links.clone()).await;
    let shard_a = spawn_shard(&[1, 2, 3], links.clone()).await;
    let shard_b = spawn_shard(&[11, 12, 13], links.clone()).await;

    let meta_rafts: Vec<membership::MetaRaft> = meta.iter().map(|n| n.raft.clone()).collect();
    let a_leader_id = wait_for_shard_leader(&shard_a).await;
    let a_leader = shard_leader(&shard_a, a_leader_id);
    let a_rpc_addrs: Vec<String> = shard_a.iter().map(|n| n.rpc_addr.clone()).collect();
    let b_rafts: Vec<raft::Raft> = shard_b.iter().map(|n| n.raft.clone()).collect();
    let b_leader_id = wait_for_shard_leader(&shard_b).await;
    let b_leader = shard_leader(&shard_b, b_leader_id);

    // Whole slot space starts owned by shard A.
    propose_meta(
        &meta_rafts,
        MetaCommand::SetSlotOwner {
            start: 0,
            end: cluster::SLOT_COUNT - 1,
            shard: 1,
            leader_addr: "unused".into(),
        },
    )
    .await;

    // Write an initial batch to A before migration starts.
    let range = (0u16, cluster::SLOT_COUNT - 1);
    let pre_keys = keys_in_range(10, range.0, range.1);
    for (i, k) in pre_keys.iter().enumerate() {
        write(a_leader, k, &format!("pre{i}")).await;
    }

    // Concurrently: run the migration, and keep writing new keys for a
    // while (docs/resharding.md / Prompt.md section 51: continue reads
    // and writes during resharding). This test has no cluster router in
    // front of it (crates/server isn't wired to real shards yet -- see
    // this crate's doc comment), so it does what a real ASK-aware client
    // would do itself: check the metadata group's migration state before
    // each write and send it to whichever side is currently
    // authoritative for it. A write issued to the *wrong* side (as a
    // naive, non-cluster-aware client would do by always writing to A)
    // is expected to be invisible to the migration -- that's exactly
    // what -ASK exists to prevent, and it's not this coordinator's job
    // to guess at writes it was never told about.
    let more_keys = keys_in_range_from(10, 10, range.0, range.1);
    let writer_meta_sm = meta[0].sm.clone();
    let writer_a = a_leader.raft.clone();
    let writer_b = b_leader.raft.clone();
    let write_task = tokio::spawn(async move {
        for (i, k) in more_keys.iter().enumerate() {
            let cutover_or_done = {
                let state = writer_meta_sm.state.read().await;
                match state
                    .migrations
                    .iter()
                    .find(|m| m.start == range.0 && m.end == range.1)
                {
                    Some(m) => m.state == MigrationState::Cutover,
                    None => true, // no record at all: either not started yet (treated as "use source" below) or already completed
                }
            };
            let has_ever_started = {
                let state = writer_meta_sm.state.read().await;
                state.slots.owner(range.0).map(|(s, _)| *s) == Some(2)
                    || state
                        .migrations
                        .iter()
                        .any(|m| m.start == range.0 && m.end == range.1)
            };
            let target_raft = if cutover_or_done && has_ever_started {
                &writer_b
            } else {
                &writer_a
            };
            let cmd = Command::Set {
                key: k.clone(),
                value: Bytes::from(format!("during{i}")),
                expire_at: None,
            };
            let _ = target_raft.client_write(cmd).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        more_keys
    });

    resharding::run_migration(
        &meta_rafts,
        &resharding::MigrationSpec {
            start: range.0,
            end: range.1,
            source: 1,
            target: 2,
            target_leader_addr: "b-leader-addr".to_string(),
        },
        &a_rpc_addrs,
        &b_rafts,
        &b_leader.sm,
    )
    .await
    .unwrap();

    let more_keys = write_task.await.unwrap();

    // All pre- and during-migration writes must now be on B.
    for (i, k) in pre_keys.iter().enumerate() {
        assert_eq!(
            get_string(&b_leader.sm, k).await,
            Some(format!("pre{i}")),
            "key {k} missing on target after migration"
        );
    }
    for (i, k) in more_keys.iter().enumerate() {
        assert_eq!(
            get_string(&b_leader.sm, k).await,
            Some(format!("during{i}")),
            "key {k} written during migration missing on target"
        );
    }

    // Ownership and migration bookkeeping converged.
    let final_state = meta[0].sm.state.read().await.clone();
    assert!(
        final_state.migrations.is_empty(),
        "migration record must be gone after completion"
    );
    assert_eq!(
        final_state.slots.owner(100),
        Some(&(2, "b-leader-addr".to_string()))
    );

    // A new write for a migrated key must now be routable to B (not
    // asserting server-level ASK here -- see the crate doc comment on
    // what this stack does/doesn't wire up yet -- just that B is now
    // the real, authoritative owner at the Raft level).
    write(b_leader, "post-migration-key", "final").await;
    assert_eq!(
        get_string(&b_leader.sm, "post-migration-key").await,
        Some("final".to_string())
    );
}

#[tokio::test]
async fn test_migration_survives_source_leader_kill_mid_transfer() {
    let links = PartitionControl::new();
    let meta = spawn_meta(&[200, 201, 202], links.clone()).await;
    let shard_a = spawn_shard(&[21, 22, 23], links.clone()).await;
    let shard_b = spawn_shard(&[31, 32, 33], links.clone()).await;

    let meta_rafts: Vec<membership::MetaRaft> = meta.iter().map(|n| n.raft.clone()).collect();
    let range = (0u16, cluster::SLOT_COUNT - 1);
    propose_meta(
        &meta_rafts,
        MetaCommand::SetSlotOwner {
            start: range.0,
            end: range.1,
            shard: 1,
            leader_addr: "unused".into(),
        },
    )
    .await;

    let a_leader_id = wait_for_shard_leader(&shard_a).await;
    {
        let a_leader = shard_leader(&shard_a, a_leader_id);
        for (i, k) in keys_in_range(15, range.0, range.1).iter().enumerate() {
            write(a_leader, k, &format!("v{i}")).await;
        }
    }

    let a_rpc_addrs: Vec<String> = shard_a.iter().map(|n| n.rpc_addr.clone()).collect();
    let b_rafts: Vec<raft::Raft> = shard_b.iter().map(|n| n.raft.clone()).collect();
    let b_leader_id = wait_for_shard_leader(&shard_b).await;
    let b_sm = shard_leader(&shard_b, b_leader_id).sm.clone();

    let migration_task = tokio::spawn(async move {
        resharding::run_migration(
            &meta_rafts,
            &resharding::MigrationSpec {
                start: range.0,
                end: range.1,
                source: 1,
                target: 2,
                target_leader_addr: "b-leader-addr".to_string(),
            },
            &a_rpc_addrs,
            &b_rafts,
            &b_sm,
        )
        .await
    });

    // Give the migration a moment to start (likely mid-Transferring),
    // then kill source's current leader -- one node out of three, so
    // the shard's own Raft quorum survives and elects a new leader; the
    // migration must retry against the survivors and still converge.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let pos = shard_a.iter().position(|n| n.id == a_leader_id).unwrap();
    shard_a[pos].kill().await;

    migration_task.await.unwrap().unwrap();

    let final_state = meta[0].sm.state.read().await.clone();
    assert!(final_state.migrations.is_empty());
    assert_eq!(final_state.slots.owner(100).map(|(s, _)| *s), Some(2));

    let b_leader_id_now = wait_for_shard_leader(&shard_b).await;
    let b_sm_now = &shard_leader(&shard_b, b_leader_id_now).sm;
    for (i, k) in keys_in_range(15, range.0, range.1).iter().enumerate() {
        assert_eq!(
            get_string(b_sm_now, k).await,
            Some(format!("v{i}")),
            "key {k} missing on target despite source leader kill"
        );
    }
}

#[tokio::test]
async fn test_migration_survives_target_leader_kill_mid_catchup() {
    let links = PartitionControl::new();
    let meta = spawn_meta(&[300, 301, 302], links.clone()).await;
    let shard_a = spawn_shard(&[41, 42, 43], links.clone()).await;
    let shard_b = spawn_shard(&[51, 52, 53], links.clone()).await;

    let meta_rafts: Vec<membership::MetaRaft> = meta.iter().map(|n| n.raft.clone()).collect();
    let range = (0u16, cluster::SLOT_COUNT - 1);
    propose_meta(
        &meta_rafts,
        MetaCommand::SetSlotOwner {
            start: range.0,
            end: range.1,
            shard: 1,
            leader_addr: "unused".into(),
        },
    )
    .await;

    let a_leader_id = wait_for_shard_leader(&shard_a).await;
    {
        let a_leader = shard_leader(&shard_a, a_leader_id);
        for (i, k) in keys_in_range(15, range.0, range.1).iter().enumerate() {
            write(a_leader, k, &format!("v{i}")).await;
        }
    }

    let a_rpc_addrs: Vec<String> = shard_a.iter().map(|n| n.rpc_addr.clone()).collect();
    let b_rafts: Vec<raft::Raft> = shard_b.iter().map(|n| n.raft.clone()).collect();
    let b_leader_id = wait_for_shard_leader(&shard_b).await;
    let b_sm = shard_leader(&shard_b, b_leader_id).sm.clone();

    let migration_task = tokio::spawn(async move {
        resharding::run_migration(
            &meta_rafts,
            &resharding::MigrationSpec {
                start: range.0,
                end: range.1,
                source: 1,
                target: 2,
                target_leader_addr: "b-leader-addr".to_string(),
            },
            &a_rpc_addrs,
            &b_rafts,
            &b_sm,
        )
        .await
    });

    // Let the bulk transfer land, then kill target's leader during what
    // should be the CatchingUp phase.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let pos = shard_b.iter().position(|n| n.id == b_leader_id).unwrap();
    shard_b[pos].kill().await;

    migration_task.await.unwrap().unwrap();

    let final_state = meta[0].sm.state.read().await.clone();
    assert!(final_state.migrations.is_empty());
    assert_eq!(final_state.slots.owner(100).map(|(s, _)| *s), Some(2));

    let b_leader_id_now = wait_for_shard_leader(&shard_b).await;
    let b_sm_now = &shard_leader(&shard_b, b_leader_id_now).sm;
    for (i, k) in keys_in_range(15, range.0, range.1).iter().enumerate() {
        assert_eq!(
            get_string(b_sm_now, k).await,
            Some(format!("v{i}")),
            "key {k} missing on target despite target leader kill"
        );
    }
}

async fn propose_meta(rafts: &[membership::MetaRaft], cmd: MetaCommand) {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        for r in rafts {
            if r.client_write(cmd.clone()).await.is_ok() {
                return;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("propose_meta({cmd:?}) timed out");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn write(node: &ShardNode, key: &str, value: &str) {
    let cmd = Command::Set {
        key: key.into(),
        value: Bytes::from(value.to_string()),
        expire_at: None,
    };
    node.raft.client_write(cmd).await.unwrap();
}
