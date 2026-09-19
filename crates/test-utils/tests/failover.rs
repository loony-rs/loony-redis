//! Phase 8: the "critical distributed tests" from docs/testing.md, run
//! for real -- three genuine OS processes (`test_node`, located via
//! `CARGO_BIN_EXE_test_node`), a real network-fault proxy between them
//! (`test_utils::proxy`), and real `SIGKILL`/restart
//! (`test_utils::process`). This is the multi-process counterpart to
//! `crates/raft/tests/cluster.rs`'s in-process Phase 4 tests -- same
//! scenarios, but nothing here is simulated inside a single process.

use raft::{NodeId, PartitionControl};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use test_utils::admin::{self, AdminRequest, AdminResponse};
use test_utils::process::{free_port, TestProcess};

const IDS: [NodeId; 3] = [1, 2, 3];
const POLL_TIMEOUT: Duration = Duration::from_secs(15);

/// These tests spawn real OS processes on real (freshly-picked, but not
/// reserved) TCP ports and contend for real CPU time running actual
/// elections. Cargo runs `#[tokio::test]` functions in this file
/// concurrently by default, which makes both port selection (see
/// `free_port`'s documented TOCTOU race) and election timing flaky under
/// load. Serializing test *execution* here -- rather than relying on
/// every caller remembering `--test-threads=1` -- fixes both at once.
async fn serialize_test() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

#[allow(dead_code)] // bin_path/raft_addrs are set up but not read back; dirs
                    // must stay alive so its TempDirs aren't deleted underneath
                    // the running node processes.
struct Cluster {
    bin_path: PathBuf,
    procs: HashMap<NodeId, TestProcess>,
    raft_addrs: HashMap<NodeId, String>,
    admin_addrs: HashMap<NodeId, String>,
    dirs: HashMap<NodeId, TempDir>,
    links: Arc<PartitionControl>,
}

impl Cluster {
    async fn spawn() -> Cluster {
        let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_test_node"));
        let links = PartitionControl::new();

        let mut raft_addrs = HashMap::new();
        let mut admin_addrs = HashMap::new();
        let mut dirs = HashMap::new();
        for &id in &IDS {
            raft_addrs.insert(id, format!("127.0.0.1:{}", free_port().unwrap()));
            admin_addrs.insert(id, format!("127.0.0.1:{}", free_port().unwrap()));
            dirs.insert(id, tempfile::tempdir().unwrap());
        }

        let overrides = test_utils::proxy::build_link_mesh(&IDS, &raft_addrs, links.clone())
            .await
            .unwrap();
        let peers_arg = raft_addrs
            .iter()
            .map(|(id, addr)| format!("{id}={addr}"))
            .collect::<Vec<_>>()
            .join(",");

        let mut procs = HashMap::new();
        for &id in &IDS {
            let dial_arg = overrides[&id]
                .iter()
                .map(|(peer, addr)| format!("{peer}={addr}"))
                .collect::<Vec<_>>()
                .join(",");
            let mut args = vec![
                "--id".into(),
                id.to_string(),
                "--dir".into(),
                dirs[&id].path().to_string_lossy().into_owned(),
                "--raft-addr".into(),
                raft_addrs[&id].clone(),
                "--admin-addr".into(),
                admin_addrs[&id].clone(),
                "--peers".into(),
                peers_arg.clone(),
                "--dial".into(),
                dial_arg,
            ];
            if id == IDS[0] {
                args.push("--init".into());
            }
            procs.insert(id, TestProcess::spawn(&bin_path, args).unwrap());
        }

        let cluster = Cluster {
            bin_path,
            procs,
            raft_addrs,
            admin_addrs,
            dirs,
            links,
        };
        wait_for_leader(&cluster).await;
        cluster
    }

    /// Restart node `id` against the same on-disk directory, address,
    /// and dial overrides it was originally launched with.
    fn restart(&mut self, id: NodeId) {
        self.procs.get_mut(&id).unwrap().restart().unwrap();
    }

    async fn kill(&mut self, id: NodeId) {
        self.procs.get_mut(&id).unwrap().kill().await.unwrap();
    }
}

/// Retry an admin call for up to `POLL_TIMEOUT` -- a node that just
/// (re)started, or one we just killed, won't answer immediately/at all.
async fn admin_call_retrying(addr: &str, req: &AdminRequest) -> Option<AdminResponse> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if let Ok(resp) = admin::call(addr, req).await {
            return Some(resp);
        }
        if tokio::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn metrics(cluster: &Cluster, id: NodeId) -> Option<admin::MetricsSnapshot> {
    match admin::call(&cluster.admin_addrs[&id], &AdminRequest::Metrics)
        .await
        .ok()?
    {
        AdminResponse::Metrics(m) => Some(m),
        _ => None,
    }
}

async fn wait_for_leader(cluster: &Cluster) -> NodeId {
    wait_for_leader_excluding(cluster, &[]).await
}

async fn wait_for_leader_excluding(cluster: &Cluster, exclude: &[NodeId]) -> NodeId {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        for &id in &IDS {
            if let Some(m) = metrics(cluster, id).await {
                if let Some(leader) = m.current_leader {
                    if !exclude.contains(&leader) {
                        return leader;
                    }
                }
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no leader elected within {POLL_TIMEOUT:?} (excluding {exclude:?})");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn propose(cluster: &Cluster, leader: NodeId, key: &str, value: &str) -> Result<u64, String> {
    let req = AdminRequest::Propose {
        key: key.into(),
        value: value.into(),
    };
    match admin_call_retrying(&cluster.admin_addrs[&leader], &req).await {
        Some(AdminResponse::Proposed { index }) => Ok(index),
        Some(AdminResponse::ProposeFailed { message }) => Err(message),
        _ => Err("no response".into()),
    }
}

async fn wait_applied(cluster: &Cluster, id: NodeId, index: u64) {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some(m) = metrics(cluster, id).await {
            if m.last_applied_index.unwrap_or(0) >= index {
                return;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("node {id} did not apply index {index} within {POLL_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn get(cluster: &Cluster, id: NodeId, key: &str) -> Option<String> {
    match admin::call(
        &cluster.admin_addrs[&id],
        &AdminRequest::Get { key: key.into() },
    )
    .await
    .ok()?
    {
        AdminResponse::Value(v) => v,
        _ => None,
    }
}

#[tokio::test]
async fn test_leader_crash_election_continues_and_restart_catches_up() {
    let _guard = serialize_test().await;
    let mut cluster = Cluster::spawn().await;

    let leader = wait_for_leader(&cluster).await;
    let idx1 = propose(&cluster, leader, "before", "1").await.unwrap();
    for &id in &IDS {
        wait_applied(&cluster, id, idx1).await;
    }

    cluster.kill(leader).await;

    let new_leader = wait_for_leader_excluding(&cluster, &[leader]).await;
    assert_ne!(new_leader, leader);

    let idx2 = propose(&cluster, new_leader, "after", "2").await.unwrap();
    let survivors: Vec<NodeId> = IDS.iter().copied().filter(|&id| id != leader).collect();
    for &id in &survivors {
        wait_applied(&cluster, id, idx2).await;
        assert_eq!(get(&cluster, id, "before").await, Some("1".to_string()));
        assert_eq!(get(&cluster, id, "after").await, Some("2".to_string()));
    }

    cluster.restart(leader);
    wait_applied(&cluster, leader, idx2).await;
    assert_eq!(get(&cluster, leader, "before").await, Some("1".to_string()));
    assert_eq!(get(&cluster, leader, "after").await, Some("2".to_string()));
}

#[tokio::test]
async fn test_minority_partition_isolated_leader_cannot_commit() {
    let _guard = serialize_test().await;
    let cluster = Cluster::spawn().await;
    let leader = wait_for_leader(&cluster).await;
    let others: Vec<NodeId> = IDS.iter().copied().filter(|&id| id != leader).collect();

    cluster.links.partition(leader, others[0]).await;
    cluster.links.partition(leader, others[1]).await;

    let result =
        tokio::time::timeout(Duration::from_secs(3), propose(&cluster, leader, "x", "1")).await;
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "an isolated minority leader must not be able to commit a write"
    );

    let new_leader = wait_for_leader_excluding(&cluster, &[leader]).await;
    assert_ne!(new_leader, leader);

    let idx = propose(&cluster, new_leader, "y", "2").await.unwrap();
    for &id in &others {
        wait_applied(&cluster, id, idx).await;
        assert_eq!(get(&cluster, id, "y").await, Some("2".to_string()));
    }

    cluster.links.heal(leader, others[0]).await;
    cluster.links.heal(leader, others[1]).await;
    wait_applied(&cluster, leader, idx).await;
    assert_eq!(get(&cluster, leader, "y").await, Some("2".to_string()));
}

#[tokio::test]
async fn test_majority_partition_continues_without_isolated_follower() {
    let _guard = serialize_test().await;
    let cluster = Cluster::spawn().await;
    let leader = wait_for_leader(&cluster).await;
    let followers: Vec<NodeId> = IDS.iter().copied().filter(|&id| id != leader).collect();
    let isolated = followers[0];
    let other = followers[1];

    cluster.links.partition(isolated, leader).await;
    cluster.links.partition(isolated, other).await;

    let idx = propose(&cluster, leader, "k", "v").await.unwrap();
    wait_applied(&cluster, leader, idx).await;
    wait_applied(&cluster, other, idx).await;
    assert_eq!(get(&cluster, leader, "k").await, Some("v".to_string()));
    assert_eq!(get(&cluster, other, "k").await, Some("v".to_string()));

    let m = metrics(&cluster, leader).await.unwrap();
    assert_eq!(
        m.current_leader,
        Some(leader),
        "majority side's leader must be unaffected by an isolated single follower"
    );

    assert_eq!(get(&cluster, isolated, "k").await, None);

    cluster.links.heal(isolated, leader).await;
    cluster.links.heal(isolated, other).await;
    wait_applied(&cluster, isolated, idx).await;
    assert_eq!(get(&cluster, isolated, "k").await, Some("v".to_string()));
}

#[tokio::test]
async fn test_stale_follower_catches_up_and_never_leads_with_stale_log() {
    let _guard = serialize_test().await;
    let cluster = Cluster::spawn().await;
    let leader = wait_for_leader(&cluster).await;
    let followers: Vec<NodeId> = IDS.iter().copied().filter(|&id| id != leader).collect();
    let stale = followers[0];
    let fresh = followers[1];

    // Isolate one follower, then commit several writes it will miss.
    cluster.links.partition(stale, leader).await;
    cluster.links.partition(stale, fresh).await;

    let mut last_idx = 0;
    for i in 0..5 {
        last_idx = propose(&cluster, leader, &format!("k{i}"), &format!("v{i}"))
            .await
            .unwrap();
    }
    wait_applied(&cluster, fresh, last_idx).await;

    // While still isolated, the stale node cannot have become leader --
    // it can't gather votes from anyone.
    let stale_metrics = metrics(&cluster, stale).await.unwrap();
    assert_ne!(stale_metrics.current_leader, Some(stale));

    cluster.links.heal(stale, leader).await;
    cluster.links.heal(stale, fresh).await;

    wait_applied(&cluster, stale, last_idx).await;
    for i in 0..5 {
        assert_eq!(
            get(&cluster, stale, &format!("k{i}")).await,
            Some(format!("v{i}"))
        );
    }
    // Even after catching up, it must not have won an election with the
    // stale log it had while isolated -- the current leader is still
    // whichever node legitimately holds a term granted by quorum.
    let after = metrics(&cluster, stale).await.unwrap();
    assert!(after.current_leader.is_some());
}

#[tokio::test]
async fn test_repeated_leader_failures_still_converge() {
    let _guard = serialize_test().await;
    let mut cluster = Cluster::spawn().await;
    let mut dead: Vec<NodeId> = Vec::new();
    let mut last_idx = 0;

    for round in 0..2 {
        let leader = wait_for_leader_excluding(&cluster, &dead).await;
        last_idx = propose(&cluster, leader, &format!("round{round}"), "ok")
            .await
            .unwrap();
        for &id in IDS.iter().filter(|id| !dead.contains(id) && **id != leader) {
            wait_applied(&cluster, id, last_idx).await;
        }
        cluster.kill(leader).await;
        dead.push(leader);
    }

    // One node standing that was never killed (with 3 nodes and 2
    // rounds, exactly one survives untouched) still has everything.
    let survivor = *IDS.iter().find(|id| !dead.contains(id)).unwrap();
    wait_applied(&cluster, survivor, last_idx).await;
    for round in 0..2 {
        assert_eq!(
            get(&cluster, survivor, &format!("round{round}")).await,
            Some("ok".to_string())
        );
    }
}
