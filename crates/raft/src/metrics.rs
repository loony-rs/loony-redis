//! Derives docs/observability.md's `raft_*` values from a group's live
//! `openraft::RaftMetrics`. Deliberately returns plain numbers rather
//! than `prometheus` types: this crate has no opinion on a metrics
//! backend, and doesn't know the `group_id` label a caller hosting
//! multiple concurrent groups needs to attach -- see docs/raft.md on why
//! that label is required at all.
//!
//! `leader_changes_total`/`elections_total` aren't in `RaftMetrics`
//! itself (it's a snapshot, not a counter of transitions), so
//! `watch_transitions` below derives them by diffing consecutive
//! snapshots off the metrics watch channel `Raft::metrics()` already
//! exposes.

use std::collections::BTreeMap;

use openraft::{RaftMetrics, RaftTypeConfig, ServerState};

use crate::NodeId;

/// One group's metrics at a point in time, ready to be copied into
/// whatever Prometheus gauges the caller registered for its `group_id`.
#[derive(Debug, Clone, Default)]
pub struct GroupMetrics {
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub leader: Option<NodeId>,
    pub is_leader: bool,
    /// `commit_index - matched_index` per follower. Only ever non-empty
    /// when this node is the group's leader -- openraft only tracks
    /// followers' match indices on the leader (docs/observability.md's
    /// `replication_lag{group_id, follower_node_id}`).
    pub replication_lag: BTreeMap<NodeId, u64>,
}

pub fn snapshot<C>(id: NodeId, m: &RaftMetrics<C::NodeId, C::Node>) -> GroupMetrics
where
    C: RaftTypeConfig<NodeId = NodeId>,
{
    let commit_index = m.last_log_index.unwrap_or(0);
    let applied_index = m.last_applied.map(|l| l.index).unwrap_or(0);

    let mut replication_lag = BTreeMap::new();
    if let Some(repl) = &m.replication {
        for (follower, log_id) in repl.iter() {
            if *follower == id {
                continue;
            }
            let matched = log_id.map(|l| l.index).unwrap_or(0);
            replication_lag.insert(*follower, commit_index.saturating_sub(matched));
        }
    }

    GroupMetrics {
        term: m.current_term,
        commit_index,
        applied_index,
        leader: m.current_leader,
        is_leader: m.current_leader == Some(id),
        replication_lag,
    }
}

/// Watch `raft`'s metrics stream for the rest of the process's life,
/// calling `on_leader_change` whenever `current_leader` changes (covers
/// both "a new leader was elected" and "this group lost its leader") and
/// `on_election` whenever this node's own state transitions *into*
/// `Candidate` (docs/observability.md's `raft_leader_changes_total`/
/// `raft_elections_total`). Callers own the actual counters -- typically
/// closures over a `prometheus::IntCounter::inc()`.
pub fn watch_transitions<C>(
    raft: openraft::Raft<C>,
    mut on_leader_change: impl FnMut() + Send + 'static,
    mut on_election: impl FnMut() + Send + 'static,
) -> tokio::task::JoinHandle<()>
where
    C: RaftTypeConfig<NodeId = NodeId>,
{
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        let mut prev = rx.borrow().clone();
        while rx.changed().await.is_ok() {
            let curr = rx.borrow().clone();
            if curr.current_leader != prev.current_leader {
                on_leader_change();
            }
            if curr.state == ServerState::Candidate && prev.state != ServerState::Candidate {
                on_election();
            }
            prev = curr;
        }
    })
}

/// Whether this group looks able to serve traffic right now
/// (docs/observability.md's `/health`: "for at least one shard it hosts,
/// it can reach a quorum"). A known leader is used as the proxy for
/// quorum health: openraft only ever reports one once a quorum has
/// actually accepted its leadership.
pub fn is_group_healthy<C>(m: &RaftMetrics<C::NodeId, C::Node>) -> bool
where
    C: RaftTypeConfig<NodeId = NodeId>,
{
    m.current_leader.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, TypeConfig};
    use openraft::{LogId, Vote};

    fn base_metrics(id: NodeId) -> RaftMetrics<NodeId, Node> {
        RaftMetrics::<NodeId, Node>::new_initial(id)
    }

    #[test]
    fn test_snapshot_leader_has_no_replication_before_any_reported() {
        let m = base_metrics(1);
        let gm = snapshot::<TypeConfig>(1, &m);
        assert_eq!(gm.term, 0);
        assert_eq!(gm.commit_index, 0);
        assert_eq!(gm.applied_index, 0);
        assert!(gm.replication_lag.is_empty());
        assert!(!gm.is_leader);
    }

    #[test]
    fn test_snapshot_computes_replication_lag_excluding_self() {
        let mut m = base_metrics(1);
        m.current_term = 5;
        m.current_leader = Some(1);
        m.last_log_index = Some(10);
        m.last_applied = Some(LogId::new(openraft::LeaderId::new(5, 1), 10));

        let mut repl = std::collections::BTreeMap::new();
        repl.insert(1u64, Some(LogId::new(openraft::LeaderId::new(5, 1), 10))); // self
        repl.insert(2u64, Some(LogId::new(openraft::LeaderId::new(5, 1), 7))); // behind
        repl.insert(3u64, None); // never replicated
        m.replication = Some(repl);

        let gm = snapshot::<TypeConfig>(1, &m);
        assert!(gm.is_leader);
        assert_eq!(gm.commit_index, 10);
        assert_eq!(gm.applied_index, 10);
        assert_eq!(gm.replication_lag.len(), 2);
        assert_eq!(gm.replication_lag[&2], 3);
        assert_eq!(gm.replication_lag[&3], 10);
    }

    #[test]
    fn test_is_group_healthy_requires_known_leader() {
        let m = base_metrics(1);
        assert!(!is_group_healthy::<TypeConfig>(&m));

        let mut m2 = base_metrics(1);
        m2.current_leader = Some(1);
        assert!(is_group_healthy::<TypeConfig>(&m2));
    }

    #[test]
    fn test_vote_field_unused_but_metrics_still_constructible() {
        // Sanity check that `new_initial` gives a usable default vote,
        // since RaftMetrics has no public constructor for arbitrary field
        // values -- tests above mutate the struct directly instead.
        let m = base_metrics(9);
        assert_eq!(m.vote, Vote::default());
    }
}
