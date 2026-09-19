//! Wires docs/observability.md's `raft_*`/`wal_*` metrics for `test_node`
//! -- the only binary in this workspace that currently runs a real Raft
//! group with a real WAL (`crates/server` isn't wired to either yet, see
//! PLAN.md's tracked Phase 8/9 gap), so it's the one place these can be
//! demonstrated scraping real, non-fabricated numbers end-to-end.
//!
//! `test_node` only ever hosts one group, so `group_id` is a constant
//! label here rather than something computed per call -- a binary
//! hosting several concurrent groups (a real future shard server) would
//! pass a different value per group instead.

use std::sync::Arc;

use metrics::prometheus::{
    Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts, Registry,
};

pub struct NodeMetrics {
    pub raft_term: IntGauge,
    pub raft_commit_index: IntGauge,
    pub raft_applied_index: IntGauge,
    pub raft_leader_changes_total: IntCounter,
    pub raft_elections_total: IntCounter,
    pub replication_lag: IntGaugeVec,
    pub wal_bytes_written: IntGauge,
    pub wal_fsync_duration_seconds: Histogram,
}

impl NodeMetrics {
    pub fn new(registry: &Registry, group_id: &str) -> anyhow::Result<Arc<Self>> {
        let cl = |name: &str, help: &str| Opts::new(name, help).const_label("group_id", group_id);

        let raft_term = IntGauge::with_opts(cl("raft_term", "Current Raft term"))?;
        let raft_commit_index =
            IntGauge::with_opts(cl("raft_commit_index", "Last log index appended"))?;
        let raft_applied_index =
            IntGauge::with_opts(cl("raft_applied_index", "Last log index applied"))?;
        let raft_leader_changes_total = IntCounter::with_opts(cl(
            "raft_leader_changes_total",
            "Times this node observed current_leader change",
        ))?;
        let raft_elections_total = IntCounter::with_opts(cl(
            "raft_elections_total",
            "Times this node became a Candidate",
        ))?;
        let replication_lag = IntGaugeVec::new(
            Opts::new(
                "replication_lag",
                "commit_index - matched_index per follower (leader only)",
            )
            .const_label("group_id", group_id),
            &["follower_node_id"],
        )?;
        let wal_bytes_written = IntGauge::with_opts(cl(
            "wal_bytes_written",
            "Cumulative bytes written to this group's WAL",
        ))?;
        let wal_fsync_duration_seconds = Histogram::with_opts(
            HistogramOpts::new("wal_fsync_duration_seconds", "Per-fsync duration")
                .const_label("group_id", group_id),
        )?;

        registry.register(Box::new(raft_term.clone()))?;
        registry.register(Box::new(raft_commit_index.clone()))?;
        registry.register(Box::new(raft_applied_index.clone()))?;
        registry.register(Box::new(raft_leader_changes_total.clone()))?;
        registry.register(Box::new(raft_elections_total.clone()))?;
        registry.register(Box::new(replication_lag.clone()))?;
        registry.register(Box::new(wal_bytes_written.clone()))?;
        registry.register(Box::new(wal_fsync_duration_seconds.clone()))?;

        Ok(Arc::new(NodeMetrics {
            raft_term,
            raft_commit_index,
            raft_applied_index,
            raft_leader_changes_total,
            raft_elections_total,
            replication_lag,
            wal_bytes_written,
            wal_fsync_duration_seconds,
        }))
    }

    /// Copy a freshly-computed `raft::metrics::GroupMetrics` into the
    /// gauges. Replaces the whole `replication_lag` label set each call
    /// (a follower that drops out of `m.replication_lag` -- e.g. this
    /// node lost leadership -- must stop being reported, not linger at
    /// its last value).
    pub fn update_raft(&self, term: u64, m: &raft::metrics::GroupMetrics) {
        self.raft_term.set(term as i64);
        self.raft_commit_index.set(m.commit_index as i64);
        self.raft_applied_index.set(m.applied_index as i64);
        self.replication_lag.reset();
        for (follower, lag) in &m.replication_lag {
            self.replication_lag
                .with_label_values(&[&follower.to_string()])
                .set(*lag as i64);
        }
    }

    /// Fold in a `persistence::WalStats` sample taken since `prev`,
    /// observing one histogram sample per fsync that happened in the
    /// interval at that interval's average duration -- an honest
    /// approximation given the WAL only tracks cumulative totals, not
    /// individual fsync call sites (see `persistence::Wal::stats`).
    pub fn update_wal(&self, prev: persistence::WalStats, curr: persistence::WalStats) {
        self.wal_bytes_written.set(curr.bytes_written as i64);
        let new_fsyncs = curr.fsync_count.saturating_sub(prev.fsync_count);
        if new_fsyncs > 0 {
            let delta = curr.fsync_duration_total - prev.fsync_duration_total;
            let avg = delta.as_secs_f64() / new_fsyncs as f64;
            for _ in 0..new_fsyncs {
                self.wal_fsync_duration_seconds.observe(avg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new_registers_every_metric() {
        let registry = Registry::new();
        let m = NodeMetrics::new(&registry, "shard").unwrap();
        m.raft_term.set(1);
        m.replication_lag.with_label_values(&["2"]).set(5);
        m.wal_fsync_duration_seconds.observe(0.001);

        let names: Vec<_> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        for expected in [
            "raft_term",
            "raft_commit_index",
            "raft_applied_index",
            "raft_leader_changes_total",
            "raft_elections_total",
            "replication_lag",
            "wal_bytes_written",
            "wal_fsync_duration_seconds",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    #[test]
    fn test_update_raft_replaces_replication_lag_labels() {
        let registry = Registry::new();
        let m = NodeMetrics::new(&registry, "shard").unwrap();

        let mut gm = raft::metrics::GroupMetrics {
            commit_index: 10,
            ..Default::default()
        };
        gm.replication_lag.insert(2, 3);
        m.update_raft(5, &gm);
        assert_eq!(m.replication_lag.with_label_values(&["2"]).get(), 3);

        // Node 2 drops out (e.g. leadership lost) -- must not linger.
        let gm2 = raft::metrics::GroupMetrics::default();
        m.update_raft(5, &gm2);
        assert_eq!(m.replication_lag.with_label_values(&["2"]).get(), 0);
    }

    #[test]
    fn test_update_wal_observes_one_sample_per_new_fsync() {
        let registry = Registry::new();
        let m = NodeMetrics::new(&registry, "shard").unwrap();

        let prev = persistence::WalStats::default();
        let curr = persistence::WalStats {
            bytes_written: 100,
            fsync_count: 2,
            fsync_duration_total: Duration::from_millis(20),
        };
        m.update_wal(prev, curr);
        assert_eq!(m.wal_bytes_written.get(), 100);
        assert_eq!(m.wal_fsync_duration_seconds.get_sample_count(), 2);
    }
}
