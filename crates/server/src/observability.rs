//! The subset of docs/observability.md's required metrics that this
//! crate can genuinely report on its own: request counts/latency and
//! connection/memory gauges. `raft_*`/`wal_*` metrics live where the
//! real Raft groups and WAL actually run (`test-utils`'s `test_node` for
//! now -- see PLAN.md's Phase 8/9 note on `crates/server` not yet being
//! wired to real shards/membership). `cluster_nodes`/`cluster_shards`
//! are reported from whatever `ClusterRouting` this server was given,
//! which is the only cluster-shaped state it holds.

use metrics::prometheus::{CounterVec, HistogramOpts, HistogramVec, IntGauge, Opts, Registry};
use std::sync::Arc;

pub struct ServerMetrics {
    pub requests_total: CounterVec,
    pub requests_failed_total: CounterVec,
    pub command_duration_seconds: HistogramVec,
    pub connections_active: IntGauge,
    pub memory_used_bytes: IntGauge,
    pub cluster_nodes: IntGauge,
    pub cluster_shards: IntGauge,
}

impl ServerMetrics {
    pub fn new(registry: &Registry) -> anyhow::Result<Arc<Self>> {
        let requests_total = CounterVec::new(
            Opts::new("requests_total", "Commands processed"),
            &["command"],
        )?;
        let requests_failed_total = CounterVec::new(
            Opts::new("requests_failed_total", "Commands that returned an error"),
            &["command", "error_kind"],
        )?;
        let command_duration_seconds = HistogramVec::new(
            HistogramOpts::new("command_duration_seconds", "Command dispatch latency"),
            &["command"],
        )?;
        let connections_active =
            IntGauge::new("connections_active", "Currently open client connections")?;
        let memory_used_bytes = IntGauge::new(
            "memory_used_bytes",
            "Best-effort process RSS (Linux /proc/self/statm)",
        )?;
        let cluster_nodes = IntGauge::new(
            "cluster_nodes",
            "Distinct shards seen in this node's slot table (proxy for node count -- \
             crates/server has no separate membership view, see docs/observability.md)",
        )?;
        let cluster_shards = IntGauge::new("cluster_shards", "Distinct shards owning slots")?;

        registry.register(Box::new(requests_total.clone()))?;
        registry.register(Box::new(requests_failed_total.clone()))?;
        registry.register(Box::new(command_duration_seconds.clone()))?;
        registry.register(Box::new(connections_active.clone()))?;
        registry.register(Box::new(memory_used_bytes.clone()))?;
        registry.register(Box::new(cluster_nodes.clone()))?;
        registry.register(Box::new(cluster_shards.clone()))?;

        Ok(Arc::new(ServerMetrics {
            requests_total,
            requests_failed_total,
            command_duration_seconds,
            connections_active,
            memory_used_bytes,
            cluster_nodes,
            cluster_shards,
        }))
    }

    pub fn record(&self, command: &str, response: &protocol::Frame, elapsed: std::time::Duration) {
        self.requests_total.with_label_values(&[command]).inc();
        self.command_duration_seconds
            .with_label_values(&[command])
            .observe(elapsed.as_secs_f64());
        if let protocol::Frame::Error(msg) = response {
            let kind = msg.split_whitespace().next().unwrap_or("ERR");
            self.requests_failed_total
                .with_label_values(&[command, kind])
                .inc();
        }
    }
}

/// Best-effort resident set size in bytes, read fresh on every call from
/// `/proc/self/statm` (Linux only). `docs/performance.md` already flags
/// precise per-key memory accounting as a deferred limitation; this is
/// the same honest tradeoff applied to the process-wide gauge instead of
/// not reporting one at all. Returns `None` off Linux or if the read
/// fails for any reason.
pub fn read_rss_bytes() -> Option<i64> {
    // Avoid a `libc` dependency for one constant: 4096 is the page size on
    // every Linux architecture this project targets (x86_64, aarch64).
    const PAGE_SIZE: i64 = 4096;
    let contents = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: i64 = contents.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * PAGE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_registers_all_metrics_without_error() {
        let registry = Registry::new();
        let m = ServerMetrics::new(&registry).unwrap();
        m.connections_active.inc();
        // Vec-labeled metrics (CounterVec/HistogramVec) only appear in a
        // gather() once at least one label combination has been touched
        // -- an empty CounterVec has no children to report.
        m.record(
            "PING",
            &protocol::Frame::ok(),
            std::time::Duration::from_millis(1),
        );

        let families = registry.gather();
        let names: Vec<_> = families.iter().map(|f| f.get_name().to_string()).collect();
        for expected in [
            "requests_total",
            "command_duration_seconds",
            "connections_active",
            "memory_used_bytes",
            "cluster_nodes",
            "cluster_shards",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    #[test]
    fn test_record_tracks_success_and_failure_separately() {
        let registry = Registry::new();
        let m = ServerMetrics::new(&registry).unwrap();
        m.record(
            "GET",
            &protocol::Frame::ok(),
            std::time::Duration::from_millis(1),
        );
        m.record(
            "GET",
            &protocol::Frame::error("ERR boom"),
            std::time::Duration::from_millis(1),
        );
        assert_eq!(m.requests_total.with_label_values(&["GET"]).get(), 2.0);
        assert_eq!(
            m.requests_failed_total
                .with_label_values(&["GET", "ERR"])
                .get(),
            1.0
        );
    }

    #[test]
    fn test_read_rss_bytes_is_positive_on_linux() {
        let rss = read_rss_bytes();
        assert!(rss.is_some_and(|v| v > 0));
    }
}
