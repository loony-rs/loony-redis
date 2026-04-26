use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::cluster::{NodeState, SharedCluster, SharedHealth};
use crate::replication::Replication;
use crate::storage::Store;

// ── Per-command counters ───────────────────────────────────────────────────

struct CmdStats {
    ok:  AtomicU64,
    err: AtomicU64,
}

impl CmdStats {
    fn new() -> Self {
        CmdStats { ok: AtomicU64::new(0), err: AtomicU64::new(0) }
    }
}

// ── Metrics ────────────────────────────────────────────────────────────────

pub struct Metrics {
    pub start_time:         Instant,
    pub host:               String,
    pub port:               u16,
    pub connections_active: AtomicI64,
    pub connections_total:  AtomicU64,
    commands_ok:            AtomicU64,
    commands_err:           AtomicU64,
    per_cmd:                DashMap<String, CmdStats>,
}

impl Metrics {
    pub fn new(host: String, port: u16) -> Arc<Self> {
        Arc::new(Metrics {
            start_time:         Instant::now(),
            host,
            port,
            connections_active: AtomicI64::new(0),
            connections_total:  AtomicU64::new(0),
            commands_ok:        AtomicU64::new(0),
            commands_err:       AtomicU64::new(0),
            per_cmd:            DashMap::new(),
        })
    }

    pub fn record_cmd(&self, cmd: &str, is_err: bool) {
        let entry = self.per_cmd.entry(cmd.to_string()).or_insert_with(CmdStats::new);
        if is_err {
            self.commands_err.fetch_add(1, Ordering::Relaxed);
            entry.err.fetch_add(1, Ordering::Relaxed);
        } else {
            self.commands_ok.fetch_add(1, Ordering::Relaxed);
            entry.ok.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn conn_open(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn conn_close(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn total_commands(&self) -> u64 {
        self.commands_ok.load(Ordering::Relaxed) + self.commands_err.load(Ordering::Relaxed)
    }

    pub fn total_connections(&self) -> u64 {
        self.connections_total.load(Ordering::Relaxed)
    }

    pub fn active_connections(&self) -> i64 {
        self.connections_active.load(Ordering::Relaxed)
    }

    async fn prometheus_text(
        &self,
        store: &Store,
        repl: &Replication,
        cluster: &Option<SharedCluster>,
        health: &Option<SharedHealth>,
    ) -> String {
        let mut out = String::with_capacity(4096);

        // Helper macro: single-value metric block
        macro_rules! gauge {
            ($name:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP {0} {1}\n# TYPE {0} gauge\n{0} {2}\n\n",
                    $name, $help, $val
                ))
            };
        }
        macro_rules! counter {
            ($name:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP {0} {1}\n# TYPE {0} counter\n{0} {2}\n\n",
                    $name, $help, $val
                ))
            };
        }

        counter!("loony_uptime_seconds",
            "Seconds since server start",
            self.uptime_secs());

        gauge!("loony_connections_active",
            "Currently active client connections",
            self.connections_active.load(Ordering::Relaxed));

        counter!("loony_connections_total",
            "Total client connections accepted since start",
            self.connections_total.load(Ordering::Relaxed));

        // commands_total — labelled by status
        out.push_str("# HELP loony_commands_total Commands processed, by status\n");
        out.push_str("# TYPE loony_commands_total counter\n");
        out.push_str(&format!(
            "loony_commands_total{{status=\"ok\"}} {}\n",
            self.commands_ok.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "loony_commands_total{{status=\"err\"}} {}\n\n",
            self.commands_err.load(Ordering::Relaxed)
        ));

        // loony_cmd_total — per-command, per-status
        out.push_str("# HELP loony_cmd_total Commands processed per command type\n");
        out.push_str("# TYPE loony_cmd_total counter\n");
        let mut cmds: Vec<(String, u64, u64)> = self.per_cmd.iter().map(|e| {
            (
                e.key().clone(),
                e.value().ok.load(Ordering::Relaxed),
                e.value().err.load(Ordering::Relaxed),
            )
        }).collect();
        cmds.sort_by(|a, b| a.0.cmp(&b.0));
        for (cmd, ok, err) in &cmds {
            if *ok > 0 {
                out.push_str(&format!(
                    "loony_cmd_total{{cmd=\"{cmd}\",status=\"ok\"}} {ok}\n"
                ));
            }
            if *err > 0 {
                out.push_str(&format!(
                    "loony_cmd_total{{cmd=\"{cmd}\",status=\"err\"}} {err}\n"
                ));
            }
        }
        out.push('\n');

        // keyspace
        let (total_keys, keys_with_expiry) = store.keyspace_info();
        gauge!("loony_keys_total",            "Total keys in the store", total_keys);
        gauge!("loony_keys_with_expiry_total", "Keys that have a TTL set", keys_with_expiry);

        // replication
        counter!("loony_replication_commits_total",
            "Write commands committed on this node",
            repl.commit_count.load(Ordering::Relaxed));

        // cluster slot ownership
        if let Some(c) = cluster {
            let state = c.read().await;
            let slots_owned: u32 = state.config
                .slot_ranges_for(state.config.my_index)
                .iter()
                .map(|(s, e)| (e - s + 1) as u32)
                .sum();
            gauge!("loony_cluster_slots_owned",
                "Hash slots owned by this node", slots_owned);
            gauge!("loony_cluster_nodes_total",
                "Number of nodes in the cluster", state.config.nodes.len());
        }

        // peer health
        if let Some(h) = health {
            let peers = h.snapshot().await;
            if !peers.is_empty() {
                out.push_str("# HELP loony_peer_alive Peer liveness: 1=alive, 0=suspected/failed\n");
                out.push_str("# TYPE loony_peer_alive gauge\n");
                for (addr, state) in &peers {
                    let v = if *state == NodeState::Alive { 1 } else { 0 };
                    out.push_str(&format!("loony_peer_alive{{peer=\"{addr}\"}} {v}\n"));
                }
                out.push('\n');

                out.push_str("# HELP loony_peer_failed Peer is in failed state: 1=failed\n");
                out.push_str("# TYPE loony_peer_failed gauge\n");
                for (addr, state) in &peers {
                    let v = if *state == NodeState::Failed { 1 } else { 0 };
                    out.push_str(&format!("loony_peer_failed{{peer=\"{addr}\"}} {v}\n"));
                }
                out.push('\n');
            }
        }

        out
    }

    async fn is_ready(&self, health: &Option<SharedHealth>) -> bool {
        if let Some(h) = health {
            let peers = h.snapshot().await;
            return !peers.iter().any(|(_, s)| *s == NodeState::Failed);
        }
        true
    }
}

// ── HTTP metrics server ────────────────────────────────────────────────────

pub fn start_metrics_server(
    addr:    String,
    metrics: Arc<Metrics>,
    store:   Arc<Store>,
    repl:    Arc<Replication>,
    cluster: Option<SharedCluster>,
    health:  Option<SharedHealth>,
) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("metrics server bind {addr} failed: {e}");
                return;
            }
        };
        info!("metrics  http://{addr}/metrics  health http://{addr}/health");

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let metrics = metrics.clone();
            let store   = store.clone();
            let repl    = repl.clone();
            let cluster = cluster.clone();
            let health  = health.clone();

            tokio::spawn(async move {
                let mut buf = vec![0u8; 2048];
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request = std::str::from_utf8(&buf[..n]).unwrap_or("");
                // Extract path from "GET /path HTTP/1.1"
                let path = request
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");

                let (status, reason, ct, body) = match path {
                    "/metrics" => {
                        let body = metrics
                            .prometheus_text(&store, &repl, &cluster, &health)
                            .await;
                        (200u16, "OK", "text/plain; version=0.0.4; charset=utf-8", body)
                    }
                    "/health" => (
                        200, "OK",
                        "application/json",
                        "{\"status\":\"ok\"}\n".to_string(),
                    ),
                    "/ready" => {
                        if metrics.is_ready(&health).await {
                            (200, "OK",   "application/json", "{\"status\":\"ok\"}\n".to_string())
                        } else {
                            (503, "Service Unavailable", "application/json",
                             "{\"status\":\"degraded\"}\n".to_string())
                        }
                    }
                    _ => (404, "Not Found", "text/plain", "not found\n".to_string()),
                };

                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\n\
                     Content-Type: {ct}\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\
                     \r\n\
                     {body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
}
