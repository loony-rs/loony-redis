//! Standalone Raft node process for Phase 8's fault-injection harness.
//! Not a production binary -- `crates/server` doesn't wire sharding/Raft
//! together yet (that's a later phase); this exists purely so
//! `test-utils`'s tests can exercise real OS processes and a real
//! network-fault proxy, per docs/testing.md.

use bytes::Bytes;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use test_utils::admin::{to_io_err, AdminRequest, AdminResponse, MetricsSnapshot};
use test_utils::node_metrics::NodeMetrics;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    id: u64,

    #[arg(long)]
    dir: String,

    #[arg(long)]
    raft_addr: String,

    #[arg(long)]
    admin_addr: String,

    /// `id=addr,id=addr,...` -- every cluster member's real address,
    /// used only for the `--init` node's `initialize()` call.
    #[arg(long)]
    peers: String,

    /// `id=addr,id=addr,...` -- per-target dial overrides for this
    /// node's own outbound Raft RPCs (see `raft::network::Network::with_overrides`).
    /// Omit an id to dial its real `--peers` address directly.
    #[arg(long, default_value = "")]
    dial: String,

    #[arg(long, default_value_t = false)]
    init: bool,

    /// Serves `/metrics`, `/live`, `/ready`, `/health` (docs/observability.md).
    /// Optional: existing call sites that omit it simply run without a
    /// metrics endpoint.
    #[arg(long)]
    metrics_addr: Option<String>,
}

fn parse_map(s: &str) -> HashMap<u64, String> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let (id, addr) = pair.split_once('=')?;
            Some((id.parse().ok()?, addr.to_string()))
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Default to `warn`: openraft is very chatty at `info`, and this
    // binary is only ever driven by test-utils's own test harness --
    // set RUST_LOG=info (or finer) explicitly when debugging a failure.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .init();

    let args = Args::parse();
    let peers = parse_map(&args.peers);
    let dial_overrides = parse_map(&args.dial);

    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 400,
            election_timeout_max: 800,
            ..Default::default()
        }
        .validate()?,
    );

    let links = raft::PartitionControl::new(); // unused in-process; faults are applied by the external proxy
    let network = raft::Network::with_overrides(args.id, links, dial_overrides);

    let dir = std::path::PathBuf::from(&args.dir);
    let (raft_handle, log_store, state_machine) =
        raft::start_node(args.id, &dir, config, network).await?;

    let raft_listener = TcpListener::bind(&args.raft_addr).await?;
    let serve_raft = raft_handle.clone();
    tokio::spawn(async move {
        let _ = raft::network::serve(raft_listener, serve_raft).await;
    });

    let health = Arc::new(metrics::Health::new());
    if let Some(metrics_addr) = &args.metrics_addr {
        let registry = metrics::Registry::new();
        let node_metrics = NodeMetrics::new(&registry, "shard")?;
        spawn_metrics_sampler(raft_handle.clone(), log_store.clone(), node_metrics.clone());
        spawn_health_watcher(raft_handle.clone(), health.clone());
        {
            let leader_changes = node_metrics.clone();
            let elections = node_metrics.clone();
            raft::metrics::watch_transitions::<raft::TypeConfig>(
                raft_handle.clone(),
                move || leader_changes.raft_leader_changes_total.inc(),
                move || elections.raft_elections_total.inc(),
            );
        }
        let metrics_listener = TcpListener::bind(metrics_addr).await?;
        metrics::serve(metrics_listener, registry, health.clone());
    }
    health.set_ready(true);

    if args.init {
        let raft_for_init = raft_handle.clone();
        let members: std::collections::BTreeMap<u64, raft::Node> = peers
            .iter()
            .map(|(&id, addr)| (id, raft::Node { addr: addr.clone() }))
            .collect();
        tokio::spawn(async move {
            // Give peers a moment to start listening before proposing
            // the initial membership.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            if let Err(e) = raft_for_init.initialize(members).await {
                tracing::warn!(
                    "initialize() failed (harmless if the cluster is already initialized): {e}"
                );
            }
        });
    }

    let admin_listener = TcpListener::bind(&args.admin_addr).await?;
    serve_admin(admin_listener, raft_handle, state_machine).await
}

/// Periodically copy this group's live Raft/WAL numbers into `node_metrics`
/// (docs/observability.md's `raft_*`/`wal_*`) -- polling rather than
/// pushing on every change since the WAL's counters aren't event-driven
/// and a 500ms staleness bound is more than adequate for scraping.
fn spawn_metrics_sampler(
    raft: raft::Raft,
    log_store: raft::LogStore,
    node_metrics: Arc<NodeMetrics>,
) {
    tokio::spawn(async move {
        let mut prev_wal = persistence::WalStats::default();
        loop {
            let m = raft.metrics().borrow().clone();
            let gm = raft::metrics::snapshot::<raft::TypeConfig>(m.id, &m);
            node_metrics.update_raft(m.current_term, &gm);

            let curr_wal = log_store.wal_stats().await;
            node_metrics.update_wal(prev_wal, curr_wal);
            prev_wal = curr_wal;

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });
}

/// Drive `/health` off this group's own quorum state (docs/observability.md:
/// "for at least one shard it hosts, it can reach a quorum") -- reacts to
/// the metrics watch channel directly rather than polling on a timer, so
/// `/health` reflects a lost/regained leader without the sampler's delay.
fn spawn_health_watcher(raft: raft::Raft, health: Arc<metrics::Health>) {
    tokio::spawn(async move {
        let mut rx = raft.metrics();
        loop {
            let healthy = raft::metrics::is_group_healthy::<raft::TypeConfig>(&rx.borrow());
            health.set_healthy(healthy);
            if rx.changed().await.is_err() {
                break;
            }
        }
    });
}

async fn serve_admin(
    listener: TcpListener,
    raft: raft::Raft,
    sm: Arc<raft::StateMachineStore>,
) -> anyhow::Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await?;
        let raft = raft.clone();
        let sm = sm.clone();
        tokio::spawn(async move {
            let _ = handle_admin_conn(socket, raft, sm).await;
        });
    }
}

async fn handle_admin_conn(
    mut socket: TcpStream,
    raft: raft::Raft,
    sm: Arc<raft::StateMachineStore>,
) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    socket.read_exact(&mut buf).await?;
    let req: AdminRequest = bincode::deserialize(&buf).map_err(to_io_err)?;

    let resp = match req {
        AdminRequest::Propose { key, value } => {
            let cmd = persistence::Command::Set {
                key,
                value: Bytes::from(value),
                expire_at: None,
            };
            match raft.client_write(cmd).await {
                Ok(r) => AdminResponse::Proposed {
                    index: r.log_id.index,
                },
                Err(e) => AdminResponse::ProposeFailed {
                    message: e.to_string(),
                },
            }
        }
        AdminRequest::Get { key } => {
            let value = match sm.store.get(&key) {
                Some(storage::Value::String(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
                _ => None,
            };
            AdminResponse::Value(value)
        }
        AdminRequest::Metrics => {
            let m = raft.metrics().borrow().clone();
            let gm = raft::metrics::snapshot::<raft::TypeConfig>(m.id, &m);
            AdminResponse::Metrics(MetricsSnapshot {
                current_leader: m.current_leader,
                current_term: m.current_term,
                commit_index: gm.commit_index,
                last_applied_index: m.last_applied.map(|l| l.index),
                state: format!("{:?}", m.state),
                replication_lag: gm.replication_lag.into_iter().collect(),
            })
        }
    };

    let payload = bincode::serialize(&resp).map_err(to_io_err)?;
    socket
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    socket.write_all(&payload).await?;
    Ok(())
}
