use loony_redis::{cluster, commands, consensus, network, observability, persistence, replication, storage};

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "loony-redis", about = "A Redis-compatible in-memory KV store")]
struct Args {
    /// Bind address for client connections
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// TCP port for client connections
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Path to Append-Only File (omit to disable)
    #[arg(long)]
    aof: Option<String>,

    // ── Phase-4 async replication ──────────────────────────────────────────

    /// Replicate from this leader address (Phase-4 async replication)
    #[arg(long)]
    replicaof: Option<String>,

    // ── Phase-5 Raft consensus ─────────────────────────────────────────────

    /// This node's Raft address, e.g. 127.0.0.1:7379
    #[arg(long)]
    raft_addr: Option<String>,

    /// Comma-separated Raft addresses of peer nodes
    #[arg(long)]
    peers: Option<String>,

    // ── Phase-6 cluster sharding ───────────────────────────────────────────

    /// All cluster node addresses (comma-separated), e.g.
    /// 127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381
    #[arg(long)]
    cluster_nodes: Option<String>,

    /// This node's own address as it appears in --cluster-nodes
    #[arg(long)]
    cluster_self: Option<String>,

    // ── Phase-10 observability ─────────────────────────────────────────────

    /// Address for the Prometheus metrics + health HTTP server (default disabled)
    #[arg(long, default_value = "127.0.0.1:9090")]
    metrics_addr: String,

    /// Disable the metrics HTTP server
    #[arg(long, default_value_t = false)]
    no_metrics: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Phase-10: Metrics — initialise before anything else so uptime starts from t=0.
    let metrics = observability::Metrics::new(args.host.clone(), args.port);

    let store = Arc::new(storage::Store::new());

    // Phase-4 replication state.
    let repl = if let Some(ref leader) = args.replicaof {
        info!("Starting as replica of {leader}");
        Arc::new(replication::Replication::new_follower(leader.clone()))
    } else {
        Arc::new(replication::Replication::new_leader())
    };

    // Replay AOF before accepting connections.
    let aof = if let Some(ref path) = args.aof {
        let frames = persistence::load_frames(path).await?;
        if !frames.is_empty() {
            info!("Replaying {} AOF commands", frames.len());
            let replay_ctx = commands::CommandContext {
                store: store.clone(),
                aof: None,
                repl: repl.clone(),
                raft: None,
                cluster: None,
                health: None,
                metrics: None,
                is_replica_replay: true,
            };
            for frame in frames {
                commands::execute(frame, &replay_ctx).await;
            }
        }
        Some(Arc::new(persistence::Aof::open(path).await?))
    } else {
        None
    };

    // Phase-5: Raft consensus.
    let raft = if let (Some(raft_addr), Some(peers_str)) =
        (&args.raft_addr, &args.peers)
    {
        let peers: Vec<String> = peers_str
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        info!("Starting Raft node {} with {} peers", raft_addr, peers.len());

        let store_for_apply = store.clone();
        let repl_for_apply = repl.clone();
        let apply_fn: consensus::ApplyFn = Arc::new(move |frame| {
            let store = store_for_apply.clone();
            let repl = repl_for_apply.clone();
            Box::pin(async move {
                let ctx = commands::CommandContext {
                    store,
                    aof: None,
                    repl,
                    raft: None,
                    cluster: None,
                    health: None,
                    metrics: None,
                    is_replica_replay: true,
                };
                commands::execute(frame, &ctx).await
            })
        });

        Some(consensus::start_raft(raft_addr.clone(), peers, raft_addr.clone(), apply_fn).await)
    } else {
        None
    };

    // Phase-6: cluster sharding + Phase-8: health table.
    let (cluster, health, cluster_self) = if let (Some(nodes_str), Some(self_addr)) =
        (&args.cluster_nodes, &args.cluster_self)
    {
        let nodes: Vec<String> = nodes_str
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        let cfg = cluster::ClusterConfig::new(nodes.clone(), self_addr)?;
        let ranges = cfg.slot_ranges_for(cfg.my_index);
        info!(
            "Cluster mode: node {} owns slots {:?}",
            self_addr, ranges
        );

        let peers: Vec<String> = nodes.iter().filter(|a| *a != self_addr).cloned().collect();
        let health = cluster::HealthTable::new(&peers);
        let shared_cluster = Arc::new(tokio::sync::RwLock::new(
            cluster::ClusterState { config: cfg },
        ));
        (Some(shared_cluster), Some(health), Some(self_addr.clone()))
    } else {
        (None, None, None)
    };

    // Phase-10: Start metrics HTTP server.
    if !args.no_metrics {
        observability::start_metrics_server(
            args.metrics_addr.clone(),
            metrics.clone(),
            store.clone(),
            repl.clone(),
            cluster.clone(),
            health.clone(),
        );
    }

    // Phase-4 follower loop (only without Raft).
    if raft.is_none() {
        if let Some(ref leader) = args.replicaof {
            let leader = leader.clone();
            let store2 = store.clone();
            let repl2 = repl.clone();
            tokio::spawn(async move {
                network::run_follower_loop(leader, store2, repl2).await;
            });
        }
    }

    let server = network::Server::new(store, aof, repl, raft, cluster, health, cluster_self, metrics);
    server.run(&format!("{}:{}", args.host, args.port)).await
}
