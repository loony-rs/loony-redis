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
    let (raft_handle, _log_store, state_machine) =
        raft::start_node(args.id, &dir, config, network).await?;

    let raft_listener = TcpListener::bind(&args.raft_addr).await?;
    let serve_raft = raft_handle.clone();
    tokio::spawn(async move {
        let _ = raft::network::serve(raft_listener, serve_raft).await;
    });

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
            AdminResponse::Metrics(MetricsSnapshot {
                current_leader: m.current_leader,
                current_term: m.current_term,
                last_applied_index: m.last_applied.map(|l| l.index),
                state: format!("{:?}", m.state),
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
