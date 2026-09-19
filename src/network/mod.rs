use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::cluster::{run_heartbeat_task, SharedCluster, SharedHealth};
use crate::commands::{execute, CommandContext};
use crate::consensus::Raft;
use crate::observability::Metrics;
use crate::persistence::Aof;
use protocol::{parse_frame, write_frame_into, Frame};
use crate::replication::{snapshot_entry_to_resp, Replication};
use storage::Store;

pub struct Server {
    store:   Arc<Store>,
    aof:     Option<Arc<Aof>>,
    repl:    Arc<Replication>,
    raft:    Option<Arc<Raft>>,
    cluster: Option<SharedCluster>,
    health:  Option<SharedHealth>,
    my_addr: Option<String>,
    metrics: Arc<Metrics>,
}

impl Server {
    pub fn new(
        store:   Arc<Store>,
        aof:     Option<Arc<Aof>>,
        repl:    Arc<Replication>,
        raft:    Option<Arc<Raft>>,
        cluster: Option<SharedCluster>,
        health:  Option<SharedHealth>,
        my_addr: Option<String>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Server { store, aof, repl, raft, cluster, health, my_addr, metrics }
    }

    pub async fn run(self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("loony-redis listening on {addr}");

        let store   = self.store;
        let aof     = self.aof;
        let repl    = self.repl;
        let raft    = self.raft;
        let cluster = self.cluster;
        let health  = self.health;
        let metrics = self.metrics;

        // Spawn heartbeat task when cluster + health are both active.
        if let (Some(c), Some(h), Some(my)) = (cluster.clone(), health.clone(), self.my_addr) {
            tokio::spawn(run_heartbeat_task(my, c, h));
        }

        loop {
            let (socket, peer) = listener.accept().await?;
            debug!("new connection from {peer}");

            let store   = store.clone();
            let aof     = aof.clone();
            let repl    = repl.clone();
            let raft    = raft.clone();
            let cluster = cluster.clone();
            let health  = health.clone();
            let metrics = metrics.clone();

            tokio::spawn(async move {
                // Disable Nagle — crucial for request/response latency.
                let _ = socket.set_nodelay(true);

                metrics.conn_open();
                let ctx = CommandContext {
                    store: store.clone(),
                    aof,
                    repl: repl.clone(),
                    raft: raft.clone(),
                    cluster: cluster.clone(),
                    health,
                    metrics: Some(metrics.clone()),
                    is_replica_replay: false,
                };
                if let Err(e) = handle_connection(socket, ctx, store, repl).await {
                    if !is_conn_reset(&e) {
                        error!("connection {peer} error: {e}");
                    }
                }
                metrics.conn_close();
                debug!("connection {peer} closed");
            });
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    ctx: CommandContext,
    store: Arc<Store>,
    repl: Arc<Replication>,
) -> anyhow::Result<()> {
    // 32 KB read buffer — reduces read(2) syscalls under pipelined workloads.
    let mut buf = BytesMut::with_capacity(32 * 1024);
    // Accumulate all serialised responses for one read burst, then flush once.
    // This reduces write(2) syscalls from N (one per command) to 1 per batch.
    let mut out = BytesMut::with_capacity(32 * 1024);

    loop {
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);

                    // SYNC is handled at the network layer; flush pending
                    // responses before switching the connection to streaming mode.
                    if is_sync_command(&frame) {
                        if !out.is_empty() {
                            socket.write_all(&out).await?;
                            out.clear();
                        }
                        info!("New replica connected, starting full-sync");
                        handle_replica_sync(socket, store, repl).await?;
                        return Ok(());
                    }

                    let response = execute(frame, &ctx).await;
                    // Serialise directly into the accumulator — no per-response alloc.
                    write_frame_into(&mut out, &response);
                }
                Ok(None) => break,
                Err(e) => {
                    write_frame_into(&mut out, &Frame::error(format!("ERR protocol error: {e}")));
                    buf.clear();
                    break;
                }
            }
        }

        // One syscall for the entire pipeline batch.
        if !out.is_empty() {
            socket.write_all(&out).await?;
            out.clear();
        }
    }
}

fn is_sync_command(frame: &Frame) -> bool {
    if let Frame::Array(Some(args)) = frame {
        if let Some(Frame::Bulk(Some(cmd))) = args.first() {
            return cmd.to_ascii_uppercase() == b"SYNC";
        }
    }
    false
}

/// Stream a full snapshot then live writes to a newly-connected replica.
async fn handle_replica_sync(
    mut socket: TcpStream,
    store: Arc<Store>,
    repl: Arc<Replication>,
) -> anyhow::Result<()> {
    // Subscribe BEFORE taking the snapshot so we don't miss writes that arrive
    // while we're serialising and sending it.
    let mut rx = repl.subscribe();

    // Snapshot
    let entries = store.snapshot();
    let count = entries.len();
    for entry in &entries {
        socket.write_all(&snapshot_entry_to_resp(entry)).await?;
    }
    info!("Snapshot sent: {count} keys");

    // Drain any writes that arrived during snapshot transfer.
    // Then forward the live stream indefinitely.
    loop {
        match rx.recv().await {
            Ok(bytes) => {
                socket.write_all(&bytes).await?;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("Replica lagged by {n} messages — disconnecting (will reconnect)");
                return Ok(());
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Ok(());
            }
        }
    }
}

// ── Follower background task ───────────────────────────────────────────────

/// Connect to the leader, receive full-sync + live stream, apply to `store`.
/// Retries with exponential backoff on failure.
pub async fn run_follower_loop(
    leader_addr: String,
    store: Arc<Store>,
    repl: Arc<Replication>,
) {
    let mut backoff = Duration::from_millis(500);
    loop {
        info!("Connecting to leader at {leader_addr}");
        match replicate_from_leader(&leader_addr, &store, &repl).await {
            Ok(()) => {
                info!("Leader connection closed");
            }
            Err(e) => {
                error!("Replication error: {e}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
        info!("Reconnecting in {:?}", backoff);
    }
}

async fn replicate_from_leader(
    leader_addr: &str,
    store: &Arc<Store>,
    repl: &Arc<Replication>,
) -> anyhow::Result<()> {
    let mut socket = TcpStream::connect(leader_addr).await?;
    info!("Connected to leader {leader_addr}");

    // Send SYNC
    socket.write_all(b"*1\r\n$4\r\nSYNC\r\n").await?;

    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    let ctx = CommandContext {
                        store: store.clone(),
                        aof: None,
                        repl: repl.clone(),
                        raft: None,
                        cluster: None,
                        health: None,
                        metrics: None,
                        is_replica_replay: true,
                    };
                    execute(frame, &ctx).await;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("Replication stream parse error: {e}");
                    buf.clear();
                    break;
                }
            }
        }
    }
}

fn is_conn_reset(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .map(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            )
        })
        .unwrap_or(false)
}
