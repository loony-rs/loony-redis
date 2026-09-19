//! Real network-fault proxy (docs/testing.md): each directed link
//! between two nodes gets its own dedicated forwarding task, sitting
//! between the real processes rather than inside either of them.
//!
//! Because openraft's cluster membership address for a peer is
//! replicated (every node has the *same* view of "node j's address"),
//! a single shared proxy in front of node j couldn't tell which caller
//! a connection came from without inspecting source IPs. This harness
//! avoids that entirely: each node process is launched with its own
//! per-target dial override (`raft::network::Network::with_overrides`)
//! pointing every peer at a link-specific proxy address that only that
//! one caller ever uses -- so each `FaultyLink` inherently knows both
//! ends of the link it's gating, no caller identification needed.
//!
//! Partition state is the exact same `raft::network::PartitionControl`
//! a test drives directly -- the proxy consults it live, so
//! `partition`/`heal` calls take effect on already-running links.

use raft::{NodeId, PartitionControl};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

/// Spawn a proxy for the directed link `from -> to`: listens on a fresh
/// ephemeral port and forwards accepted connections to `backend_addr`
/// (node `to`'s real Raft RPC address), unless `links` currently has
/// `(from, to)` blocked, in which case the connection is dropped
/// immediately -- the caller sees this as a network failure (not
/// necessarily "connection refused", but always an error), same
/// observable effect as a genuinely unreachable peer.
///
/// Returns the address callers should dial to reach `to` via this link.
pub async fn spawn_faulty_link(
    from: NodeId,
    to: NodeId,
    backend_addr: String,
    links: Arc<PartitionControl>,
) -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?.to_string();

    tokio::spawn(async move {
        loop {
            let (inbound, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => return, // listener closed, e.g. proxy torn down
            };
            let backend_addr = backend_addr.clone();
            let links = links.clone();
            tokio::spawn(async move {
                if links.is_blocked(from, to).await {
                    drop(inbound);
                    return;
                }
                let outbound = match TcpStream::connect(&backend_addr).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let mut inbound = inbound;
                let mut outbound = outbound;
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });

    Ok(listen_addr)
}

/// Build a full mesh of directed-link proxies for `ids`, given each
/// node's real backend address, and return, per node, the dial-override
/// map (`target id -> proxy address`) that node should be launched with.
pub async fn build_link_mesh(
    ids: &[NodeId],
    real_addrs: &std::collections::HashMap<NodeId, String>,
    links: Arc<PartitionControl>,
) -> anyhow::Result<std::collections::HashMap<NodeId, std::collections::HashMap<NodeId, String>>> {
    let mut overrides: std::collections::HashMap<
        NodeId,
        std::collections::HashMap<NodeId, String>,
    > = ids
        .iter()
        .map(|&id| (id, std::collections::HashMap::new()))
        .collect();

    for &from in ids {
        for &to in ids {
            if from == to {
                continue;
            }
            let backend = real_addrs[&to].clone();
            let proxy_addr = spawn_faulty_link(from, to, backend, links.clone()).await?;
            overrides.get_mut(&from).unwrap().insert(to, proxy_addr);
        }
    }

    Ok(overrides)
}
