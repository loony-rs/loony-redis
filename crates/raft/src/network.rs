//! `RaftNetworkFactory`/`RaftNetwork` over raw TCP (docs/raft.md: RPCs
//! travel over internal cluster-addr connections, framed separately from
//! the client-facing RESP port). Wire format: a 4-byte little-endian
//! length prefix followed by a bincode-encoded [`RpcRequest`]/
//! [`RpcResponse`].
//!
//! Generic over any `RaftTypeConfig` fixing `NodeId`/`Node` to this
//! crate's aliases -- every group in this project (a shard's data group
//! or the metadata group, Phase 7) uses the same transport, differing
//! only in what command type (`D`) travels inside `AppendEntriesRequest`.
//!
//! Also provides [`PartitionControl`], a shared, explicitly-gated table
//! of blocked (from, to) links. It's the mechanism the Phase 4 partition
//! tests use to simulate a network partition without a real proxy layer
//! (that's Phase 8's fault-injection harness); a connection attempt whose
//! (from, to) pair is blocked fails immediately as `Unreachable`, exactly
//! as if the peer really were unreachable. Production use simply never
//! blocks anything. Not generic itself -- every group's `NodeId` is this
//! crate's plain `u64` alias, so one table's (from, to) pairs are
//! meaningful regardless of which group's `Network` consults it.

use std::collections::HashSet;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::RaftTypeConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::{Node, NodeId, RaftError};

// `serde(bound = "")` disables serde-derive's default (and here overly
// strict) auto-generated `where C: Serialize` bound -- `C` itself is
// never serialized, only `AppendEntriesRequest<C>`/`InstallSnapshotRequest<C>`,
// whose own impls carry whatever bounds they actually need.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
enum RpcRequest<C: RaftTypeConfig> {
    AppendEntries(AppendEntriesRequest<C>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<C>),
}

// Note: unlike `RpcRequest`, this doesn't need to be generic over `C` --
// `AppendEntriesResponse`/`VoteResponse`/`InstallSnapshotResponse` are all
// typed by `NodeId` alone, independent of the group's command type.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum RpcResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError>),
    Vote(Result<VoteResponse<NodeId>, RaftError>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, RaftError<InstallSnapshotError>>),
}

/// Shared table of deliberately-blocked links, keyed by (from, to). See
/// the module doc comment.
#[derive(Default)]
pub struct PartitionControl {
    blocked: RwLock<HashSet<(NodeId, NodeId)>>,
}

impl PartitionControl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Cut the link between `a` and `b` in both directions.
    pub async fn partition(&self, a: NodeId, b: NodeId) {
        let mut blocked = self.blocked.write().await;
        blocked.insert((a, b));
        blocked.insert((b, a));
    }

    /// Restore the link between `a` and `b` in both directions.
    pub async fn heal(&self, a: NodeId, b: NodeId) {
        let mut blocked = self.blocked.write().await;
        blocked.remove(&(a, b));
        blocked.remove(&(b, a));
    }

    /// Whether the link `from -> to` is currently cut. `pub` so a
    /// separately-hosted fault-injection proxy (Phase 8's
    /// `crates/test-utils`) can consult the exact same partition state a
    /// test uses to drive `partition`/`heal`, rather than reimplementing
    /// an equivalent table.
    pub async fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.read().await.contains(&(from, to))
    }
}

pub struct Network<C> {
    my_id: NodeId,
    links: Arc<PartitionControl>,
    /// Per-target dial address overrides, consulted instead of the
    /// address `node.addr` openraft's (cluster-wide, replicated)
    /// membership carries for that target.
    ///
    /// This exists because `node.addr` is necessarily the same for every
    /// node's view of a given peer -- it's committed Raft membership
    /// data. A deployment that wants a *particular local node* to reach
    /// a peer via a different path (a specific egress route, a NAT
    /// traversal address, ...) can't express that through the
    /// replicated address alone. `crates/test-utils`'s fault-injection
    /// proxy (Phase 8) uses exactly this: each node process is given its
    /// own override table pointing every peer at a dedicated per-link
    /// proxy address, so the proxy for link (i, j) only ever serves
    /// calls from i and needs no caller identification to apply
    /// directional partition rules.
    overrides: Arc<std::collections::HashMap<NodeId, String>>,
    _config: PhantomData<C>,
}

impl<C> Clone for Network<C> {
    fn clone(&self) -> Self {
        Network {
            my_id: self.my_id,
            links: self.links.clone(),
            overrides: self.overrides.clone(),
            _config: PhantomData,
        }
    }
}

impl<C> Network<C> {
    pub fn new(my_id: NodeId, links: Arc<PartitionControl>) -> Self {
        Network {
            my_id,
            links,
            overrides: Arc::new(std::collections::HashMap::new()),
            _config: PhantomData,
        }
    }

    /// Like `new`, but every RPC to `target` dials `overrides[target]`
    /// instead of whatever address is in openraft's replicated
    /// membership for that target, when an entry is present. See the
    /// field doc comment on `Network::overrides`.
    pub fn with_overrides(
        my_id: NodeId,
        links: Arc<PartitionControl>,
        overrides: std::collections::HashMap<NodeId, String>,
    ) -> Self {
        Network {
            my_id,
            links,
            overrides: Arc::new(overrides),
            _config: PhantomData,
        }
    }
}

impl<C> RaftNetworkFactory<C> for Network<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    type Network = Connection<C>;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = self
            .overrides
            .get(&target)
            .cloned()
            .unwrap_or_else(|| node.addr.clone());
        Connection {
            from: self.my_id,
            target,
            addr,
            links: self.links.clone(),
            _config: PhantomData,
        }
    }
}

pub struct Connection<C> {
    from: NodeId,
    target: NodeId,
    addr: String,
    links: Arc<PartitionControl>,
    _config: PhantomData<C>,
}

impl<C> Connection<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    async fn call(&self, req: &RpcRequest<C>) -> io::Result<RpcResponse> {
        if self.links.is_blocked(self.from, self.target).await {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "link partitioned",
            ));
        }

        let mut stream = TcpStream::connect(&self.addr).await?;
        let payload = bincode::serialize(req).map_err(to_io_err)?;
        stream
            .write_all(&(payload.len() as u32).to_le_bytes())
            .await?;
        stream.write_all(&payload).await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        bincode::deserialize(&buf).map_err(to_io_err)
    }

    /// `io::Error` -> `Unreachable` for connection-level failures (so
    /// openraft backs off instead of retrying immediately), `Network`
    /// for anything else (framing/serialization failures on an
    /// otherwise-live connection).
    fn to_rpc_error<E: std::error::Error>(
        &self,
        e: io::Error,
    ) -> openraft::error::RPCError<NodeId, Node, RaftError<E>> {
        match e.kind() {
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotConnected
            | io::ErrorKind::TimedOut
            | io::ErrorKind::HostUnreachable => {
                openraft::error::RPCError::Unreachable(Unreachable::new(&e))
            }
            _ => openraft::error::RPCError::Network(NetworkError::new(&e)),
        }
    }

    fn unexpected_response<E: std::error::Error>(
        &self,
    ) -> openraft::error::RPCError<NodeId, Node, RaftError<E>> {
        let e = io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected RPC response variant",
        );
        openraft::error::RPCError::Network(NetworkError::new(&e))
    }
}

impl<C> RaftNetwork<C> for Connection<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, openraft::error::RPCError<NodeId, Node, RaftError>>
    {
        let resp = self
            .call(&RpcRequest::AppendEntries(rpc))
            .await
            .map_err(|e| self.to_rpc_error(e))?;
        match resp {
            RpcResponse::AppendEntries(r) => r.map_err(|e| {
                openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e))
            }),
            _ => Err(self.unexpected_response()),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<NodeId, Node, RaftError<InstallSnapshotError>>,
    > {
        let resp = self
            .call(&RpcRequest::InstallSnapshot(rpc))
            .await
            .map_err(|e| self.to_rpc_error(e))?;
        match resp {
            RpcResponse::InstallSnapshot(r) => r.map_err(|e| {
                openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e))
            }),
            _ => Err(self.unexpected_response()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, openraft::error::RPCError<NodeId, Node, RaftError>> {
        let resp = self
            .call(&RpcRequest::Vote(rpc))
            .await
            .map_err(|e| self.to_rpc_error(e))?;
        match resp {
            RpcResponse::Vote(r) => r.map_err(|e| {
                openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e))
            }),
            _ => Err(self.unexpected_response()),
        }
    }
}

fn to_io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Bind `addr` and serve (see [`serve`]). Split out so callers that don't
/// need to know the bound port up front can do it in one call.
pub async fn serve_addr<C>(addr: &str, raft: openraft::Raft<C>) -> anyhow::Result<()>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    let listener = TcpListener::bind(addr).await?;
    serve(listener, raft).await
}

/// Server side: accept RPCs on an already-bound `listener` and dispatch
/// them to `raft`. Runs until the listener errors (e.g. the socket is
/// closed) -- callers spawn this as its own task per node. Taking an
/// already-bound listener (rather than an address to bind) lets a caller
/// bind an ephemeral port (`:0`), learn the real address via
/// `listener.local_addr()`, and only then start serving -- no bind/rebind
/// race, which matters for tests that spin up many nodes on ephemeral
/// ports.
pub async fn serve<C>(listener: TcpListener, raft: openraft::Raft<C>) -> anyhow::Result<()>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    loop {
        let (socket, _peer) = listener.accept().await?;
        let raft = raft.clone();
        tokio::spawn(async move {
            let _ = handle_conn(socket, raft).await;
        });
    }
}

async fn handle_conn<C>(mut socket: TcpStream, raft: openraft::Raft<C>) -> io::Result<()>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node, Entry = openraft::Entry<C>>,
{
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    socket.read_exact(&mut buf).await?;
    let req: RpcRequest<C> = bincode::deserialize(&buf).map_err(to_io_err)?;

    let resp = match req {
        RpcRequest::AppendEntries(r) => RpcResponse::AppendEntries(raft.append_entries(r).await),
        RpcRequest::Vote(r) => RpcResponse::Vote(raft.vote(r).await),
        RpcRequest::InstallSnapshot(r) => {
            RpcResponse::InstallSnapshot(raft.install_snapshot(r).await)
        }
    };

    let payload = bincode::serialize(&resp).map_err(to_io_err)?;
    socket
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    socket.write_all(&payload).await?;
    Ok(())
}
