//! A tiny out-of-band control protocol so the test harness can drive a
//! `test_node` process (propose a write, read a value, poll metrics)
//! without touching the Raft RPC port at all -- same framing discipline
//! as `raft::network` (4-byte little-endian length prefix + bincode),
//! but this is test-only, never something a real deployment exposes.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRequest {
    Propose { key: String, value: String },
    Get { key: String },
    Metrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub current_leader: Option<raft::NodeId>,
    pub current_term: u64,
    pub last_applied_index: Option<u64>,
    pub state: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminResponse {
    Proposed { index: u64 },
    ProposeFailed { message: String },
    Value(Option<String>),
    Metrics(MetricsSnapshot),
}

/// Connect to `addr`'s admin port, send one request, read the one
/// response, and close -- matches the per-call connection model
/// `raft::network` uses for Raft RPCs. Callers are expected to retry on
/// `io::Error` themselves (e.g. while a node is still starting up, or
/// mid-restart).
pub async fn call(addr: &str, req: &AdminRequest) -> std::io::Result<AdminResponse> {
    let mut stream = TcpStream::connect(addr).await?;
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

pub fn to_io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
}
