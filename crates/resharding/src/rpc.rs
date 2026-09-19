//! The migration data-transfer RPC (docs/resharding.md): lets a
//! migration's target fetch a snapshot of a source shard's keys within
//! a slot range. Any replica of the source shard may serve this -- a
//! reply from a lagging follower is safe to use (it just means the
//! coordinator's diff-and-sync loop takes another pass to fully
//! converge, never a correctness problem), so unlike Raft/admin RPCs
//! there is no notion of "must be the leader" here.
//!
//! Framing matches `raft::network`/`test_utils::admin`: a 4-byte
//! little-endian length prefix followed by bincode.

use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RpcRequest {
    GetRangeSnapshot { start: u16, end: u16 },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RpcResponse {
    Entries(Vec<storage::KeyEntry>),
}

fn to_io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn in_range(key: &str, start: u16, end: u16) -> bool {
    let slot = cluster::slot_for_key(key.as_bytes());
    slot >= start && slot <= end
}

/// Accept connections on `listener` and answer `GetRangeSnapshot`
/// requests by filtering `sm`'s current key/value data to the requested
/// slot range. Runs until the listener errors (closed).
pub async fn serve(listener: TcpListener, sm: Arc<raft::StateMachineStore>) -> anyhow::Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await?;
        let sm = sm.clone();
        tokio::spawn(async move {
            let _ = handle_conn(socket, sm).await;
        });
    }
}

async fn handle_conn(mut socket: TcpStream, sm: Arc<raft::StateMachineStore>) -> io::Result<()> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    socket.read_exact(&mut buf).await?;
    let req: RpcRequest = bincode::deserialize(&buf).map_err(to_io_err)?;

    let resp = match req {
        RpcRequest::GetRangeSnapshot { start, end } => {
            let entries = sm
                .store
                .snapshot_entries()
                .into_iter()
                .filter(|e| in_range(&e.key, start, end))
                .collect();
            RpcResponse::Entries(entries)
        }
    };

    let payload = bincode::serialize(&resp).map_err(to_io_err)?;
    socket
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    socket.write_all(&payload).await?;
    Ok(())
}

/// Fetch the slot range `[start, end]`'s current entries from `addr`.
pub async fn fetch_range_snapshot(
    addr: &str,
    start: u16,
    end: u16,
) -> io::Result<Vec<storage::KeyEntry>> {
    let mut stream = TcpStream::connect(addr).await?;
    let payload =
        bincode::serialize(&RpcRequest::GetRangeSnapshot { start, end }).map_err(to_io_err)?;
    stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(&payload).await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    match bincode::deserialize(&buf).map_err(to_io_err)? {
        RpcResponse::Entries(entries) => Ok(entries),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use storage::Value;

    async fn state_machine_with(entries: &[(&str, &str)]) -> Arc<raft::StateMachineStore> {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(raft::StateMachineStore::open(dir.path()).await.unwrap());
        for (k, v) in entries {
            sm.store.set(
                (*k).to_string(),
                Value::String(Bytes::from(v.to_string())),
                None,
            );
        }
        sm
    }

    #[tokio::test]
    async fn test_fetch_range_snapshot_filters_by_slot() {
        // Pick two keys guaranteed to land in different slots.
        let (key_low, key_high) = {
            let mut a = None;
            let mut b = None;
            for i in 0u64.. {
                let k = format!("k{i}");
                let slot = cluster::slot_for_key(k.as_bytes());
                if slot < cluster::SLOT_COUNT / 2 && a.is_none() {
                    a = Some(k);
                } else if slot >= cluster::SLOT_COUNT / 2 && b.is_none() {
                    b = Some(k);
                }
                if a.is_some() && b.is_some() {
                    break;
                }
            }
            (a.unwrap(), b.unwrap())
        };

        let sm = state_machine_with(&[(&key_low, "low-value"), (&key_high, "high-value")]).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, sm));

        let entries = fetch_range_snapshot(&addr, 0, cluster::SLOT_COUNT / 2 - 1)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, key_low);
        assert!(matches!(&entries[0].value, Value::String(b) if b == "low-value"));
    }

    #[tokio::test]
    async fn test_fetch_range_snapshot_empty_when_nothing_matches() {
        let sm = state_machine_with(&[]).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, sm));

        let entries = fetch_range_snapshot(&addr, 0, cluster::SLOT_COUNT - 1)
            .await
            .unwrap();
        assert!(entries.is_empty());
    }
}
