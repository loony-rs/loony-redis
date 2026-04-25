use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

use crate::protocol::{serialize_frame, Frame};
use crate::storage::SnapshotEntry;

/// Maximum number of write-command bytes buffered for each lagging replica.
const BROADCAST_CAPACITY: usize = 4096;

pub struct Replication {
    /// Whether this node currently acts as the write leader.
    pub is_leader: bool,
    /// Address of the leader this node should replicate from (followers only).
    pub leader_addr: Option<String>,
    /// Broadcast channel: every committed write is sent here as serialised RESP.
    write_tx: broadcast::Sender<Bytes>,
    /// How many write commands have been committed on this node.
    pub commit_count: AtomicUsize,
}

impl Replication {
    pub fn new_leader() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Replication {
            is_leader: true,
            leader_addr: None,
            write_tx: tx,
            commit_count: AtomicUsize::new(0),
        }
    }

    pub fn new_follower(leader_addr: String) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Replication {
            is_leader: false,
            leader_addr: Some(leader_addr),
            write_tx: tx,
            commit_count: AtomicUsize::new(0),
        }
    }

    /// Publish a serialised write command to every connected replica.
    pub fn broadcast(&self, cmd: Bytes) {
        self.commit_count.fetch_add(1, Ordering::Relaxed);
        // Ignore the error: it just means there are no subscribers yet.
        let _ = self.write_tx.send(cmd);
    }

    /// Subscribe to the write stream. Call this *before* taking a snapshot
    /// so that writes arriving during the snapshot transfer are buffered here
    /// and sent to the replica immediately afterwards.
    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.write_tx.subscribe()
    }
}

// ── Snapshot serialisation ─────────────────────────────────────────────────

/// Convert a store snapshot entry into a RESP-serialised command that recreates it.
pub fn snapshot_entry_to_resp(entry: &SnapshotEntry) -> Bytes {
    let frame = match entry {
        SnapshotEntry::String { key, value } => Frame::array(vec![
            Frame::bulk_str("SET"),
            Frame::Bulk(Some(Bytes::from(key.clone().into_bytes()))),
            Frame::Bulk(Some(value.clone())),
        ]),

        SnapshotEntry::List { key, items } => {
            let mut args = vec![
                Frame::bulk_str("RPUSH"),
                Frame::Bulk(Some(Bytes::from(key.clone().into_bytes()))),
            ];
            args.extend(items.iter().map(|b| Frame::Bulk(Some(b.clone()))));
            Frame::array(args)
        }

        SnapshotEntry::Hash { key, fields } => {
            let mut args = vec![
                Frame::bulk_str("HSET"),
                Frame::Bulk(Some(Bytes::from(key.clone().into_bytes()))),
            ];
            for (f, v) in fields {
                args.push(Frame::Bulk(Some(f.clone())));
                args.push(Frame::Bulk(Some(v.clone())));
            }
            Frame::array(args)
        }

        SnapshotEntry::Set { key, members } => {
            let mut args = vec![
                Frame::bulk_str("SADD"),
                Frame::Bulk(Some(Bytes::from(key.clone().into_bytes()))),
            ];
            args.extend(members.iter().map(|b| Frame::Bulk(Some(b.clone()))));
            Frame::array(args)
        }
    };

    serialize_frame(&frame)
}
