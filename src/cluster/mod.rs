/// Phase 6 — slot-based cluster sharding.
///
/// Design
/// ──────
/// • 16 384 hash slots (identical to Redis Cluster).
/// • Slot for a key = CRC16(hash_tag(key)) % 16384.
/// • Hash tags: if a key contains `{tag}`, only `tag` is hashed, letting
///   callers co-locate related keys on the same node.
/// • Nodes are sorted alphabetically; slots are distributed evenly so every
///   node in the cluster derives the same routing table without coordination.
/// • When a command targets a slot owned by a different node the server
///   transparently proxies the request and returns the response, so ordinary
///   clients (not cluster-aware) work without MOVED handling.
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::protocol::{parse_frame, serialize_frame, Frame};

pub const NUM_SLOTS: u16 = 16384;

// ── CRC16 (CCITT, polynomial 0x1021) ──────────────────────────────────────

pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Extract the hash tag from a key. If the key contains `{non-empty}`,
/// only that portion is hashed — otherwise the whole key is used.
fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(close) = key[open + 1..].iter().position(|&b| b == b'}') {
            let tag = &key[open + 1..open + 1 + close];
            if !tag.is_empty() {
                return tag;
            }
        }
    }
    key
}

pub fn slot_for_key(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % NUM_SLOTS
}

// ── Cluster configuration ──────────────────────────────────────────────────

#[derive(Clone)]
pub struct ClusterConfig {
    /// All cluster nodes sorted alphabetically (used for deterministic slot assignment).
    pub nodes: Vec<String>,
    /// Index of this node in `nodes`.
    pub my_index: usize,
    /// slot_to_node[slot] = node index.
    slot_to_node: Vec<usize>,
}

impl ClusterConfig {
    /// Build a `ClusterConfig` from a list of node addresses and this node's
    /// own address. Slots are divided evenly across nodes in sorted order.
    pub fn new(mut nodes: Vec<String>, my_addr: &str) -> anyhow::Result<Self> {
        nodes.sort();
        nodes.dedup();

        let n = nodes.len();
        anyhow::ensure!(n > 0, "cluster must have at least one node");

        let my_index = nodes
            .iter()
            .position(|a| a == my_addr)
            .ok_or_else(|| anyhow::anyhow!("--cluster-self {my_addr} not found in --cluster-nodes"))?;

        // Distribute slots evenly; the last node absorbs any remainder.
        let base = NUM_SLOTS as usize / n;
        let mut slot_to_node = vec![0usize; NUM_SLOTS as usize];
        for (node_idx, node_slots) in slot_to_node.chunks_mut(base).enumerate() {
            for s in node_slots.iter_mut() {
                *s = node_idx.min(n - 1);
            }
        }
        // Assign remaining slots to the last node.
        let assigned = base * n;
        for s in slot_to_node[assigned..].iter_mut() {
            *s = n - 1;
        }

        Ok(ClusterConfig { nodes, my_index, slot_to_node })
    }

    pub fn my_addr(&self) -> &str {
        &self.nodes[self.my_index]
    }

    pub fn is_mine(&self, slot: u16) -> bool {
        self.slot_to_node[slot as usize] == self.my_index
    }

    pub fn node_for_slot(&self, slot: u16) -> &str {
        &self.nodes[self.slot_to_node[slot as usize]]
    }

    /// Contiguous slot ranges owned by `node_index` (for CLUSTER SLOTS).
    pub fn slot_ranges_for(&self, node_index: usize) -> Vec<(u16, u16)> {
        let mut ranges = Vec::new();
        let mut start: Option<u16> = None;
        for (slot, &owner) in self.slot_to_node.iter().enumerate() {
            let slot = slot as u16;
            if owner == node_index {
                if start.is_none() {
                    start = Some(slot);
                }
            } else if let Some(s) = start.take() {
                ranges.push((s, slot - 1));
            }
        }
        if let Some(s) = start {
            ranges.push((s, NUM_SLOTS - 1));
        }
        ranges
    }

    // ── CLUSTER command responses ─────────────────────────────────────────

    /// `CLUSTER INFO` bulk-string payload.
    pub fn cluster_info(&self) -> String {
        let ok = "ok";
        format!(
            "cluster_enabled:1\r\n\
             cluster_state:{ok}\r\n\
             cluster_slots_assigned:{NUM_SLOTS}\r\n\
             cluster_slots_ok:{NUM_SLOTS}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:1\r\n\
             cluster_my_epoch:1\r\n",
            self.nodes.len(),
            self.nodes.len(),
        )
    }

    /// `CLUSTER NODES` text payload (one line per node).
    pub fn cluster_nodes_text(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let flags = if i == self.my_index { "myself,master" } else { "master" };
                let ranges = self.slot_ranges_for(i);
                let slots_str = ranges
                    .iter()
                    .map(|(s, e)| format!("{s}-{e}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let id = node_id(addr);
                format!("{id} {addr}@0 {flags} - 0 0 {i} connected {slots_str}")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `CLUSTER SLOTS` nested-array response.
    pub fn cluster_slots_frame(&self) -> Frame {
        let ranges: Vec<Frame> = self
            .nodes
            .iter()
            .enumerate()
            .flat_map(|(i, addr)| {
                self.slot_ranges_for(i).into_iter().map(move |(start, end)| {
                    let (host, port) = split_addr(addr);
                    Frame::array(vec![
                        Frame::integer(start as i64),
                        Frame::integer(end as i64),
                        Frame::array(vec![
                            Frame::bulk_str(host),
                            Frame::integer(port as i64),
                            Frame::bulk_str(node_id(addr)),
                        ]),
                    ])
                })
            })
            .collect();
        Frame::array(ranges)
    }

    /// `CLUSTER SHARDS` response (Redis 7+ format used by newer redis-cli).
    pub fn cluster_shards_frame(&self) -> Frame {
        let shards: Vec<Frame> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let ranges = self.slot_ranges_for(i);
                let slot_pairs: Vec<Frame> = ranges
                    .into_iter()
                    .flat_map(|(s, e)| vec![Frame::integer(s as i64), Frame::integer(e as i64)])
                    .collect();
                let (host, port) = split_addr(addr);
                Frame::array(vec![
                    Frame::bulk_str("slots"),
                    Frame::array(slot_pairs),
                    Frame::bulk_str("nodes"),
                    Frame::array(vec![Frame::array(vec![
                        Frame::bulk_str("id"),
                        Frame::bulk_str(node_id(addr)),
                        Frame::bulk_str("endpoint"),
                        Frame::bulk_str(host.clone()),
                        Frame::bulk_str("ip"),
                        Frame::bulk_str(host),
                        Frame::bulk_str("port"),
                        Frame::integer(port as i64),
                        Frame::bulk_str("role"),
                        Frame::bulk_str("master"),
                    ])]),
                ])
            })
            .collect();
        Frame::array(shards)
    }
}

/// Public wrapper so the commands module can call it.
pub fn node_id_of(addr: &str) -> String {
    node_id(addr)
}

fn node_id(addr: &str) -> String {
    // Use a deterministic 40-char hex string derived from the address.
    format!("{:0>40}", crc16(addr.as_bytes()))
}

fn split_addr(addr: &str) -> (String, u16) {
    let mut parts = addr.rsplitn(2, ':');
    let port: u16 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(6379);
    let host = parts.next().unwrap_or("127.0.0.1").to_string();
    (host, port)
}

// ── Routing helpers ────────────────────────────────────────────────────────

/// The slot that a command operates on, or `None` for keyless commands.
pub enum CommandSlot {
    /// Single slot — all keys map to it.
    Slot(u16),
    /// Multi-key command where keys span multiple slots.
    CrossSlot,
    /// No key involved (PING, DBSIZE, etc.) — execute locally.
    None,
}

pub fn command_slot(cmd: &str, args: &[Frame]) -> CommandSlot {
    match cmd {
        // Single-key commands (key at position 1).
        "GET" | "SET" | "SETNX" | "GETSET" | "APPEND" | "STRLEN"
        | "INCR" | "INCRBY" | "DECR" | "DECRBY"
        | "TYPE" | "EXPIRE" | "PEXPIRE" | "TTL" | "PTTL" | "PERSIST"
        | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX"
        | "HSET" | "HMSET" | "HGET" | "HMGET" | "HGETALL" | "HDEL"
        | "HLEN" | "HEXISTS" | "HKEYS" | "HVALS"
        | "SADD" | "SMEMBERS" | "SISMEMBER" | "SREM" | "SCARD" => {
            single_key_slot(args, 1)
        }

        // Multi-key commands — all must land in the same slot.
        "DEL" | "UNLINK" | "EXISTS" | "MGET" => all_same_slot(args, 1, 1),
        "MSET" | "MSETNX" => all_same_slot(args, 1, 2), // stride 2 (key-value pairs)

        // No routing required.
        _ => CommandSlot::None,
    }
}

fn single_key_slot(args: &[Frame], idx: usize) -> CommandSlot {
    match args.get(idx) {
        Some(Frame::Bulk(Some(k))) => CommandSlot::Slot(slot_for_key(k)),
        _ => CommandSlot::None,
    }
}

/// Check that all keys in `args` starting at `start` with `stride` step hash
/// to the same slot.
fn all_same_slot(args: &[Frame], start: usize, stride: usize) -> CommandSlot {
    let mut slot: Option<u16> = None;
    let mut i = start;
    while i < args.len() {
        if let Some(Frame::Bulk(Some(k))) = args.get(i) {
            let s = slot_for_key(k);
            match slot {
                None => slot = Some(s),
                Some(prev) if prev != s => return CommandSlot::CrossSlot,
                _ => {}
            }
        }
        i += stride;
    }
    match slot {
        Some(s) => CommandSlot::Slot(s),
        None => CommandSlot::None,
    }
}

// ── Transparent proxy ──────────────────────────────────────────────────────

/// Forward `frame` to another cluster node and return its response.
/// A new TCP connection is opened per call (Phase 11 will add pooling).
pub async fn proxy_to(node_addr: &str, frame: Frame) -> Frame {
    debug!("proxying command to {node_addr}");
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        let mut conn = TcpStream::connect(node_addr).await?;
        conn.write_all(&serialize_frame(&frame)).await?;
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            conn.read_buf(&mut buf).await?;
            if let Ok(Some((resp, _))) = parse_frame(&buf) {
                return Ok::<Frame, anyhow::Error>(resp);
            }
        }
    })
    .await;

    match result {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => Frame::error(format!("ERR cluster proxy failed: {e}")),
        Err(_) => Frame::error(format!("ERR cluster proxy timeout to {node_addr}")),
    }
}

// ── Public convenience type ────────────────────────────────────────────────

pub type SharedCluster = Arc<ClusterConfig>;

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_known_values() {
        // Redis test vectors
        assert_eq!(slot_for_key(b"foo"), 12182);
        assert_eq!(slot_for_key(b"bar"), 5061);
        assert_eq!(slot_for_key(b"hello"), 866);
    }

    #[test]
    fn test_hash_tag() {
        // Keys with same tag land on the same slot.
        assert_eq!(slot_for_key(b"{user}.name"), slot_for_key(b"{user}.email"));
        // Empty tag — whole key is hashed.
        assert_ne!(slot_for_key(b"{}key"), slot_for_key(b"{}other"));
    }

    #[test]
    fn test_slot_distribution() {
        let nodes = vec![
            "127.0.0.1:6379".to_string(),
            "127.0.0.1:6380".to_string(),
            "127.0.0.1:6381".to_string(),
        ];
        let cfg = ClusterConfig::new(nodes, "127.0.0.1:6379").unwrap();
        // Every slot is assigned.
        assert_eq!(cfg.slot_to_node.len(), 16384);
        // Ranges are contiguous and non-overlapping.
        let ranges0 = cfg.slot_ranges_for(0);
        let ranges1 = cfg.slot_ranges_for(1);
        let ranges2 = cfg.slot_ranges_for(2);
        assert!(!ranges0.is_empty());
        assert!(!ranges1.is_empty());
        assert!(!ranges2.is_empty());
        // Total slots covered = 16384.
        let total: u16 = [&ranges0, &ranges1, &ranges2]
            .iter()
            .flat_map(|r| r.iter())
            .map(|(s, e)| e - s + 1)
            .sum();
        assert_eq!(total, 16384);
    }

    #[test]
    fn test_cluster_config_self_not_in_nodes() {
        let nodes = vec!["127.0.0.1:6379".to_string()];
        assert!(ClusterConfig::new(nodes, "127.0.0.1:9999").is_err());
    }
}
