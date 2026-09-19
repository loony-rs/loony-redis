//! Routing decisions (docs/sharding.md, docs/protocol.md). This is the
//! "Cluster Router" layer in docs/architecture.md: given a parsed
//! command, decide whether this shard should handle it locally, another
//! shard owns it (`-MOVED`), or it spans multiple slots (`CROSSSLOT`).
//!
//! No server-side proxying (decision 0003): the caller gets a decision,
//! never a fetched-on-your-behalf answer.
//!
//! `SlotTable` here is a simple, directly-constructed slot -> owner map.
//! It is deliberately not yet the consensus-backed `ClusterState` from
//! docs/membership.md -- that lands in Phase 7. For now (a single real
//! shard, per PLAN.md Phase 5), an unset slot defaults to "owned by
//! whichever shard is asking" rather than an error, since there is no
//! other authoritative source yet; only an explicit entry pointing
//! elsewhere produces a `MOVED`.

use protocol::Frame;

pub use crate::slots::slot_for_key;

pub type ShardId = u64;

/// Which slot(s), if any, a command's keys map to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandSlot {
    /// Single slot -- all keys (there may be several, e.g. `DEL`) map to it.
    Slot(u16),
    /// Multi-key command whose keys span more than one slot.
    CrossSlot,
    /// No key involved (PING, INFO, DBSIZE, ...) -- always local.
    None,
}

/// Determine the slot(s) touched by `cmd`/`args`, ported from the
/// prototype's `command_slot` (already correct -- see docs/architecture.md's
/// reuse-vs-rebuild note). `args` is the full command array including the
/// command name at index 0.
pub fn command_slot(cmd: &str, args: &[Frame]) -> CommandSlot {
    match cmd {
        // Single-key commands (key at position 1).
        "GET" | "SET" | "SETNX" | "GETSET" | "APPEND" | "STRLEN" | "INCR" | "INCRBY" | "DECR"
        | "DECRBY" | "TYPE" | "EXPIRE" | "PEXPIRE" | "TTL" | "PTTL" | "PERSIST" | "LPUSH"
        | "RPUSH" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX" | "LSET" | "LINSERT"
        | "LREM" | "LTRIM" | "HSET" | "HMSET" | "HGET" | "HMGET" | "HGETALL" | "HDEL" | "HLEN"
        | "HEXISTS" | "HKEYS" | "HVALS" | "HSETNX" | "HINCRBY" | "HINCRBYFLOAT" | "SADD"
        | "SMEMBERS" | "SISMEMBER" | "SREM" | "SCARD" | "SRANDMEMBER" | "SPOP" | "ZADD"
        | "ZSCORE" | "ZRANK" | "ZREVRANK" | "ZCARD" | "ZCOUNT" | "ZINCRBY" | "ZREM" | "ZRANGE"
        | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZPOPMIN" | "ZPOPMAX"
        | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" => single_key_slot(args, 1),

        // Multi-key commands -- every key must land in the same slot.
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

/// Slot ownership table: `slot -> (owning shard, that shard's leader
/// address)`. An unset slot has no explicit owner recorded.
///
/// `Serialize`/`Deserialize` so `membership`'s metadata group (Phase 7)
/// can include it wholesale in a `ClusterState` snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SlotTable {
    owners: Vec<Option<(ShardId, String)>>,
}

impl SlotTable {
    pub fn new() -> Self {
        SlotTable {
            owners: vec![None; crate::slots::SLOT_COUNT as usize],
        }
    }

    /// Record ownership for every slot in `range` (inclusive of both ends).
    pub fn set_owner(
        &mut self,
        range: std::ops::RangeInclusive<u16>,
        shard: ShardId,
        leader_addr: impl Into<String>,
    ) {
        let addr = leader_addr.into();
        for slot in range {
            self.owners[slot as usize] = Some((shard, addr.clone()));
        }
    }

    pub fn owner(&self, slot: u16) -> Option<&(ShardId, String)> {
        self.owners[slot as usize].as_ref()
    }
}

impl Default for SlotTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Handle locally.
    Local,
    /// Another shard owns this slot -- reply `-MOVED`.
    Moved { slot: u16, leader_addr: String },
    /// This slot is mid-`Cutover` and this node is the migration
    /// source -- reply `-ASK` (docs/resharding.md). The client must
    /// send `ASKING` then retry against `leader_addr` without updating
    /// its long-term routing cache.
    Ask { slot: u16, leader_addr: String },
    /// The command's keys span more than one slot -- reply `-CROSSSLOT`.
    CrossSlot,
}

/// Decide how `my_shard` should handle `cmd`/`args` given the current
/// `table` and any in-flight `migrations`. A slot with no recorded owner
/// defaults to `Local` -- see the module doc comment for why (no Phase 7
/// metadata group yet, for callers that don't have one).
///
/// Per docs/resharding.md, `Preparing`/`Transferring`/`CatchingUp` change
/// nothing about routing -- the source is still fully authoritative.
/// Only `Cutover` changes behavior, and only for the source: it replies
/// `-ASK` instead of serving the request itself.
pub fn route(
    table: &SlotTable,
    migrations: &[crate::SlotMigration],
    my_shard: ShardId,
    cmd: &str,
    args: &[Frame],
) -> RoutingDecision {
    match command_slot(cmd, args) {
        CommandSlot::None => RoutingDecision::Local,
        CommandSlot::CrossSlot => RoutingDecision::CrossSlot,
        CommandSlot::Slot(slot) => {
            if let Some(m) = crate::migration_for_slot(migrations, slot) {
                if m.state == crate::MigrationState::Cutover && m.source == my_shard {
                    return RoutingDecision::Ask {
                        slot,
                        leader_addr: m.target_leader_addr.clone(),
                    };
                }
            }
            match table.owner(slot) {
                None => RoutingDecision::Local,
                Some((shard, _)) if *shard == my_shard => RoutingDecision::Local,
                Some((_, addr)) => RoutingDecision::Moved {
                    slot,
                    leader_addr: addr.clone(),
                },
            }
        }
    }
}

pub fn moved_error(slot: u16, leader_addr: &str) -> Frame {
    Frame::error(format!("MOVED {slot} {leader_addr}"))
}

pub fn ask_error(slot: u16, leader_addr: &str) -> Frame {
    Frame::error(format!("ASK {slot} {leader_addr}"))
}

pub fn crossslot_error() -> Frame {
    Frame::error("CROSSSLOT Keys in request don't hash to the same slot")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::Bulk(Some(Bytes::copy_from_slice(p.as_bytes()))))
            .collect()
    }

    #[test]
    fn test_single_key_command_slot() {
        let a = args(&["GET", "foo"]);
        assert_eq!(
            command_slot("GET", &a),
            CommandSlot::Slot(slot_for_key(b"foo"))
        );
    }

    #[test]
    fn test_del_multi_key_same_slot_via_hash_tag() {
        let a = args(&["DEL", "{u}.a", "{u}.b"]);
        assert_eq!(
            command_slot("DEL", &a),
            CommandSlot::Slot(slot_for_key(b"{u}.a"))
        );
    }

    #[test]
    fn test_del_multi_key_cross_slot() {
        let a = args(&["DEL", "foo", "bar"]);
        assert_eq!(command_slot("DEL", &a), CommandSlot::CrossSlot);
    }

    #[test]
    fn test_ping_has_no_slot() {
        let a = args(&["PING"]);
        assert_eq!(command_slot("PING", &a), CommandSlot::None);
    }

    #[test]
    fn test_route_local_by_default_when_slot_unowned() {
        let table = SlotTable::new();
        let a = args(&["GET", "foo"]);
        assert_eq!(route(&table, &[], 1, "GET", &a), RoutingDecision::Local);
    }

    #[test]
    fn test_route_local_when_this_shard_owns_the_slot() {
        let mut table = SlotTable::new();
        let slot = slot_for_key(b"foo");
        table.set_owner(slot..=slot, 1, "127.0.0.1:7001");
        let a = args(&["GET", "foo"]);
        assert_eq!(route(&table, &[], 1, "GET", &a), RoutingDecision::Local);
    }

    #[test]
    fn test_route_moved_when_another_shard_owns_the_slot() {
        let mut table = SlotTable::new();
        let slot = slot_for_key(b"foo");
        table.set_owner(slot..=slot, 2, "127.0.0.1:7002");
        let a = args(&["GET", "foo"]);
        assert_eq!(
            route(&table, &[], 1, "GET", &a),
            RoutingDecision::Moved {
                slot,
                leader_addr: "127.0.0.1:7002".to_string()
            }
        );
    }

    #[test]
    fn test_route_crossslot_regardless_of_ownership() {
        let table = SlotTable::new();
        let a = args(&["DEL", "foo", "bar"]);
        assert_eq!(route(&table, &[], 1, "DEL", &a), RoutingDecision::CrossSlot);
    }

    #[test]
    fn test_route_ask_when_source_mid_cutover() {
        let mut table = SlotTable::new();
        let slot = slot_for_key(b"foo");
        table.set_owner(slot..=slot, 1, "127.0.0.1:7001");
        let migrations = vec![crate::SlotMigration {
            start: slot,
            end: slot,
            source: 1,
            target: 2,
            target_leader_addr: "127.0.0.1:7002".to_string(),
            state: crate::MigrationState::Cutover,
        }];
        let a = args(&["GET", "foo"]);
        assert_eq!(
            route(&table, &migrations, 1, "GET", &a),
            RoutingDecision::Ask {
                slot,
                leader_addr: "127.0.0.1:7002".to_string()
            }
        );
    }

    #[test]
    fn test_route_unaffected_by_migration_before_cutover() {
        // Preparing/Transferring/CatchingUp must not change routing --
        // the source stays fully authoritative until Cutover.
        let mut table = SlotTable::new();
        let slot = slot_for_key(b"foo");
        table.set_owner(slot..=slot, 1, "127.0.0.1:7001");
        for state in [
            crate::MigrationState::Preparing,
            crate::MigrationState::Transferring,
            crate::MigrationState::CatchingUp,
        ] {
            let migrations = vec![crate::SlotMigration {
                start: slot,
                end: slot,
                source: 1,
                target: 2,
                target_leader_addr: "127.0.0.1:7002".to_string(),
                state,
            }];
            let a = args(&["GET", "foo"]);
            assert_eq!(
                route(&table, &migrations, 1, "GET", &a),
                RoutingDecision::Local,
                "state {state:?} must not affect routing"
            );
        }
    }

    #[test]
    fn test_route_cutover_does_not_affect_the_target() {
        // The target isn't the source, so it must not itself start
        // answering ASK for a slot it doesn't yet own.
        let mut table = SlotTable::new();
        let slot = slot_for_key(b"foo");
        table.set_owner(slot..=slot, 1, "127.0.0.1:7001");
        let migrations = vec![crate::SlotMigration {
            start: slot,
            end: slot,
            source: 1,
            target: 2,
            target_leader_addr: "127.0.0.1:7002".to_string(),
            state: crate::MigrationState::Cutover,
        }];
        let a = args(&["GET", "foo"]);
        assert_eq!(
            route(&table, &migrations, 2, "GET", &a),
            RoutingDecision::Moved {
                slot,
                leader_addr: "127.0.0.1:7001".to_string()
            }
        );
    }

    #[test]
    fn test_ask_error_format() {
        let f = ask_error(42, "127.0.0.1:7002");
        match f {
            Frame::Error(s) => assert_eq!(s, "ASK 42 127.0.0.1:7002"),
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn test_moved_error_format() {
        let f = moved_error(42, "127.0.0.1:7002");
        match f {
            Frame::Error(s) => assert_eq!(s, "MOVED 42 127.0.0.1:7002"),
            _ => panic!("expected error frame"),
        }
    }
}
