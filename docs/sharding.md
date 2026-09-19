# Sharding

## Slot model

- `SLOT_COUNT = 16384` (matches Redis Cluster).
- `slot(key) = CRC16_CCITT(hash_tag(key)) % 16384`, where `CRC16_CCITT` uses
  polynomial `0x1021`, initial value `0` — the same parameters the prior
  prototype already verified against Redis test vectors
  (`slot_for_key(b"foo") == 12182`, etc.). This code is reused as-is from
  `src/cluster/mod.rs`'s CRC16 implementation; it is correct and already
  tested.

## Hash tags

`hash_tag(key)`: if `key` contains a substring between the first `{` and
the first `}` after it, and that substring is non-empty, hash only that
substring. Otherwise hash the whole key. This is the standard Redis Cluster
rule and guarantees:

```
{user123}:profile   -> slot(user123)
{user123}:sessions  -> slot(user123)
```

so both can be placed in the same shard and participate in one shard-local
multi-key command (see [[consistency]] multi-key section).

Property tests required (see [[testing]]): hashing is deterministic
(same key always maps to same slot, across process restarts and across
nodes); hash-tag extraction matches Redis's documented algorithm
byte-for-byte, including the edge cases of an empty `{}` (falls back to
whole-key hash) and multiple `{...}` groups (only the first is used).

## Slot ownership

Slot ownership is a single table: `slot -> ShardId`, replicated by the
metadata Raft group (see [[membership]]) as part of `ClusterState`. It is
never derived locally by a node from gossip or heartbeats. Every node's
router consults its local cached copy of this table (kept current by
applying metadata-group commits) to decide:

- if the local node's shard owns the slot: handle directly.
- if the slot is owned by another shard and not migrating: reply
  `-MOVED <slot> <host>:<port>` with the current owner's leader address
  (see [[protocol]]). No transparent server-side proxying — the prior
  prototype's `proxy_to` behavior is removed; proxying hides topology from
  the client and defeats the point of client-side routing caches.
  See `docs/decisions/0003-moved-ask-not-proxy.md`.
  - Exception: `test-utils` / `client` crate MAY implement transparent
    retry-on-MOVED entirely on the client side, but the server never does
    it for the client.
- if the slot is actively migrating and this node is the source or target:
  follow [[resharding]]'s per-state rules, which may mean replying
  `-ASK <slot> <host>:<port>`.

## Shard = independent Raft group

Each shard is a Raft group with `replication_factor` members (default 3).
Shards do not share Raft state, terms, or logs with each other — see
[[raft]] for why (independent leadership, parallel writes, localized
failure). The metadata group is *also* a Raft group but a distinct one: it
does not hold key/value data and is not "one global group for the whole
database" (see [[architecture]]'s control/data-plane split and
`docs/decisions/0002-metadata-raft-group.md`).

## Replica placement

The metadata group's `ClusterState` records, per shard, which physical
nodes host its `replication_factor` replicas. Initial placement and
rebalancing are decided by the control plane and executed by:

1. metadata group commits an updated placement (e.g., "shard 3 gains a
   replica on node D").
2. node D is told (via its own poll of `ClusterState` or a direct RPC) to
   join shard 3's Raft group as a new, empty member.
3. shard 3's Raft group replicates a normal Raft membership-change entry
   adding node D as a learner, then promotes it to a full voting member
   once it has caught up (standard Raft joint-consensus / single-server
   membership-change safety rule — see [[raft]]).

Placement decisions never bypass step 3; the control plane can *propose*
where replicas go, but a replica only becomes a voting member of a shard's
Raft group through that shard's own Raft membership-change process, never
by the control plane unilaterally editing the shard's roster.

## Multi-key operations (recap, see [[consistency]] for the full contract)

Supported only within one slot (via hash tags). The router rejects (with a
client error, not silent partial execution) any multi-key command whose
keys don't all map to the same slot.
