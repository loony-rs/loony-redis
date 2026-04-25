# Architecture

loony-redis is built in eight incremental phases, each adding a layer of capability without breaking the previous one. This document explains the design decisions and trade-offs made at each layer.

---

## Module Map

```
src/
  main.rs          Startup sequencing: parse args → build state → run server
  network/         TCP I/O, connection lifecycle, replication streaming
  protocol/        RESP parser + serializer
  commands/        Command dispatch and all command implementations
  storage/         In-memory data store (thread-safe, TTL-aware)
  persistence/     Append-Only File (AOF)
  replication/     Async leader–follower replication
  consensus/       Raft consensus engine
  cluster/         Hash-slot sharding, routing, migration, health/gossip
```

---

## Phase 1 — Storage Engine

**File:** `src/storage/mod.rs`

The store is a `DashMap<String, Entry>` where `Entry` holds a `Value` (enum over String/List/Hash/Set) and an optional expiry `Instant`.

Key properties:
- **Lock-free reads**: `DashMap` uses shard-level locking, so concurrent reads on different keys never contend.
- **TTL is lazy**: expiry is checked on access, not via a background sweeper. This avoids timer overhead at the cost of stale memory until the next access.
- **PTTL = −1** means no expiry; **PTTL = −2** means already expired (key is deleted on read).

---

## Phase 2 — RESP Protocol and TCP Server

**Files:** `src/protocol/mod.rs`, `src/network/mod.rs`

The server accepts connections with `tokio::net::TcpListener`. Each connection runs in its own `tokio::spawn` task, sharing `Arc<Store>` and `Arc<Replication>`.

The RESP parser is zero-copy where possible — bulk strings are sliced from the `BytesMut` read buffer using `bytes::Bytes` (reference-counted, no memcpy).

---

## Phase 3 — Append-Only File

**File:** `src/persistence/mod.rs`

Every write command is serialised as a RESP array and appended to the AOF with `AsyncWriteExt`. On startup, the file is read back as a stream of RESP frames and replayed through the normal command dispatch path with `is_replica_replay: true` (skips READONLY checks and replication fan-out).

Trade-off: AOF provides durability at the cost of write amplification (every SET writes to disk before returning OK). RDB snapshots are not implemented — they would add startup speed but require snapshotting all data atomically.

---

## Phase 4 — Async Leader–Follower Replication

**File:** `src/replication/mod.rs`

The leader holds a `tokio::sync::broadcast::Sender<Bytes>`. Every successful write is serialised and sent on the channel. Each follower task holds a `Receiver` and forwards bytes to the follower's TCP connection.

On connect, a follower sends `SYNC`. The leader:
1. Subscribes to the broadcast channel *before* taking a snapshot (to avoid missing writes during transfer).
2. Sends each key as a RESP command (`SET`, `RPUSH`, etc.) via `snapshot_entry_to_resp`.
3. Forwards the live broadcast stream indefinitely.

Followers reject write commands with `READONLY`.

---

## Phase 5 — Raft Consensus

**File:** `src/consensus/mod.rs`

A minimal Raft implementation covering:
- **Leader election** via randomised election timeouts (150–300 ms).
- **Log replication** with majority quorum (`AppendEntries` RPCs).
- **Apply function**: a caller-supplied `ApplyFn` is invoked for every committed log entry. In `main.rs` this runs the committed frame through `commands::execute` with `is_replica_replay: true`.

When Raft is active, write commands on a non-leader return `MOVED <leader_addr>`. Writes on the leader are serialised into the Raft log; the client receives `OK` only after the entry is committed on a majority.

Raft and async replication are mutually exclusive: the follower loop is only started when `--raft-addr` is not set.

---

## Phase 6 — Hash-Slot Sharding

**File:** `src/cluster/mod.rs`

16 384 slots (identical to Redis Cluster). Slot assignment:

```
slot = CRC16(hash_tag(key)) % 16384
```

**Hash tags**: if a key contains `{tag}`, only the content inside `{}` is hashed, allowing multi-key operations to be co-located on one shard (e.g. `{user}.name` and `{user}.email` land on the same node).

**CRC16**: CCITT polynomial 0x1021, initial value 0. Produces the same values as Redis — `slot_for_key(b"foo") == 12182`.

**Deterministic routing**: nodes are sorted alphabetically and slots divided evenly. Every node independently computes the same routing table from the same sorted list, with no coordination. The last node absorbs any remainder slots.

**Transparent proxying**: when a command targets a slot not owned by the receiving node, it opens a TCP connection to the correct node, forwards the RESP frame, and returns the response. The client sees no `MOVED` error.

---

## Phase 7 — Dynamic Membership

**File:** `src/cluster/mod.rs` (MEET / FORGET / SYNC / DRAIN handlers)

### Adding a node (`CLUSTER MEET host port`)

The coordinator node (any existing node the operator sends MEET to):
1. Computes the new sorted node list.
2. Sends `CLUSTER SYNC <new-node-list>` to the new node so it can accept incoming keys.
3. Sends `CLUSTER SYNC` to all existing peers — each peer independently determines which of *its own* slots must migrate out and pushes them.
4. Performs its own migration (same `slots_to_send` computation).
5. Commits the new `ClusterConfig` locally.

### Removing a node (`CLUSTER FORGET node-id`)

Critical ordering constraint — violating it causes proxy loops:

1. **Update this node's routing** (can now accept keys for newly acquired slots).
2. **Send `CLUSTER SYNC`** to all surviving peers (they update routing too).
3. **Send `CLUSTER DRAIN`** to the departing node (it pushes all its keys to the new owners using the updated routing). Receivers already have the correct routing before the first key arrives.

### Migration helpers

- `drain_slots(slots, store)` — atomically removes all keys in `slots` from the local store and returns them as `MigrateEntry = (key, Value, pttl)`.
- `push_entries_to(addr, entries)` — opens one TCP connection and sends each entry as a RESP command (`SET`, `RPUSH`, `HSET`, `SADD`), preserving TTL with `PX`.
- `slots_to_send(old_cfg, new_cfg)` — returns a `HashMap<target_addr, Vec<slot>>` for this node only. Each node independently computes what *it* needs to send, so there are no conflicts.

---

## Phase 8 — Fault Detection and Recovery

**File:** `src/cluster/mod.rs` (HealthTable, run_heartbeat_task, auto_heal)

### HealthTable

```
NodeState: Alive → Suspected → Failed
```

Each peer has a `missed` counter and a `last_seen` timestamp. State transitions:
- **Miss**: `missed += 1`. At `missed >= 3` (1.5 s) → `Failed`.
- **Alive**: successful PING resets `missed = 0` and `state = Alive`.
- **Gossip**: state only *worsens* via gossip (no resurrection). A node returns to `Alive` only through a direct PING.

### Heartbeat task

Runs as a background `tokio::spawn` task per cluster node. Every 500 ms:
1. PINGs each non-failed peer (200 ms timeout).
2. On success: marks peer `Alive`, sends `CLUSTER GOSSIP <csv>` piggybacking this node's view.
3. On failure: increments miss counter; at threshold triggers `auto_heal`.

### auto_heal

Called when a peer reaches `Failed` state:
1. Remove the failed node from the local routing table.
2. Send `CLUSTER SYNC <surviving-nodes>` to all other alive peers.
3. Log that data on the failed node is lost (no DRAIN — the node is unreachable).

This is a best-effort liveness fix, not a durability guarantee. Combine with the replication layer for data safety.

### Gossip

The `CLUSTER GOSSIP <addr=state,...>` internal command is sent after each successful heartbeat. The receiver calls `apply_gossip` which only applies transitions that worsen state — this prevents old `alive` gossip from resurrecting a node that a third peer has already marked `failed`.

---

## Concurrency Model

| Shared state | Protection |
|---|---|
| Key-value store | `DashMap` (shard-level locks) |
| Cluster config | `Arc<tokio::sync::RwLock<ClusterState>>` |
| Health table | `tokio::sync::RwLock<HashMap<...>>` |
| Replication channel | `tokio::sync::broadcast` |
| Raft log | `tokio::sync::Mutex` inside consensus module |

The cluster `RwLock` is always acquired, the routing decision made, and the lock *released* before any I/O (proxy TCP call, migration push). This prevents holding a read lock across a potentially slow network call.

---

## What Is Not Implemented

- **RDB snapshots** — AOF only.
- **Connection pooling** for intra-cluster RPCs — a new TCP connection is opened per proxy call. Phase 11 (performance) would add pooling.
- **TLS** — all traffic is plaintext.
- **Pub/Sub** — not implemented.
- **Transactions (MULTI/EXEC)** — not implemented.
- **Replica promotion in cluster mode** — each shard is a single node; adding replication per shard is a future phase.
- **Prometheus metrics** — Phase 10 (observability).
