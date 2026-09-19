# Architecture

## Starting point

This is not a greenfield design. A prior single-process prototype already
exists on this branch (pre-`v2` history, preserved in git). A code-grounded
gap analysis against this document's requirements found:

**Reusable as-is (adapt, don't rewrite):**
- RESP parser/encoder (`protocol/`) — correctly incremental, handles
  fragmented and pipelined reads, has round-trip tests.
- Data type implementations (`storage/{list,hash,set,zset}.rs`) — String,
  List, Hash, Set, ZSet all present over `DashMap`.
- Observability skeleton (`observability/`) — real Prometheus `/metrics`
  and `/health`, needs more metrics, not a rewrite.

**Must be rebuilt, not patched, because they fail the requirements in this
document's other sections:**
- `consensus/` — one hand-rolled Raft group per process, in-memory-only
  log (lost on restart), never composed with sharding. Replaced by a
  per-shard multi-Raft design ([[raft]]) built on a mature Raft crate.
- `persistence/` — AOF only, no checksums, no seq/term fields, no
  configurable fsync, no snapshotting. Replaced by a real WAL
  ([[persistence]]).
- `cluster/` membership and migration — nodes identified by `IP:port`
  only, membership is gossip/heartbeat with no consensus backing, migration
  is unstaged copy-then-flip, redirection is silent server-side proxying
  instead of client-facing `MOVED`/`ASK`. Replaced per [[membership]],
  [[resharding]], [[protocol]].
- TTL representation — uses process-local monotonic `Instant`, which is
  nondeterministic across replicas and meaningless after restart. Replaced
  with an explicit `expire_at: Option<u64>` (epoch millis) carried inside
  the replicated command, per invariant S5.

This asymmetry — keep the data/protocol layer, rebuild the distribution
layer — is why the workspace is restructured into crates below rather than
kept as one `src/` tree: it lets the reused code become a stable,
independently-tested dependency of the rebuilt layers instead of being
entangled with them again.

## Layered architecture

```
Client
  |
  v
RESP Server            (protocol framing, connection lifecycle)
  |
  v
Command Parser/Validator (limits, typed command structs)
  |
  v
Cluster Router          (slot lookup, MOVED/ASK, migration awareness)
  |
  v
Slot -> Shard mapping    (owned by the metadata Raft group)
  |
  v
Shard / Raft group       (one independent Raft group per shard)
  |
  v
State Machine             (deterministic command application)
  |
  +----> In-memory storage (DashMap-backed, reused from prototype)
  |
  +----> WAL              (per-shard, authoritative log)
  |
  +----> Snapshot          (compacts WAL, restores state machine)
```

## Control plane vs. data plane

```
Control Plane                       Data Plane
--------------                      ----------
Node identity                       Client requests
Cluster membership                  Routing (slot -> shard leader)
Slot ownership table                Raft replication (per shard)
Replica placement                   State machine execution
Resharding coordination             WAL
                                     Snapshot / recovery
```

The control plane is implemented as its own dedicated Raft group (the
**metadata group**), not as gossip and not as a per-node local file. This
is deliberate: [[system-model]] requires unambiguous slot ownership (S6)
and consensus-backed membership, and gossip alone cannot provide either
without risking split-brain (see [[failure-model]]). The metadata group
replicates a small amount of data (node list, slot ownership table,
migration state) and is not the same thing as "one global Raft group for
the whole database" — it never replicates key/value data, and the
per-shard groups remain independent for actual data traffic. See
`docs/decisions/0002-metadata-raft-group.md`.

## Module map (target crate layout)

```
crates/
  protocol/      RESP2 parser + encoder (ported from src/protocol)
  storage/       Data types + TTL-aware store (ported from src/storage)
  raft/          Per-shard Raft group wiring on top of a chosen Raft crate
  persistence/   WAL + snapshot, shared by every Raft group (data + metadata)
  cluster/       Slot table, hash-tag CRC16, routing, migration state machine
  membership/    Node identity, join/leave, metadata Raft group's command set
  replication/   (folded into raft/ — no separate async-broadcast replication path)
  commands/      Command parsing/validation, dispatch to state machine
  server/        RESP TCP server, connection handling, wiring of the above
  client/        Minimal cluster-aware client for integration tests
  metrics/       Prometheus metrics + health endpoints
  test-utils/    Fault-injection harness (process control, network faults)
```

`replication/` from the prototype is not carried forward as a separate
module: async broadcast-based replication was the weak-consistency path
identified in the gap analysis, and per-shard Raft (which every write goes
through unconditionally, no "Raft vs. plain replication" branch) subsumes
it. There is exactly one write path, not two.

## Concurrency model

- Tokio for all networking and connection I/O.
- Raft group drivers and WAL/storage mutation run as their own tasks per
  shard; a slow disk in one shard's WAL must not stall another shard's
  Tokio tasks (bounded channels between the async network layer and each
  shard's apply loop, sized per [[performance]]).
- No `unsafe`. `DashMap` (already in use) provides the concurrent map;
  correctness for replicated state does not depend on it beyond
  memory-safety, since determinism (S5) is enforced at the command level,
  not the data-structure level.

## What changes are deferred

Everything in `Prompt.md` section 49 (non-goals): Lua scripting, cross-shard
transactions, Pub/Sub, Streams, modules, geospatial, full Redis
compatibility, complex eviction, BFT, multi-region, global transactions.
