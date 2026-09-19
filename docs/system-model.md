# System Model

## Scope

loony-redis is a horizontally scalable, distributed, in-memory key-value
database written in Rust. It is inspired by Redis and Redis Cluster but is
its own architecture, not a wire-for-wire clone.

This document defines the assumptions the rest of the design depends on:
what kind of failures the system tolerates, what trust model applies to
cluster members, and what "correct" means for this system.

## Deployment shape

- Target: 3-9 nodes for the initial implementation, scaling further without
  redesign.
- Each node runs one process (`kvdb-server`) that can host multiple shard
  replicas plus the metadata role.
- Default configuration: `replication_factor = 3`, `slot_count = 16384`.
- A shard is a Raft group of exactly `replication_factor` members: one
  leader, the rest followers.

## Node identity

- Every node has a stable identity (`NodeId`, a ULID assigned on first
  start and persisted to local disk under `data/<node_id>/identity`).
- Identity is independent of IP:port. A node may restart with a new address
  and rejoin using its persisted identity plus updated address, which is
  propagated through the metadata Raft group (see [[membership]]).
- Loss of the identity file is treated as loss of the node: it must rejoin
  as a brand-new node with an empty data directory, never reuse an old
  identity with different disk contents.

## Trust model

- **Byzantine behavior is explicitly NOT supported.** Every node in the
  cluster is assumed to run the real, unmodified loony-redis binary and to
  follow the protocol honestly. A node can crash, stall, or lose messages,
  but it will not lie, forge a term, forge a vote, or intentionally corrupt
  a message that passes its checksum.
- Untrusted external actors are handled at the client-facing edge (auth,
  ACLs, TLS — see `docs/decisions/` for extension points), not inside the
  Raft/cluster protocol.
- Because Byzantine faults are out of scope, correctness proofs and tests
  in this project reason about crash-stop and omission failures only.

## Supported failures

The system must remain correct (never violate an invariant in
[[invariants]]) and, where quorum allows, available under:

- process crash (any node, any role, at any point in its lifecycle)
- node restart (with local disk intact) after a crash
- network delay (arbitrary, unbounded within a test's timeout)
- network packet loss
- network packet duplication
- network message reordering
- temporary network partition (any subset vs. any subset)
- permanent node loss (disk and process gone forever)
- disk / WAL failure detection (fsync failure, truncated tail on unclean
  shutdown)
- leader failure (single or repeated, for a shard or for the metadata
  group)
- follower failure (single or multiple, up to the fault-tolerance bound)

## Explicitly not supported

- Byzantine / malicious nodes (see Trust model above).
- Silent, undetected disk bit-rot that defeats the WAL/snapshot checksums.
  Detected corruption is handled (see [[recovery]]); undetectable
  corruption is out of scope.
- Clock synchronization guarantees. No correctness property depends on
  wall-clock ordering between nodes. Wall clocks are used only for
  human-facing metrics/logging and for TTL values that are themselves
  replicated as explicit data (see [[consistency]]).
- Cross-shard atomic transactions (see [[sharding]] and the non-goals in
  `Prompt.md` section 49).

## Definition of correctness

A behavior is correct if it satisfies every invariant in
[[invariants]] under every failure combination listed above, for an
unbounded but finite number of failures and recoveries (the system must
never permanently wedge itself into an unsafe state; it may become
temporarily unavailable while consensus is unreachable).

Performance, feature completeness, and Redis command-surface compatibility
are secondary to this definition and are never grounds for weakening it.
