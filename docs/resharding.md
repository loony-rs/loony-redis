# Resharding

## Why the prototype's approach is insufficient

`cluster/mod.rs`'s `drain_slots`/`push_entries_to`/`auto_heal` drained keys
and pushed them to the target, then swapped `ClusterConfig` — an unstaged
copy-then-flip with no resumability and a window where a key can vanish
between drain and push-completion. This violates T2/T6 in [[invariants]]
(migration semantics must be defined per operation and per state) and is
replaced by an explicit state machine, coordinated through the metadata
Raft group ([[membership]]).

## Migration states

```
PREPARING -> TRANSFERRING -> CATCHING_UP -> CUTOVER -> COMPLETED
```

All states for a given slot (or contiguous slot range being migrated
together) are recorded as a `SlotMigration` record in the metadata group's
`ClusterState`, keyed by slot range, with `source_shard`, `target_shard`,
and the current state. This record is the single source of truth clients
and nodes consult — never a node-local flag.

### PREPARING
- Operator (or the control plane's rebalancer) proposes a `SlotMigration`
  entry to the metadata group: source shard, target shard, slot range,
  state = `PREPARING`.
- Target shard allocates space/registers the incoming range; no data has
  moved yet. Source shard continues serving all traffic for the range
  normally, as if no migration were happening.
- Client-visible behavior: unchanged — GET/SET/DEL for keys in the range
  behave exactly as steady-state ownership by the source.

### TRANSFERRING
- Source shard's leader begins streaming a consistent snapshot of the
  slot range's keys to the target shard's leader (an internal RPC, not a
  client-facing operation).
- Source shard continues serving reads and writes normally during the
  transfer. Writes that land on keys already transferred are **also**
  forwarded to the target (see "mutation tracking" below) so the target
  doesn't miss them.
- Client-visible behavior: identical to `PREPARING` — all traffic for the
  range still goes to source, still normal MOVED-based routing (nothing
  points at target yet).

### CATCHING_UP
- Bulk transfer is complete. The target now applies a queue of mutations
  that occurred on the source during/after the bulk transfer (see
  "mutation tracking"). This state exists specifically to close the gap
  between "snapshot taken" and "fully caught up to source's live state,"
  which the prototype had no equivalent of.
- Client-visible behavior: still unchanged; source still authoritative.
- Exit condition: target's applied state for the range is within a small,
  bounded lag of source's live state (analogous to Raft catch-up, but
  across shards rather than within one Raft group).

### CUTOVER
- A short state during which the metadata group commits the ownership
  change atomically: `slot range: source_shard -> target_shard`. During
  this specific state (bounded, target a few hundred ms to low seconds,
  not open-ended):
  - **GET/SET/DEL arriving at source**: source replies `-ASK <slot>
    <target addr>` rather than serving the key directly. This models
    "ownership is in the middle of transferring; the correct owner is
    over there but hasn't confirmed it's fully live yet," matching Redis
    Cluster's `ASK` semantics.
  - **A client that receives -ASK must send `ASKING` then retry the exact
    command against the target** (see [[protocol]]) — the target accepts
    the command only when prefixed by `ASKING` for that connection, to
    distinguish "directed here by an ASK redirect" from "ordinary
    possibly-misrouted request," matching Redis Cluster's rationale for
    the two-step ASK/ASKING protocol.
  - Source shard stops accepting new writes for the range the instant it
    enters CUTOVER (returns `-ASK` for all of them), guaranteeing no write
    can land on source after this point and be missed by target.
- Exit condition: metadata group's ownership-change entry for the range is
  committed. This is the atomic instant ownership legally transfers.

### COMPLETED
- Metadata group's slot table now shows target as sole owner, with no
  `SlotMigration` record for the range (or the record is retained with
  state=`COMPLETED` for audit/observability, then garbage-collected).
- Client-visible behavior: normal steady-state routing to target; a
  request that still lands on source gets an ordinary `-MOVED` (source no
  longer owns the slot at all, this isn't an ASK situation anymore).
- Source shard drops its copy of the migrated range's data once it has
  confirmed (via the committed ownership change) that it's no longer
  needed — never before COMPLETED, so a rollback/failure before this point
  can always fall back to "source still has everything."

## Mutation tracking during TRANSFERRING/CATCHING_UP

Every write the source shard's Raft group commits for a key inside an
actively migrating range is, in addition to being applied locally,
appended to a per-migration forwarding queue and streamed to the target.
The target applies forwarded mutations in the order it receives them,
which is safe because they arrive in the source's own commit order (the
forwarding is done by the source's apply loop, right after it applies the
command locally — see [[raft]]'s apply step). This queue is bounded (see
[[performance]] backpressure); if it fills because the target can't keep
up, the migration simply stays in `TRANSFERRING`/`CATCHING_UP` longer
rather than dropping mutations.

## Failure handling per state

- **Source fails during PREPARING/TRANSFERRING/CATCHING_UP**: source's
  shard Raft group elects a new leader per normal shard failover
  ([[raft]]); the new leader has the same committed state (including any
  migration-forwarding-queue entries that were themselves part of
  committed source-shard log entries, if forwarding progress is itself
  tracked as replicated state) and resumes transferring. If forwarding
  progress was only in the old leader's local memory, the new leader
  restarts the transfer/catch-up for the range from the target's last
  confirmed applied point (target reports this on reconnect) — never from
  scratch, and never skipping data.
- **Target fails during TRANSFERRING/CATCHING_UP**: target's shard elects
  a new leader; migration resumes once the new leader is ready, since
  source hasn't given up ownership yet (still `PREPARING`/`TRANSFERRING`/
  `CATCHING_UP`, source is still fully authoritative and safe to keep
  serving from). No data loss risk because nothing was ever removed from
  source before `COMPLETED`.
- **Either side fails during CUTOVER**: CUTOVER is defined as "commit an
  ownership-change entry to the metadata group." Either that commit
  happened (state is now effectively COMPLETED from the metadata group's
  perspective, even if a node hasn't heard yet — it will on reconnect) or
  it didn't (state is still CATCHING_UP/TRANSFERRING from the metadata
  group's perspective, and both shards' Raft groups have their own
  independent leadership regardless of the node failure, so retry is safe;
  the migration coordinator retries the CUTOVER commit attempt).
- **Migration interrupted/restarted**: because `SlotMigration` state is
  itself Raft-replicated in the metadata group, any node/coordinator can
  read the current state after a restart and resume from exactly that
  state — there's no separate "migration coordinator" whose crash loses
  progress; the coordinator role can be picked up by any node by reading
  `ClusterState`.

## Client-visible retry contract

A client (or the routing layer in [[protocol]]) must, on `-MOVED`, update
its routing cache and retry against the new owner; on `-ASK`, send
`ASKING` then retry the *same* command against the named target without
updating its long-term routing cache (the redirect is temporary, scoped
to this one migration, per Redis Cluster convention). Clients must never
treat `-ASK` as a permanent ownership change.
