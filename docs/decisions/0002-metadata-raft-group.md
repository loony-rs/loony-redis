# 0002 — Cluster metadata gets its own dedicated Raft group

## Decision

Cluster membership, slot ownership, replica placement, and migration
state are replicated through one dedicated "metadata" Raft group, separate
from every per-shard data Raft group.

## Context

`Prompt.md` requires (a) each shard to have an independent Raft group,
explicitly forbidding one global Raft group for the entire database
(section 8), and (b) unambiguous, consensus-quality slot ownership and
consensus-backed membership (sections 6, 27, invariant S6/T1). The
prototype satisfies neither: it has no per-shard Raft at all, and its
membership/slot-ownership state (`ClusterConfig`/`HealthTable`) is
gossip/heartbeat-driven with no consensus backing, allowing a 3-missed-
heartbeat `auto_heal` to unilaterally rewrite routing.

These two requirements — "no single global Raft group" and "consensus-
backed membership" — could look contradictory unless membership is
recognized as a distinct workload from key/value data.

## Options considered

1. **Membership via gossip only (status quo), routing derived from
   local per-node belief.** Rejected: this is exactly the design that
   fails S6/T1 and risks the split-brain-adjacent `auto_heal` behavior
   already found in the gap analysis.
2. **Fold membership into one of the shard Raft groups (e.g., shard 0
   also carries cluster metadata).** Rejected: couples an arbitrary
   shard's availability/load to cluster-wide control-plane operations,
   and makes "shard 0 is down" a uniquely worse event than any other
   shard being down, for no good reason.
3. **A single global Raft group that both replicates all key/value data
   and cluster metadata.** Rejected outright — this is precisely the
   "one global Raft group for the entire database" `Prompt.md` forbids,
   and would serialize all writes across all shards through one log,
   destroying the independent-leadership/parallel-writes benefit of
   per-shard groups entirely.
4. **A separate, dedicated metadata Raft group, small and independent
   from every data shard's group.** Chosen.

## Chosen approach

The metadata group is a Raft group like any other (per [[raft]]), but its
replicated `Command` set is cluster-control-plane operations only
(`AddNode`, `RemoveNode`, `UpdateSlotOwnership`, `SlotMigration` state
transitions — see [[membership]], [[resharding]]) — it never carries a
key/value write. It is not "the one global group for the database"
because it does not replicate database data at all; it is infrastructure
for the routing layer, analogous to a dedicated metadata/placement service
in other sharded systems (e.g., a PD/master-style component), just
implemented as a Raft group rather than a bespoke service.

## Trade-offs

- One more Raft group to run and monitor per node. Accepted: it is
  small (low write volume — membership/topology changes are rare relative
  to key/value traffic) and its own quorum failure only affects the
  ability to *change* topology, not to serve already-routed traffic on
  existing shards (routers keep serving with their last-known-good cached
  `ClusterState` until the metadata group is reachable again).
- Introduces a second kind of Raft group in the codebase (metadata vs.
  data shard), requiring the `Command` enum / state machine to be
  generic enough to host both without the type system conflating them.
  Accepted as a deliberate, documented distinction (see [[architecture]]),
  not accidental complexity.

## Consequences

- `ClusterState` (node list, slot table, replica rosters, migration
  records) is defined once, as the metadata group's state machine, and is
  the single source every router/admin-command implementation reads from.
- Gossip/heartbeats remain, but strictly as *inputs that trigger a
  proposal* to the metadata group, never as a direct mutation path (see
  [[failure-model]]'s "failure detection is not authority").
