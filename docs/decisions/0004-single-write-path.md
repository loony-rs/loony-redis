# 0004 — One write path (per-shard Raft), retire the async broadcast replication module

## Decision

Every write to every shard goes through that shard's Raft group
(propose -> commit -> apply -> ack, per [[raft]]/[[consistency]]). The
prototype's separate `replication/mod.rs` (a `tokio::sync::broadcast`
fan-out with fire-and-forget follower delivery, used whenever Raft was not
configured) is removed, not kept as an alternate mode.

## Context

The prototype has two mutually exclusive write paths selected at startup:
Raft-backed (linearizable-ish, when `--raft-addr` is set) and plain async
replication (weak — `commands/mod.rs` executes the write locally and
acks the client *before* any follower has received or applied it, via a
fire-and-forget broadcast send). Cluster/sharding mode and Raft mode were
never combined in the prototype, so any deployment actually using
sharding only ever got the weak path. This directly violates S7 in
[[invariants]] ("no premature acknowledgment") whenever the weak path is
in effect, and having two paths at all makes the system's consistency
guarantee a function of *how it was started* rather than a fixed,
documented property — which `Prompt.md` sections 11-12 require to be
explicit and singular (strong by default, replica reads as an explicit,
separate, clearly-weaker opt-in — not an alternate *write* mode).

## Options considered

1. **Keep both paths, document which is "the fast one."** Rejected:
   directly reproduces the S7 violation as a supported, documented mode,
   which `Prompt.md` explicitly disallows ("never acknowledge a strongly
   consistent write unless... consensus conditions have been satisfied").
2. **One path only: every write is a Raft proposal against its shard's
   group.** Chosen. Simplifies the system's consistency story to a single
   sentence (see [[consistency]]) and removes an entire module
   (`replication/`) as redundant once every shard has its own Raft group
   regardless of size.

## Chosen approach

`replication/mod.rs` is deleted. What used to be "replication" is now
just the steady-state behavior of a shard's Raft group, described in
[[replication]] (which documents lag, catch-up, and quorum semantics as
properties of Raft replication, not a separate mechanism).

## Trade-offs

- A single-node deployment (RF=1, no real replication needed) still goes
  through a (degenerate, single-member) Raft group for every write.
  Slightly more overhead than a direct local write, accepted for
  uniformity — there is no special-cased "small deployment" write path
  that could silently diverge in behavior from the replicated case.
- Removes the option of ever running with weaker-than-quorum write
  acknowledgment as a deliberate throughput trade-off. If that trade-off
  is wanted later, it must be designed as an explicit, clearly-labeled
  mode (matching the rigor `Prompt.md` section 12 demands for read modes)
  — not resurrected as a silent default.

## Consequences

- Every write, on every deployment shape (including single-node), has the
  same acknowledgment contract: committed and applied before ack (S7).
- `READ FROM REPLICA` ([[consistency]]) is the only place weaker-than-
  linearizable behavior is exposed, and only for reads.
