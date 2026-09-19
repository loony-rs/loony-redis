# 0001 — Raft implementation: adopt `openraft`, retire the hand-rolled consensus module

## Decision

Use the `openraft` crate for all Raft groups (per-shard and metadata).
Delete/replace `src/consensus/mod.rs`'s hand-rolled implementation.

## Context

The prototype's `consensus/mod.rs` implements leader election, terms,
AppendEntries/RequestVote, and log replication from scratch, with a
hand-rolled RPC transport over RESP frames. It has zero tests for
election, partition, or log-conflict scenarios, and its log is
in-memory-only (`log: Vec<LogEntry>`, comment at line 225 acknowledges
this is a stand-in "for Phase 5; would be on disk in production") — a
restart loses the entire Raft log, which is a direct correctness bug
under this project's own invariants (R1). It also only ever runs one
group per process, with no path to running one group per shard.

`Prompt.md` section 9 requires either integrating a well-tested Raft
implementation or, if hand-rolling, documenting it carefully and providing
all standard guarantees. Given the gaps found, "document it carefully" is
not sufficient remediation — the missing persistence and multi-group
support are structural, not documentation gaps.

## Options considered

1. **Keep and fix the hand-rolled implementation.** Add persistence,
   multi-group support, and a real test suite for election/partition
   safety. Rejected: reimplementing Raft's safety proof correctly, with
   its own test suite thorough enough to trust for a database's
   consensus layer, is a large, high-risk undertaking that a mature crate
   has already done. `Prompt.md` explicitly discourages this
   ("do not invent a subtly incorrect consensus algorithm merely to avoid
   dependencies").
2. **`tikv/raft-rs`** (the `raft` crate). Mature, used by TiKV/CockroachDB
   lineage, well-tested core algorithm. Rejected as the primary choice
   because it is a synchronous state-machine library only: it does not
   provide networking, storage persistence, or a "drive many groups"
   runtime — all of that is left to the integrator, effectively requiring
   this project to build its own async driver loop and per-group runtime
   management on top, which reintroduces much of the integration risk
   this decision is trying to avoid, especially for running many
   concurrent per-shard groups in one async (Tokio) process.
3. **`openraft`.** Async-native (Tokio), explicitly designed to run many
   concurrent `Raft<C>` instances in one process, with built-in snapshot
   installation, log compaction hooks, and documented single-server
   membership-change support. Chosen.

## Chosen approach

`openraft`, with this project implementing only the `RaftLogStorage`,
`RaftStateMachine`, and `RaftNetwork` traits (backed by the WAL in
[[persistence]], the in-memory store in [[architecture]], and internal
TCP connections respectively). See [[raft]] for the full mapping of
crate-provided vs. project-implemented responsibilities.

## Trade-offs

- Adds an external dependency for the single most safety-critical part of
  the system — accepted, because the alternative (trusting a from-scratch
  implementation with no partition/election test coverage) is a larger
  risk.
- `openraft`'s API surface and generic trait system has a learning curve;
  accepted as a one-time integration cost against every future group we
  add for free.
- Ties the project to `openraft`'s release cadence and any of its own
  bugs. Mitigated by pinning a version and tracking its changelog/issues
  as part of ongoing maintenance, same as any other core dependency.

## Consequences

- `src/consensus/mod.rs` is deleted, not incrementally patched.
- Every shard and the metadata group get independent `openraft::Raft`
  instances in the same process (see [[raft]], [[sharding]]).
- Persistence work ([[persistence]]) is scoped as "implement the storage
  trait correctly," not "design a Raft log format from first principles."
