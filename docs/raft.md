# Raft

## Decision: use a mature crate, not a hand-rolled implementation

The prior prototype (`src/consensus/mod.rs`) hand-rolled Raft from scratch:
leader election, AppendEntries, RequestVote, all correct-looking but with
an **in-memory-only log** (lost on every restart, which breaks invariant
R1 in [[invariants]] outright) and no multi-group support. Per
`Prompt.md` section 9 ("Do not invent a subtly incorrect consensus
algorithm merely to avoid dependencies"), this is replaced.

**Chosen crate: [`openraft`](https://github.com/databendlabs/openraft).**
Rationale (full write-up in `docs/decisions/0001-raft-crate.md`):

- Async-native (Tokio), matching the rest of the system — no bridging
  between a sync Raft driver loop and the async server.
- Designed for **many concurrent Raft groups in one process**
  (`Raft<C>` instances are cheap, independent handles), which is exactly
  the per-shard model this system needs — unlike `tikv/raft-rs`, which
  gives you the log/election state machine but expects you to build the
  network transport, storage trait wiring, and the "drive one `RawNode`
  per group yourself" loop from scratch for every group.
- Built-in snapshot installation and log compaction hooks, and a
  documented single-server membership-change protocol, so [[resharding]]'s
  "add a replica to a shard" flow (see [[sharding]]) maps directly onto
  `openraft`'s `add_learner` / `change_membership` APIs instead of a
  custom implementation.
- Actively maintained, used in production systems (Databend), with an
  extensive test suite for the algorithm itself — the project's own
  correctness testing effort ([[testing]]) then focuses on *this system's*
  state machine, storage, and network wiring, not re-verifying Raft's core
  safety proof.

## What openraft provides vs. what this project implements

Provided by the crate (not reimplemented):
- Leader election, terms, candidate/follower/leader state transitions,
  election timeout / heartbeat scheduling.
- Log replication protocol (AppendEntries-equivalent), commit index
  advancement under quorum.
- Vote safety (candidate's log must be at least as up to date as the
  voter's).
- Single-server membership changes (learner -> voter promotion) and
  snapshot installation to a lagging/new member.

Implemented by this project, per shard group instance:
- `RaftLogStorage` / `RaftStateMachine` trait impls backed by the real WAL
  and in-memory store described in [[persistence]] — this is where "commit
  before apply, apply before ack" (S7) and "apply strictly in commit order"
  (S4) are actually enforced.
- `RaftNetwork` trait impl: RPCs travel over the existing internal
  cluster-addr TCP connections (not client-facing RESP), framed with a
  small internal wire format (length-prefixed bincode/postcard — decided
  per `docs/decisions/`), separate from the RESP port entirely.
- The `Command` enum (see [[architecture]] state machine section) that
  gets proposed — this is the project-specific payload Raft treats as an
  opaque log entry.

## One Raft group per shard, plus one metadata group

Every shard (default RF=3) runs its own `openraft::Raft` instance with its
own term sequence, log, and leader. A single node process hosts one
`Raft` instance per shard replica it holds, plus (if it hosts a metadata
role — every node does, since the metadata group is small and
lightweight) one instance for the metadata group. These are fully
independent: a term or election in shard 2's group has no relationship to
shard 5's group or the metadata group. This gives:

- independent leadership and parallel writes across shards (required by
  `Prompt.md` section 8);
- a failure (crash, partition) affecting one shard's quorum does not
  affect any other shard's availability;
- the metadata group's own small quorum (see [[membership]]) governs
  cluster-wide facts without becoming a bottleneck for per-key traffic.

## Term/commit/apply enforcement (ties to invariants)

```
propose(Command)
  -> openraft appends to leader's local log (via RaftLogStorage, i.e. WAL)
  -> replicates to followers, each appends to their own WAL
  -> once a quorum has durably appended (fsync per policy, see [[persistence]])
  -> openraft advances commit_index and calls RaftStateMachine::apply
  -> apply mutates the in-memory store (deterministically, S5)
  -> propose() future resolves with the apply result
  -> server sends the client response
```

No response is sent from the propose-future resolving early; `apply` must
have run (S7). This ordering is verified directly by the "leader crash
between commit and apply" test in [[testing]].

## Step-down and term monotonicity

`openraft` enforces S8 internally (a node observing a higher term steps
down before continuing). This project must not add any code path that
short-circuits or delays that step-down (e.g., no "finish serving this
in-flight leader read after we've already seen a higher term" exception).

## Log compaction / snapshots

Handled via `openraft`'s snapshot API, backed by this project's snapshot
format ([[persistence]]). A shard's log is compacted once a snapshot at
index `k` is durable; entries `<= k` may then be discarded from the WAL,
satisfying R3 in [[invariants]].

## Recovery

See [[recovery]] for the full startup sequence. In Raft terms: a restarted
node's `RaftLogStorage` impl reports its last known log state (from
snapshot + WAL) to `openraft` on initialization, which then correctly
resumes the node as a follower (or candidate, per normal election rules)
catching up via the crate's own AppendEntries/snapshot-install protocol —
no custom catch-up logic is written outside of implementing the storage
trait correctly.
