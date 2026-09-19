# Invariants

This is the formal contract the rest of the system is checked against. Every
phase's tests (see [[testing]]) must include a check for every invariant
that phase makes relevant. An implementation that violates one of these is
a bug regardless of how it was produced — including a bug in this document
if a stated invariant turns out to be unachievable, in which case the
document changes and every dependent design changes with it, not the other
way around.

## Safety invariants

**S1. No lost commits.** A command that has been committed (replicated to
and durably persisted by a quorum of a shard's Raft group) cannot
subsequently disappear from that shard's history, across any sequence of
crashes, restarts, elections, or partitions.

**S2. At most one leader per term.** For a given shard's Raft group and a
given term number, at most one node believes it is leader and is willing
to accept writes. This follows directly from Raft's quorum vote and is
checked by construction (only one candidate can win a majority vote in a
term) plus by test (concurrent-candidacy and network-partition scenarios
in [[testing]]).

**S3. Followers never commit conflicting history.** A follower's log,
restricted to committed indices, is always a prefix-consistent match of
its current leader's log. A follower never applies an entry at index `i`
that differs from what the leader that owns the current term has at index
`i`.

**S4. Apply order follows commit order.** The state machine for a shard
applies committed log entries strictly in increasing index order, with no
gaps and no reordering, on every replica.

**S5. Deterministic replication.** Given the same prefix of committed log
entries, every replica's state machine reaches bit-for-bit equivalent
state. This requires every replicated `Command` (see [[raft]] /
architecture) to be a pure function of its own fields — no wall-clock
reads, no local randomness, no HashMap-iteration-order dependence, no
thread-scheduling dependence. Any timestamp a command needs (e.g. TTL
`expire_at`) is computed once, by the proposer, and carried explicitly in
the command.

**S6. Unambiguous slot ownership.** At every point in time, every one of
the 16384 slots is in exactly one of two states: owned by exactly one shard
(steady state), or in a well-defined migration state with an explicit
source shard and target shard (see [[resharding]]). There is never a slot
with zero owners, and never a slot whose ownership is ambiguous outside of
a documented migration state.

**S7. No premature acknowledgment.** A client is never told a strongly
consistent write succeeded (`+OK`, or equivalent) before that write's
command has been committed by Raft quorum for its shard **and** applied to
that shard's state machine. See [[consistency]] for the exact
happens-before chain.

**S8. Term monotonicity and immediate step-down.** A node's observed
current term never decreases. On observing a term higher than its own in
any RPC, a node updates its term and steps down from candidate/leader state
before processing anything else in that RPC — there is no code path that
finishes a leader-only action using a stale term.

**S9. Idempotent replay.** Re-delivering (duplicating) any Raft RPC, or
re-applying any already-applied log entry (e.g. during crash recovery), has
no observable effect beyond the first application. This follows from S4
plus indexing applied entries by their log index and never reapplying an
index already reflected in the persisted `last_applied` marker.

## Recovery invariants

**R1. Crash cannot silently lose committed data.** If a node acknowledged
fsync of a WAL record for entry `i` before crashing, then after restart
that node's log contains entry `i` (or the node correctly identifies its
WAL as corrupted and refuses to serve, per [[failure-model]], rather than
silently omitting the entry).

**R2. Deterministic WAL replay.** Replaying a node's WAL from empty state
in order always reproduces the same sequence of log entries, independent
of when or how many times replay is executed. (Direct consequence of S5
applied to persistence rather than network replication.)

**R3. Snapshot + WAL tail reproduces committed state.** For any snapshot
taken at applied-index `k`, restoring that snapshot and replaying every
WAL entry with index `> k` up to the current commit index produces state
identical to what continuous application would have produced. This is the
invariant that makes log compaction safe.

**R4. Uncommitted entries may be lost; committed entries may not.** An
unclean shutdown may lose log entries that were appended but never reached
quorum-committed status (this is expected and safe — the client never
received an ack for them, per S7). It must never lose an entry at or below
the persisted commit index.

## Routing invariants

**T1. Every slot has a defined owner or an explicit migration state.**
(Restates S6 from the routing side: this is enforced by the metadata Raft
group described in [[membership]] / [[sharding]], which is the single
source of truth for slot ownership — not local per-node config, not
gossip.)

**T2. Migration semantics are documented per operation.** For every client
operation type (GET/SET/DEL/etc.) and every migration state
(PREPARING/TRANSFERRING/CATCHING_UP/CUTOVER/COMPLETED), the response
behavior is defined in [[resharding]] before the state is implemented. A
migration state with an undefined operation behavior is an incomplete
design, not an implementation detail to figure out later.

**T3. MOVED/ASK responses are never fabricated from stale local state
alone.** A node only returns `-MOVED` when its local view of slot ownership
(itself derived from the consensus-backed metadata group, T1) says another
shard owns the slot, and only returns `-ASK` when the metadata group has
recorded that slot as actively migrating with this node named as the
temporary redirect target.

## What is explicitly NOT guaranteed

- Linearizability for replica reads (`READ FROM REPLICA`); see
  [[consistency]] for the exact weaker guarantee offered there.
- Any guarantee under Byzantine behavior (see [[system-model]]).
- Cross-shard atomicity for multi-key operations spanning slots that are
  not co-located by a hash tag (see [[sharding]]).
