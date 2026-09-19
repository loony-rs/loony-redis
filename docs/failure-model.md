# Failure Model

This document enumerates the concrete fault classes the system must handle,
how each is detected, and what the system's response contract is. See
[[system-model]] for the trust assumptions this builds on.

## Fault classes and responses

| Fault | Detection | Response contract |
|---|---|---|
| Process crash (follower) | Peers stop receiving heartbeats/AppendEntries acks | Leader continues with remaining quorum; failed replica catches up on restart via [[recovery]] |
| Process crash (leader) | Followers' election timers expire | New leader elected via Raft (see [[raft]]); in-flight unacknowledged client writes must be retried by the client, none are silently double-applied |
| Node restart | Node re-executes startup sequence | Node reloads identity, replays [[persistence]] state, rejoins its Raft group(s) as a fresh member catching up from its last durable index |
| Network delay | N/A (tolerated, not "detected") | Raft correctness does not depend on bounded delay; only liveness (electing a leader, committing writes) does |
| Network packet loss | Missing acks / timeouts | Raft RPCs are retried; retried AppendEntries/RequestVote are idempotent by (term, index) |
| Network packet duplication | N/A | All Raft RPCs and client command IDs are idempotent; duplicate delivery must not double-apply a command |
| Network reordering | N/A | Raft log entries carry (term, index); out-of-order RPCs are rejected/reconciled by index comparison, never applied out of order |
| Temporary partition | Missed heartbeats across the cut | Minority side cannot elect a leader or commit; majority side continues; on heal, minority nodes discover a higher term and step down / truncate divergent suffix per Raft rules |
| Permanent node loss | Operator declares it lost (no automatic promotion to "permanent" from heartbeat failure alone) | Operator/admin command removes the node from cluster membership; its shard role must be re-replicated to a new node |
| Disk / WAL failure | fsync() error, checksum mismatch on read, short read at recovery | Node refuses to serve as leader or acknowledge writes with a corrupt/unwritable WAL; it marks itself unhealthy and exits the Raft group's active duty until an operator intervenes |
| Leader failure (repeated) | Same as leader crash, recurring | Each new leader must go through the same election + catch-up path; no cumulative special-casing |
| Follower failure (multiple, beyond quorum) | Loss of quorum | Shard becomes unavailable for writes (and default leader reads go with it, since reads route to the leader); this is a correct and required outcome, not a bug |

## Failure detection is not authority

Failure detection (heartbeat timeouts, gossip-style suspicion in
[[membership]]) produces a *local, unreliable* belief: `alive`,
`suspected`, `unreachable`, or `dead`. This belief:

- may drive an election timeout to fire, which starts a candidacy;
- may drive an admin/monitoring alert;
- **never** directly promotes a replica to leader, and **never** directly
  removes a node from cluster membership.

Only two things can change durable cluster state: a Raft election won by
quorum vote (for shard leadership), and a committed membership-change entry
in the metadata Raft group (for node join/leave). Gossip and heartbeat
failure detection are inputs to these processes, never substitutes for
them. This is what prevents a slow-but-alive node from being treated as
equivalent to a truly dead one in a way that could violate quorum safety.

## Split-brain prevention

See [[invariants]] for the formal safety statement. Mechanically:

- Every Raft term has at most one leader, established by majority vote.
- A node that observes a higher term than its own immediately steps down
  from candidate/leader state before doing anything else (no exceptions,
  no "finish this operation first").
- A leader that cannot maintain a quorum of acknowledged heartbeats within
  its election timeout stops treating itself as leader (Raft leader
  lease/step-down check on every client request path) rather than serving
  stale reads or accepting writes it cannot safely commit.
- Metadata (cluster membership, slot ownership) is itself Raft-replicated
  through the dedicated metadata group (see [[membership]]), so it is
  subject to the same term/quorum rules — there is no separate
  gossip-only source of truth that could disagree with it.

## Network partition scenarios (must be tested, see [[testing]])

For a 3-replica shard `{A, B, C}`:

- **Minority partition `A | B C`**: `A` cannot win an election or commit
  writes (needs 2 votes/acks, has at most 1). `B`/`C` can elect a leader
  between themselves and continue serving reads and writes. When the
  partition heals, `A` (as follower or a stale ex-leader) discovers the
  higher term from `B`/`C`'s current leader, steps down if needed, and
  repairs its log via Raft's normal AppendEntries consistency check —
  never by unioning divergent history.
- **Majority partition `A B | C`**: `A`/`B` retain quorum and can continue.
  `C` alone cannot.

## Disk/WAL failure handling

WAL and snapshot writes are checksummed (see [[persistence]]). On any
detected corruption (checksum mismatch, truncated record) during normal
operation, the node stops accepting new writes for the affected shard
immediately and surfaces the fault via logs/metrics/health endpoint,
rather than continuing to append to a log it cannot trust. During
recovery, a corrupt tail is truncated at the first bad record (this can
only affect uncommitted entries — see the WAL/commit ordering invariant in
[[invariants]]) and the node rejoins as a lagging follower that catches up
normally.
