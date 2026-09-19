# Consistency Model

## Default mode: strong / linearizable

Every write and every default (leader) read is linearizable with respect
to other linearizable operations on the same key. Concretely:

```
client
  |  SET k v
  v
shard leader (owns the key's slot)
  |
  v
append to local WAL, propose Raft entry
  |
  v
replicate AppendEntries to followers
  |
  v
quorum of replicas durably persist the entry
  |
  v
leader marks entry committed (commit_index advances)
  |
  v
state machine applies the entry (storage mutation happens here)
  |
  v
leader sends client the response
```

The response is sent **after** apply, not merely after commit, so that a
client's next GET to the same leader is guaranteed to observe its own
prior write (read-your-writes, and more generally linearizability, requires
this — committing without applying would let a client observe "OK" for a
write the state machine hasn't performed yet).

This is a direct implementation of invariant S7 in [[invariants]].

## Leader reads

Default `GET` (and other reads) are served only by the shard leader, and
only after confirming (via the Raft leader-lease/quorum-heartbeat check
described in [[raft]]) that it is still the leader for the current term.
A leader that has lost its quorum stops answering reads as leader rather
than serving a value that a newer leader elsewhere may have already
overwritten.

## Documented behavior for specific scenarios

- **GET after SET (same client, same leader):** always observes the
  write. Guaranteed by the apply-before-ack ordering above.
- **Concurrent SET from two clients:** both go through the same leader's
  Raft log, so they are linearized in whatever order the leader accepts
  and proposes them; the "losing" write's effect is visible to any GET
  issued after both complete, per normal last-write-wins-by-log-order
  semantics. There is no read-modify-write atomicity across separate
  commands unless a command is itself atomic (e.g. a single `HSET` is
  atomic; two separate `GET`+`SET` calls from a client are not, exactly
  like Redis).
- **GET during leader failure:** in-flight requests to the failed leader
  time out or get a connection error; the client must retry. A retried GET
  against the newly elected leader observes all commands committed before
  the failure, plus nothing that was proposed-but-not-committed at the old
  leader (those are correctly discarded, per R4 in [[invariants]]).
- **GET during a network partition:** on the majority side, reads proceed
  normally against the (possibly newly elected) leader. On the minority
  side, any node that was leader steps down (S8) once it cannot maintain
  quorum heartbeats within the election timeout, and stops serving leader
  reads; clients on that side see errors/timeouts, not stale data.
- **Stale followers:** a follower that has fallen behind never serves
  reads in the default read mode (leader reads only) and can never win an
  election with a log that is behind the committed history of a quorum,
  because Raft's vote-granting rule requires the voter to check the
  candidate's log is at least as up to date as its own.
- **Failover:** the new leader's log, by construction of the Raft election
  safety property, contains every entry any previous leader ever
  committed. No committed write is ever rolled back by a failover.
- **Node restart:** a restarted node replays snapshot + WAL
  ([[persistence]], [[recovery]]) before rejoining; until it has caught up
  to the current commit index it does not serve as leader (it can still
  vote, but only if its log is at least as up to date as the requester's,
  same rule as above).

## Optional replica reads: `READ FROM REPLICA`

This is a separate, explicitly opt-in read mode, never the default. A
client (or a per-connection/per-command flag) requests it explicitly;
absent that flag, all reads go to the leader as described above.

Guarantees for `READ FROM REPLICA`:

- **Maximum guarantee: bounded staleness, not linearizability.** A replica
  read reflects some prefix of the committed log up to the replica's own
  `last_applied` index, which may lag the leader's commit index by an
  unbounded amount under partition or slow replication (see
  `replication_lag` metric in [[observability]]).
- **No monotonic reads guarantee across different replicas.** If a client
  issues replica reads that happen to land on different followers (or the
  same follower before and after it briefly falls behind and catches up
  unevenly relative to another key), it can observe values go "backward"
  in the sense of returning to an older committed value it had already
  moved past — this can only happen by switching which physical replica
  answered the request. A client that pins itself to one specific replica
  connection observes a monotonically non-decreasing view of that
  replica's applied log (a single follower always applies in log order,
  per S4), but that is a property of not changing replicas, not a
  cluster-wide guarantee.
- **Partition behavior:** a replica on the minority side of a partition
  continues answering `READ FROM REPLICA` with whatever it last applied
  before the partition, growing more stale the longer the partition
  persists. It never blocks or errors just because it can't reach the
  leader — that would defeat the purpose of a replica-read mode. This
  staleness is exactly what is being traded for availability, and it must
  be requested explicitly by the client.
- **Never implied to be linearizable.** No client-facing documentation,
  error message, or command help text may describe `READ FROM REPLICA` as
  strongly consistent, "eventually consistent" in a way that hides the
  bound, or interchangeable with default reads.

## Multi-key operations

An operation touching multiple keys is only atomic if every key maps to
the same slot (enforced via Redis-style hash tags, see [[sharding]]). Such
operations are proposed as a single `Command` and go through the same
apply-after-commit path as a single-key write, so they get the same
linearizability guarantee as any other single command. Operations spanning
multiple slots are explicitly out of scope for v1 (see non-goals) and must
be rejected by the router before reaching any shard, not silently executed
non-atomically.
