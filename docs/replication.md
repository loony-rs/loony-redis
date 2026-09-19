# Replication

## There is exactly one write/replication path

The prior prototype had two independent paths: Raft-backed writes
(`consensus/`) and a separate async broadcast-channel replication
(`replication/`), mutually exclusive at startup and never combined with
sharding. This is replaced with a single path: **every write to every
shard goes through that shard's Raft group.** There is no "plain
replication mode" and no per-write choice between strong and weak
replication — see `docs/decisions/0004-single-write-path.md`.

Weaker-than-linearizable replica reads remain available, but only as an
explicit, opt-in *read* mode ([[consistency]]), never as a separate write
path. This means replication in this system *is* Raft log replication as
described in [[raft]]; this document exists to state the resulting
operational properties rather than to describe a second mechanism.

## Replication factor and quorum

```
replication_factor = 3   (default, configurable per Prompt.md section 46)
quorum = floor(replication_factor / 2) + 1 = 2
```

A shard with RF=3 tolerates exactly 1 replica failure (including a leader
failure, which becomes a follower failure from the surviving quorum's
perspective) while remaining available for both reads and writes. Losing
2 of 3 replicas takes the shard out of quorum: it becomes unavailable for
writes and for default (leader) reads until enough replicas recover.

## Follower catch-up

A follower that reconnects after being down (but with intact disk) reports
its last log index to the leader on rejoining Raft communication; the
leader sends only the missing suffix (AppendEntries), or a snapshot if the
follower is too far behind (log already compacted past the follower's
index) — both are `openraft` built-ins, not custom code (see [[raft]]).

A brand-new replica (empty disk) joins as a Raft *learner*: it receives a
full snapshot, replays it, then receives log entries going forward until
caught up, at which point the shard's Raft group promotes it to a voting
member. It never counts toward quorum while still a learner, so adding a
slow-to-catch-up replica cannot itself endanger existing quorum safety.

## Replication lag

Defined and exposed (see [[observability]]) as
`leader.commit_index - follower.last_applied_index` for each follower of
each shard, sampled from the leader's view of AppendEntries acknowledgment
plus periodic follower self-report. This is the number that
`READ FROM REPLICA` staleness ([[consistency]]) is bounded by, and it is
what "slow follower" backpressure ([[performance]] /
`Prompt.md` section 20) is measured against.

## Failure behavior (cross-reference)

Concrete scenarios (leader crash, minority/majority partition, stale
follower, repeated leader failures) are specified in [[failure-model]] and
verified by the tests in [[testing]]; this document only states the
steady-state mechanism they build on.
