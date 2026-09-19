# Crash Recovery

## Startup sequence

```
process starts
  |
  v
load node identity (<data_dir>/identity); fail loudly if missing/corrupt
  |
  v
for each local group (each shard replica this node hosts, plus metadata
group if applicable):
  |
  v
  load latest valid snapshot (verify checksum; if corrupt, fall back to
  the next-older snapshot, log the corruption, alert via metrics)
  |
  v
  initialize openraft's storage state from that snapshot
  |
  v
  scan WAL from the snapshot's last-included index forward:
    - verify each record's checksum
    - on the FIRST checksum failure or short/truncated record: stop
      scanning, treat everything from that point as absent (truncate the
      on-disk WAL tail to the last good record), log + alert
    - feed each good record to openraft as its persisted log
  |
  v
  openraft resumes the group as a follower (or starts an election per
  normal rules if it was the sole/majority member and enough peers are
  also reachable) — no custom leader-selection logic here, see [[raft]]
  |
  v
  state machine's applied index is restored to match the snapshot; any
  WAL entries beyond the snapshot but at/below the persisted commit index
  are re-applied via the normal apply path (idempotent per S9) to bring
  the in-memory store to the correct state
  |
  v
rejoin cluster: announce reachability to peers; if this node's address
changed since last run, propose an address update through the metadata
group (see [[membership]] REJOIN)
  |
  v
catch up any Raft entries committed elsewhere while this node was down,
via normal AppendEntries/snapshot-install catch-up ([[replication]])
```

## Why a corrupt WAL tail is safe to truncate

Per R4 in [[invariants]], only entries below the persisted commit index
must survive a crash; anything above it was, by construction, never
acknowledged to a client or counted toward another group member's quorum
view. A corrupt tail can therefore only ever be in the "not yet
committed" region *if* the fsync policy in [[persistence]] is respected
correctly (commit-tracking only counts a record as durable after its own
fsync succeeds) — this is the property that makes "truncate at first bad
record" a safe recovery strategy rather than a data-loss risk. If a
checksum failure is ever found at or below the last known committed
index, that is a more serious fault (disk corruption of committed data);
the node must refuse to serve as leader or follower for that group and
surface a hard failure rather than silently proceeding, per
[[failure-model]]'s disk/WAL failure handling.

## Test matrix (see [[testing]] for the full harness)

Recovery must be tested after, at minimum:
- a committed write, then crash, then restart — state must reflect the
  write.
- an uncommitted (proposed but not quorum-acked) write, then crash — state
  must NOT reflect the write (it was correctly never acknowledged).
- a partial/torn WAL write (simulate by truncating a record mid-write) —
  node must recover to the last good record, not crash-loop or corrupt
  further.
- a snapshot taken, then further writes, then crash before another
  snapshot — recovery must be snapshot + WAL-tail replay, matching R3.
- leader crash specifically (recovery + rejoin as follower, not
  re-assuming leadership just because it used to be leader).
- follower crash specifically (recovery + catch-up to current leader).

## Rejoining vs. re-JOINing

A node with intact, valid identity and disk state recovers via the
sequence above and never goes through the JOIN protocol in [[membership]]
— JOIN is only for a node with no prior state (new node, or a
deliberately wiped/replaced one). Conflating the two would risk a node
re-registering itself as if it had no history while its disk actually
contains committed data other members are still relying on.
