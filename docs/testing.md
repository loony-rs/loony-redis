# Testing

## Current coverage (gap analysis baseline)

The prototype has 29 inline unit tests, all happy-path, concentrated in
`cluster/` (CRC16/hash-tag), `protocol/` (RESP round-trip/fragmentation),
`storage/` (basic ops + one TTL test), and `zset/`. **Zero** tests exist
for `commands/`, `consensus/` (no election, no partition, no log-conflict
test despite being the most safety-critical module), `replication/`, or
`network/`. There is no `tests/` integration directory. This is the
starting point the rest of this document's requirements are measured
against — "add more tests" is not sufficient; the safety-critical layers
currently have none.

## Test categories

### Unit tests (per crate, run on every change)

- Hash slots / hash tags: CRC16 vectors against Redis, tag-extraction edge
  cases (reused/kept from prototype, extended per [[sharding]]).
- RESP parser/encoder: round-trip, fragmentation, pipelining (reused from
  prototype).
- Command validation: limits enforcement ([[protocol]]).
- Storage: data-type operations, TTL representation (**rewritten** to
  test explicit `expire_at` rather than `Instant`, per [[persistence]]).
- WAL: append/read/checksum/corruption-truncation ([[persistence]]).
- Snapshot: create/restore round-trip.
- Routing: MOVED/ASK decision logic given a slot table + migration state.
- State machine: determinism — same command sequence applied twice
  (fresh state each time) produces identical resulting state
  (property-tested, see below).

### Integration tests (`tests/`, new — none currently exist)

Minimum required clusters: single-node, three-node, five-node. Minimum
required scenarios, each as its own test:

```
SET -> GET (single node)
replication (3-node, write on leader, read on each follower via
  READ FROM REPLICA, verify eventual convergence)
leader election (kill leader, verify new leader within bounded time)
failover (kill leader, verify writes continue against new leader)
rejoin (restart killed node, verify it catches up to current commit index)
resharding (full PREPARING->COMPLETED cycle, verify data integrity)
snapshot recovery (write, snapshot, crash, restart, verify state)
TTL (set with EXPIRE, verify TTL/expiry consistent across leader,
  follower, after failover, after restart, after snapshot restore —
  Prompt.md section 22's explicit requirement)
```

### Failure-injection harness (`test-utils` crate, new)

Must be able to simulate, against real running node processes (not just
mocked components), per `Prompt.md` section 39:

```
node crash / node restart
message delay / drop / duplication / reordering
network partition / healing
slow disk
WAL failure (forced corruption/truncation injection)
```

Implementation approach: each simulated node runs as a real OS process
(not an in-process mock), so "crash" is an actual `SIGKILL`, "restart" is
actually re-executing the binary against the same data directory, and
network faults are applied at a proxy layer between nodes (each node's
peer connections routed through a controllable proxy process/library that
can inject delay/drop/duplicate/reorder/partition) rather than by
modifying the node binary itself — this way the tests exercise the real
network code path, not a special "test mode."

### Critical distributed tests (Prompt.md section 40, required, not
optional)

- Leader crash -> follower election -> write continues -> restarted
  original node catches up.
- Minority partition: isolated node cannot commit; majority continues;
  isolated node catches up on heal.
- Majority partition: majority side continues.
- Stale follower never wins an election with a log behind the committed
  quorum history (verified by deliberately constructing a lagging
  follower and attempting to force a vote).
- Repeated leader failures (kill new leader immediately after each
  election, several times in a row) still converges to a working cluster.
- Resharding + failure: kill source mid-transfer, kill target mid-catch-up,
  restart both, verify the cluster converges to a single valid ownership
  state for every slot (never zero or ambiguous owners).

### Property-based testing (proptest)

Required properties (Prompt.md section 41), each as an explicit
`proptest!` property, not just spot-checked examples:

- Hashing is deterministic (same key, same slot, always).
- RESP parser/encoder round-trip for arbitrary valid frames.
- State machine application is deterministic (same command sequence,
  fresh state, same result — see unit test above, promoted to property
  form with generated command sequences).
- WAL replay reproduces state (write random command sequence, replay from
  WAL, compare to the live state that produced it).
- Snapshot + WAL replay reproduces state (same, with an interposed
  snapshot at a random point).
- Committed operations are never lost (model-based: simulate a sequence
  of proposals/crashes/elections against a simplified model of the
  invariants and check no committed entry ever disappears).
- Replicas converge to identical committed state (simulate multiple
  replicas applying the same committed log, compare final state).

## Never weaken a test to make an implementation pass

Restated from `Prompt.md` section 2: if a test fails, the fix is either
in the implementation or (rarely, with explicit justification written into
this document and the relevant invariant) in the test's understanding of
the spec — never a silent loosening of an assertion to get a green run.
