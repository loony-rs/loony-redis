# PLAN

This plan sequences the rebuild described in `docs/`. It follows
`Prompt.md` section 50's phase list, adapted to reflect the gap analysis
against the existing prototype (see `docs/architecture.md`'s "Starting
point" section): reuse what already meets the bar (RESP protocol, data
types, observability skeleton), rebuild what doesn't (consensus,
persistence, membership, resharding, TTL representation).

Each phase below only starts once the previous phase's acceptance criteria
are met and its tests pass. No phase is skipped. No phase's tests are
weakened to pass.

## Phase 0 — Architecture (this phase)

**Deliverables (done):**
- `docs/architecture.md`, `system-model.md`, `invariants.md`,
  `consistency.md`, `sharding.md`, `raft.md`, `replication.md`,
  `membership.md`, `resharding.md`, `persistence.md`, `recovery.md`,
  `protocol.md`, `failure-model.md`, `testing.md`, `observability.md`,
  `performance.md`.
- `docs/decisions/0001`-`0004` covering the Raft crate choice, the
  metadata-Raft-group split, MOVED/ASK vs. proxying, and the single
  write-path decision.
- This `PLAN.md`.

**Acceptance criteria:**
- Architecture is internally consistent (every cross-reference between
  docs resolves to a real section — checked by reading, not tooled).
- Raft-per-shard is defined, including how it coexists with the metadata
  group without becoming "one global group" (0002).
- Consistency semantics are explicit for both write and read paths,
  including the opt-in weaker replica-read mode.
- Persistence model is explicit: one authoritative WAL per group, no
  parallel AOF.
- Failure model is explicit and enumerates the required fault classes and
  test scenarios.

No implementation work beyond what already exists in the repo happens in
this phase.

## Phase 1 — Workspace restructuring + core storage (done)

**Goal:** stand up the target crate layout (`docs/architecture.md` module
map) and port the reusable storage layer into it, fixing the one
correctness defect found (TTL representation).

**Work done:**
- Converted the root `Cargo.toml` into a workspace (`crates/protocol`,
  `crates/storage`, plus the existing root package as a workspace member)
  — no stub crates created for later phases, per the "don't create a crate
  with nothing in it before it's needed" rule.
- Ported `src/storage/{mod,list,hash,set,zset}.rs` into `crates/storage`
  (`mod.rs` -> `lib.rs`), and `src/protocol/mod.rs` into `crates/protocol`
  unchanged (already correct — no fragmentation/pipelining issues found).
- Replaced `Instant`-based expiry with explicit `expire_at: Option<u64>`
  (epoch millis via a new `storage::now_ms()`), computed once by the
  caller rather than re-derived per replica. `Store::set`/`Store::expire`
  signatures changed accordingly; `Store::pttl`/`debug_object` updated to
  compute against `now_ms()` instead of `Instant`.
- This was a full rewire, not a parallel copy: `src/storage/` and
  `src/protocol/` were deleted from the root crate, and every consumer
  (`network`, `persistence`, `replication`, `consensus`, `cluster`,
  `observability`, `commands`, `main.rs`, `benches/throughput.rs`) now
  depends on the `storage`/`protocol` crates directly, with the `EXPIRE`/
  `PEXPIRE`/`SET EX/PX` command handlers in `commands/mod.rs` updated to
  compute `expire_at` once via `now_ms()` before calling into the store.
  There is exactly one implementation of storage/protocol in the tree, not
  two.

**Tests:** all existing storage (10) and protocol (11) unit tests pass
unchanged, plus two new/rewritten TTL tests:
`test_ttl_expiry` (rewritten to use an absolute past `expire_at` instead of
a nanosecond `Duration`) and `test_expire_at_is_explicit_not_recomputed_per_replica`
(two independent `Store`s given the identical `expire_at` value agree on
remaining TTL — the property that would have failed under the old
`Instant`-based design once real replication exists).

**Acceptance:** met. `cargo build --workspace` and `cargo test --workspace`
are clean (30 tests passing: 15 storage, 11 protocol, 4 root-crate cluster
tests — no network/consensus/Raft code involved yet, matching this
phase's intended scope). Pre-existing clippy warnings in the ported
storage code (`lower` dead code, `manual_retain` in `list::lrem`,
`zadd`'s argument count) and in the legacy `consensus`/`network`/
`commands` modules are left as-is: they predate this phase, and the
legacy modules they're in are slated for deletion/rebuild in Phases 2-4,
not a general cleanup target now.

## Phase 2 — RESP server (done)

**Goal:** a standalone TCP server crate (`crates/server`) that speaks RESP
against the ported storage crate directly (no sharding, no Raft yet —
this is deliberately the same shape as the old prototype's Phase 1-2, just
in the new crate layout), so the network/command-dispatch layer is
validated before consensus is layered underneath it.

**Work done:**
- New `crates/server` crate (lib + `loony-redis-server` bin), depending
  only on `protocol` and `storage` — no AOF/replication/Raft/cluster
  coupling. This is a fresh, narrowly-scoped implementation rather than a
  line-for-line port of `src/network/mod.rs` + `src/commands/mod.rs`:
  those files mix in AOF/replication/Raft/cluster concerns that belong to
  later phases, and the required v1 command list here
  (PING/GET/SET/DEL/LPUSH/RPUSH/LPOP/HSET/HGET/SADD/SMEMBERS/EXPIRE/TTL/
  INFO) is a small enough surface that reimplementing it cleanly against
  the new `Store` API (including the Phase 1 `expire_at: Option<u64>`
  change) was less risky than surgically extracting it from the legacy
  1989-line dispatcher. The legacy `src/network` + `src/commands` are left
  untouched and still build/run as the old monolithic binary; they get
  folded into/retired in favor of this crate once Raft/WAL/sharding give
  it real parity (Phases 3-9).
- Implemented the configurable limits from `docs/protocol.md`:
  `max_key_size`, `max_value_size`, `max_command_size`,
  `max_request_size`, `max_pipeline_depth`, `max_connections`. Oversized
  bulk-string lengths are rejected by peeking the RESP header
  (`max_declared_bulk_len`) before the body is buffered at all, not after
  — satisfies "no unbounded memory allocation based on client-controlled
  lengths" for the classic single-huge-value attack; `max_request_size`
  is the backstop for buffered-but-incomplete frames in general.
  `max_connections` is enforced at accept-time via an atomic counter with
  a `Drop`-based guard, so a connection always decrements it on close.

**Tests:** 10 tests in `crates/server`: the required command list
end-to-end, a fragmented-packet test (one command trickled in across many
1-byte-delayed writes), a pipelining test (three commands in one write,
batched responses), and five limit-enforcement tests (oversized value
rejected before the body arrives, oversized key rejected, pipeline-depth
cap enforced, plus two unit tests directly on the header-peeking scanner).

**Acceptance:** met — verified manually with real `redis-cli` against the
running `loony-redis-server` binary for the full required command list
(PING, SET/GET/DEL, LPUSH/RPUSH/LPOP, HSET/HGET, SADD/SMEMBERS,
EXPIRE/TTL, INFO), all correct. `cargo build --workspace` and
`cargo test --workspace` clean; `cargo clippy -p server --all-targets` has
zero warnings.

## Phase 3 — Single-node persistence (done)

**Goal:** replace the AOF with the real WAL described in
`docs/persistence.md`, without Raft yet (a WAL for a single-node "group of
one" is a useful, independently testable stepping stone toward Phase 4).

**Work done:**
- New `crates/persistence`, with three internal modules:
  - `wal.rs`: `Wal`/`WalRecord`/`SyncPolicy`. Record format is
    `magic|version|term|index|payload_len|payload|crc32c` exactly as
    specified in `docs/persistence.md`. `Wal::open` scans the file on
    disk (not a streaming reader -- simplicity over efficiency for now,
    revisit in Phase 11 if profiling says so), stops at the first
    incomplete-or-checksum-failing record, and truncates the file to end
    exactly at the last good record before returning. `append` honors
    `SyncPolicy::{Always, Periodic, Never}`. `compact_before` rewrites the
    file via temp-file-then-rename for log compaction after a snapshot.
  - `snapshot.rs`: `Snapshot`/`SnapshotMetadata`, saved/loaded via
    temp-file-then-rename plus a whole-payload crc32c checksum.
    `load_latest_snapshot` actually implements the next-older fallback
    docs/recovery.md requires on corruption, not just a comment about it.
  - `command.rs`: the `Command` enum from `Prompt.md` section 10
    (Set/Delete/Expire/ListPushLeft/ListPushRight/ListPopLeft/HashSet/
    SetAdd) plus a deterministic, panic-free `apply(store, cmd)`.
- `lib.rs` ties these into `recover` (snapshot load + WAL replay, the
  docs/recovery.md startup sequence minus the Raft-specific steps),
  `propose` (append + apply, the pre-Raft "quorum of one" write path), and
  `snapshot_and_compact`.
- Extended `storage::Store` with `KeyEntry`/`snapshot_entries`/
  `restore_entries` so snapshots carry each key's `expire_at` -- the
  legacy `SnapshotEntry` (still used by the old async-replication
  full-sync path) doesn't carry TTL at all, and fixing that gap directly
  in this phase's new snapshot format (rather than leaving it) was
  required by docs/persistence.md's explicit "snapshot must capture...
  TTL information". `storage::Value` and its component types
  (List/Hash/Set/ZSet) gained `serde` derives to make this possible.

**Tests:** 15 tests in `crates/persistence`, including the required
`write -> snapshot -> crash -> restart -> recover -> verify` sequence,
corrupt-tail truncation (both a torn/incomplete record and a checksum
mismatch on an otherwise-complete record), WAL compaction correctness,
snapshot corruption fallback to the next-older snapshot, and an explicit
R3 test (snapshot partway through a command sequence + WAL replay
produces the same state as continuous application with no snapshot at
all). Plus 2 tests on `Command::apply`'s determinism (including the
WRONGTYPE-is-deterministic-not-a-panic case).

**Acceptance:** met for the single-node scope this phase covers (the
Raft-driven parts of `docs/recovery.md`'s test matrix -- leader vs.
follower crash specifically -- wait for Phase 4, since there's no leader/
follower distinction without Raft yet). `cargo build/test --workspace`
clean (55 tests total), zero clippy warnings introduced by this phase.

## Phase 4 — Raft (single shard) (done)

**Goal:** integrate `openraft` (decision 0001) for one Raft group, wire
`RaftLogStorage`/`RaftStateMachine` to the Phase 3 WAL and Phase 1 storage,
`RaftNetwork` over internal TCP.

**Work done:**
- New `crates/raft`, depending on `openraft = "0.9"` (`serde` +
  `storage-v2` features — v2 is the current, non-deprecated split-trait
  API; `storage-v2` also unseals `RaftLogStorage`/`RaftStateMachine` for
  a third-party impl like this one). `TypeConfig` declared via
  `openraft::declare_raft_types!` with `D = persistence::Command`,
  `R = ()`, `NodeId = u64`, `Node = openraft::BasicNode`.
- `LogStore` (`RaftLogStorage` + `RaftLogReader`): entries are served from
  an in-memory `BTreeMap<index, Vec<u8>>` rebuilt from `persistence::Wal`
  on open; writes go through the same `Wal`, with an explicit `flush()`
  before invoking openraft's `LogFlushed` callback so the callback's
  durability contract is honored regardless of `Wal`'s own fsync policy.
  `Wal` gained a new `truncate_from` (discard a conflicting suffix) to
  pair with Phase 3's `compact_before` (discard a covered prefix) —
  `truncate`/`purge` map onto these directly. `vote` and the
  purged-log-id boundary each get their own tiny checksummed file
  (`crate::blob`) since WAL records have no slot for them.
- `StateMachineStore` (`RaftStateMachine` + `RaftSnapshotBuilder`, on
  `Arc<StateMachineStore>` per openraft's own reference pattern so the
  RESP server can later hold a clone for direct leader reads without
  going through Raft): `apply` calls the exact `persistence::Command`
  apply function Phase 3 already built and tested. Snapshots are a new,
  small format specific to this crate (openraft's `SnapshotMeta` plus a
  bincode-serialized `Vec<storage::KeyEntry>`), not a reuse of
  `persistence::Snapshot` (that format doesn't carry openraft's
  membership/log-id metadata).
- `Network`/`Connection` (`RaftNetworkFactory` + `RaftNetwork`): real TCP,
  a fresh connection per RPC, length-prefixed bincode frames. Includes
  `PartitionControl`, a shared blocked-link table gating outbound
  connection attempts — the mechanism this phase's partition tests use to
  simulate a cut without building Phase 8's full fault-injection harness;
  a blocked link fails as `Unreachable`, same as a real dead peer.
- The legacy `src/consensus/mod.rs` (hand-rolled, in-memory-only Raft) is
  **not yet deleted** — it still backs the old prototype binary, which
  remains untouched, matching how Phase 2 left `src/network`/
  `src/commands` alone. It gets retired once a later phase gives
  `crates/server` real parity (sharding + this Raft crate wired
  together) and the old binary is finally replaced, not before.

**Tests:** 7 unit tests (`crates/raft/src`: WAL-backed log
append/read/reopen, truncate-conflicting-suffix, purge-and-reopen, vote
persistence; state-machine apply, snapshot build/install round-trip,
snapshot-survives-reopen) plus 4 real integration tests
(`crates/raft/tests/cluster.rs`), each a genuine 3-node cluster over real
localhost TCP with on-disk WAL/snapshot per node:
`test_three_node_replication`, `test_leader_crash_election_continues_and_rejoin_catches_up`
(kill -> new election -> continued writes -> rejoin at the same address
and disk -> catch-up), `test_minority_partition_isolated_leader_cannot_commit`,
`test_majority_partition_continues_without_isolated_follower`. Verified
stable across 5 repeated runs (no flakiness observed).

**Acceptance:** met for a single shard. `cargo build/test --workspace`
clean (67 tests total), zero clippy warnings on the new crate.

## Phase 5 — Sharding (done)

**Goal:** slot table, CRC16/hash-tag routing (ported, already correct),
slot -> shard mapping, MOVED replies (decision 0003 — no proxying).

**Work done:**
- New `crates/cluster`: `slots.rs` ports the prototype's CRC16-CCITT +
  hash-tag extraction unchanged (it was already correct); `routing.rs`
  ports `CommandSlot`/`command_slot` (per-command key extraction) from
  the prototype too, but replaces the prototype's address-based
  `ClusterConfig` and its transparent `proxy_to` with a plain
  `SlotTable` (`slot -> (ShardId, leader_addr)`) and a `route()` decision
  function returning `Local`/`Moved{slot, leader_addr}`/`CrossSlot` —
  no server-side proxying (decision 0003).
- `SlotTable` is deliberately not yet the consensus-backed `ClusterState`
  from docs/membership.md (that's Phase 7): it's a directly-constructed
  map, and an **unset** slot defaults to `Local` rather than an error,
  since there's no other authoritative source yet. Only an explicit
  entry pointing at a different shard produces a `MOVED`.
- Wired into `crates/server`: `Server::with_cluster_routing(my_shard,
  slot_table)` is a builder method (routing is a no-op by default via
  plain `Server::new`, unaffected). `dispatch` checks routing before
  executing and returns `-MOVED`/`-CROSSSLOT` directly, never proxies.

**Tests:** 17 in `crates/cluster` — the ported CRC16 test vectors and
hash-tag cases, 3 proptest properties (slot hashing is deterministic,
always in range, matching hash tags colocate), and unit tests for
`command_slot`/`route` against a synthetic multi-shard `SlotTable`. Plus
3 new end-to-end tests in `crates/server` using a real TCP client against
a routed server: `-MOVED` for a slot a fake second shard owns, local
handling for a slot with no recorded owner, and `-CROSSSLOT` for a
multi-key command spanning slots.

**Acceptance:** met. `cargo build/test --workspace` clean (84 tests
total), no new clippy warnings.

## Phase 6 — Multi-shard replication

**Goal:** multiple concurrent shard Raft groups in one process/cluster,
each independently leading and committing.

**Work:** generalize Phase 4's single-group wiring to N groups
(`docs/raft.md`'s "one Raft group per shard" section becomes real here).

**Tests:** parallel-write test (writes to two different shards proceed
independently; killing shard A's leader doesn't affect shard B's
availability).

**Acceptance:** N-shard cluster where a single-shard failure is
demonstrably isolated (per-shard test from `docs/testing.md`).

## Phase 7 — Cluster membership

**Goal:** the metadata Raft group (decision 0002): node identity, JOIN/
LEAVE/REJOIN, consensus-backed `ClusterState`.

**Work:** per `docs/membership.md`. Replaces the prototype's
`address-derived node_id` and gossip-only `ClusterConfig`/`auto_heal`.

**Tests:** join a new node, verify it's visible cluster-wide via the
metadata group's committed state (not just locally); leave/rejoin;
verify no unilateral `auto_heal`-style membership change without a
committed metadata-group entry.

**Acceptance:** membership changes are observably consensus-backed (a
test that partitions the metadata group's minority side and confirms it
cannot commit a membership change).

## Phase 8 — Failover

**Goal:** end-to-end failure detection -> election -> client redirection
-> recovery -> catch-up, using the failure-injection harness from
`docs/testing.md`.

**Work:** build `crates/test-utils` (process-level crash/restart, network
fault proxy) — does not exist in the prototype at all.

**Tests:** the full "critical distributed tests" list in
`docs/testing.md`: leader crash, minority/majority partition, stale
follower, repeated failures.

**Acceptance:** every scenario in `docs/testing.md`'s critical-tests
section passes against a real multi-process cluster, not a mocked one.

## Phase 9 — Online resharding

**Goal:** the full PREPARING->TRANSFERRING->CATCHING_UP->CUTOVER->
COMPLETED state machine from `docs/resharding.md`, replacing the
prototype's unstaged copy-then-flip.

**Tests:** full migration cycle with continuous reads/writes against the
migrating range throughout; kill-source-mid-transfer and
kill-target-mid-catch-up scenarios with verified convergence.

**Acceptance:** section 51's resharding-related acceptance steps (13-16)
pass.

## Phase 10 — Observability

**Goal:** extend the reused Prometheus/`/health` skeleton with the full
metric set and the three distinct health-endpoint semantics in
`docs/observability.md`, plus `CLUSTER INFO`/`NODES`/`SLOTS`/`RAFT INFO`.

**Acceptance:** every metric listed in `docs/observability.md` is
scrapeable; `/live`, `/ready`, `/health` are distinguishable in a test
that puts the node in each relevant state.

## Phase 11 — Performance

**Goal:** run the benchmark suite in `docs/performance.md`, only after
Phases 1-10 are correct. Optimize based on profiling, not guesses.

**Acceptance:** benchmark numbers are recorded (not claimed without
measurement), covering every workload listed in `docs/performance.md`.

## Acceptance test (Prompt.md section 51)

Run in full only after Phase 9. This is the project's definition of done
for v1 and is not considered satisfied by any subset of it passing.
