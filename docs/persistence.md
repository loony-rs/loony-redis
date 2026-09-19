# Persistence

## One authoritative log per Raft group

The prototype had two independent logs: the Raft group's in-memory-only
`Vec<LogEntry>` (`consensus/mod.rs`, lost on restart) and a separate
on-disk AOF (`persistence/mod.rs`, a command log with no checksums, no
term/index fields, unconditional flush, no configurable fsync, no
snapshotting). Per `Prompt.md` section 23 ("Do not create two independent
authoritative logs"), this is collapsed into one: **the WAL is the Raft
log's on-disk representation**, accessed exclusively through the
`RaftLogStorage` trait implementation described in [[raft]]. There is no
separate AOF.

```
Raft command (proposed)
      |
      v
   WAL (append, this IS the Raft log's persistence)
      |
      v
Raft commit (quorum durable-append acknowledged)
      |
      v
State machine apply (in-memory store mutation)
```

Every shard's Raft group and the metadata group each have their own WAL
file(s) under `<data_dir>/<group_id>/wal/`.

## WAL record format

Each record contains, in order, with a fixed header:

```
magic/version (u8)
term (u64)
index (u64)             -- monotonically increasing per group
payload_len (u32)
payload (bincode/postcard-encoded Command, or membership-change entry)
crc32c checksum (u32)    -- over term+index+payload_len+payload
```

- **Checksum**: CRC32C over the whole record body. A checksum mismatch on
  read means corruption; see recovery handling in [[recovery]] and
  [[failure-model]].
- **Append**: strictly increasing `index` per group, one writer (the
  group's own Raft-driven storage impl) — no concurrent writers to one
  group's WAL, so no write-write races to reason about.
- **Truncation**: supported for two cases only — (a) log compaction after
  a snapshot (removing a contiguous prefix `<= snapshotted index`), and
  (b) discarding a divergent suffix when a follower's log conflicts with
  a new leader's (standard Raft log-conflict resolution, truncate then
  re-append the leader's entries). Never used to "edit" committed history.

## Fsync policy

Configurable per `Prompt.md` section 46, three modes:

- **`sync = always`** (default, safest): `fsync()` after every WAL append,
  before acknowledging the write up the stack to Raft's commit-tracking.
  Guarantees R1 in [[invariants]] with the tightest bound: a crash right
  after ack means the record is on disk. Highest write latency.
- **`sync = periodic`**: batches appends and calls `fsync()` on a timer
  (configurable interval). A crash between fsyncs can lose *appended*
  (not yet flushed) records — but per R4 in [[invariants]], those are, by
  construction, records this node has not yet told anyone (peers or
  client) are durable, since "durable" for quorum-commit purposes is
  defined as "fsynced," so this cannot violate R1 as long as the
  Raft-commit-tracking code only counts a follower's ack after its own
  fsync completes, not after the in-memory append. This mode trades
  worst-case recovery window (up to one interval of committed-looking
  work) for throughput — document this trade-off wherever the config
  option is exposed, don't hide it.
- **`sync = never`**: relies on OS page cache flush only. Explicitly the
  least safe: a process crash (not just node crash) can lose records the
  OS hadn't flushed yet, with no bound. Intended for throughput
  benchmarking and non-durable test scenarios only; the docs and
  `--sync=never` CLI help text must say so plainly.

Default is `always`, matching `Prompt.md` section 24 ("default to the
safest reasonable production behavior").

## Snapshots

A snapshot for a shard group captures:
- every key's value and its `expire_at` (explicit epoch-ms, per S5 in
  [[invariants]] — never re-derived from `Instant`);
- the Raft snapshot metadata `openraft` requires (last included
  term/index, membership configuration at that point);
- for the metadata group's own snapshots: the full `ClusterState` (node
  list, slot table, in-flight `SlotMigration` records).

Snapshot creation reads a consistent point-in-time view of the in-memory
store (via a copy-on-write style pass or a brief consistent pause of new
applies — implementation detail decided during Phase 3/4, but it must
never observe a partially-applied command) and writes it to a new file
under `<data_dir>/<group_id>/snapshots/`, then atomically (rename) makes
it the current snapshot before any WAL truncation happens referencing it.
Old snapshots are retained until the new one's rename succeeds, so a
crash mid-snapshot never leaves the node without a usable snapshot.

## Required persistence test (Prompt.md section 25)

```
write data -> snapshot -> crash -> restart -> restore snapshot
  -> replay WAL tail -> verify state matches pre-crash committed state
```

This is a required integration test (see [[testing]]), not optional.
