# Performance

## Ordering: correctness first, measured second

Per `Prompt.md` sections 43-44, no scaling or throughput claim is made
without a benchmark producing it. This document defines what will be
measured and the backpressure/allocation rules that apply *before* any
optimization pass — it does not contain results yet, since Phase 0-9 must
land first.

## Backpressure (Prompt.md section 20, required before benchmarking is
meaningful)

Every queue between an async boundary and a slower consumer is bounded,
with an explicit, documented behavior when full:

- **Client request intake**: bounded per-connection buffering; a client
  sending faster than the server can dispatch experiences backpressure via
  normal TCP flow control (the server simply doesn't read more from the
  socket until it has processed/queued what it has, up to the bound) —
  never an unbounded `Vec`/channel absorbing arbitrary client-controlled
  volume.
- **Raft replication to a slow follower**: `openraft`'s own flow control
  governs this (it does not buffer unbounded log entries per follower);
  this project's job is to surface `replication_lag`
  ([[observability]]) so operators can see it, not to build a second
  buffering layer on top.
- **WAL write queue**: bounded channel between the async command-handling
  path and the (potentially blocking-on-disk) WAL writer task per group;
  full queue means new proposals for that group block/backpressure the
  client rather than accumulating unbounded pending writes in memory.
- **Internal event/metric channels**: bounded, with `try_send`-and-drop
  (with a dropped-count metric) preferred over blocking the hot path for
  non-critical telemetry.

No queue in the write or replication path may grow unbounded under
sustained overload; the system's response to overload is documented
rejection/backpressure, never silent unbounded memory growth
(`Prompt.md` section 21's `max_memory` + reject-writes-when-exceeded rule
applies here too).

## Benchmarks to run (Criterion + `scripts/bench.sh`, reusing/extending
the prototype's `benches/throughput.rs`)

```
SET throughput
GET throughput
mixed GET/SET
pipeline throughput (varying pipeline depth)
varying value sizes
varying shard counts
varying replica counts
leader vs. follower (READ FROM REPLICA) read latency
WAL sync=always vs. periodic vs. never
snapshot overhead (throughput during a snapshot vs. without)
```

## Redis comparison (only after correctness lands)

Equivalent workloads, same hardware, documented configuration for both
sides; report measured p50/p95/p99 latency, throughput, memory usage, and
pipeline behavior — never tuned selectively to favor one side
(`Prompt.md` section 44).

## Concurrency-model constraints that bound performance work

Restated from [[architecture]]: Tokio for networking, no blocking the
async runtime with WAL fsync or CPU-heavy command execution — these run
on dedicated tasks/threads per group, communicating via the bounded
channels above. Optimization work in Phase 11 operates within these
constraints (e.g., batching WAL appends, reducing serialization
allocations) rather than by relaxing them.
