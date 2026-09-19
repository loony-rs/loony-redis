# Observability

## Baseline (reused from prototype)

`observability/mod.rs` already provides a real Prometheus `/metrics`
endpoint (uptime, connection counts, per-command counters, key counts,
replication commits, cluster slot/peer health) and a `/health` endpoint,
served over real HTTP — not stubbed. This is kept and extended, not
rewritten (see [[architecture]] reuse decision), but current coverage is
basic counters/gauges only: no latency histograms, and nothing
Raft-specific (term, commit lag, election count) since there was no
per-shard Raft to report on.

## Required metrics (Prompt.md section 31)

```
requests_total{command}
requests_failed_total{command, error_kind}

command_latency (histogram, per command)
command_duration (alias/consistent naming — pick one, document it)

raft_term{group_id}
raft_commit_index{group_id}
raft_applied_index{group_id}

raft_leader_changes_total{group_id}
raft_elections_total{group_id}

replication_lag{group_id, follower_node_id}

wal_bytes_written{group_id}
wal_fsync_duration (histogram, per group)

memory_used_bytes

connections_active

cluster_nodes
cluster_shards
```

`group_id` distinguishes each shard's Raft group and the metadata group —
required precisely because this system has many concurrent Raft groups
per process (see [[raft]]); a single unlabeled `raft_term` gauge would be
meaningless once more than one group exists in a process.

## Health endpoints

Per `Prompt.md` section 32, three distinct semantics, not conflated:

- **`/live`**: the process is running and its async runtime is
  responsive (e.g., can execute a trivial no-op task within a short
  timeout). Does not check any shard/Raft/disk state.
- **`/ready`**: the node has loaded its identity, completed recovery
  ([[recovery]]) for every local group, and is accepting client
  connections. A node that's still replaying its WAL on startup is live
  but not ready.
- **`/health`** (or `healthy`, naming TBD at implementation time, but the
  semantics are fixed): the node and the specific dependencies needed to
  actually serve traffic are functioning — e.g., its WAL is writable
  (not in a detected-corruption refusal state per [[failure-model]]), and
  for at least one shard it hosts, it can reach a quorum. A degraded node
  (e.g., every shard it participates in has lost quorum due to network
  issues on its side) reports unhealthy even though it is live and was
  ready before the degradation.

"TCP port is open" is never treated as equivalent to any of the above —
each endpoint does the specific check it claims to do.

## Admin / debugging (Prompt.md section 33)

Equivalent functionality to Redis Cluster's introspection commands, exact
naming decided at implementation time but must expose:

```
CLUSTER INFO    -- cluster_nodes, cluster_shards, this node's role
CLUSTER NODES   -- NodeId, address, role, health-as-last-observed
CLUSTER SLOTS   -- slot ranges -> shard -> current leader address
RAFT INFO       -- per group_id: term, leader, commit_index,
                   applied_index, replication_lag per follower
```

plus memory usage and WAL position (last written index, last fsynced
index) per group, for operational debugging without needing to scrape
Prometheus.

## Structured logging

`tracing`/`tracing-subscriber` (already a dependency, already used in the
prototype) remains the logging backend. Every log line touching a Raft
group, WAL operation, or membership change includes `group_id` and, where
applicable, `term`/`index`, so logs from multiple concurrent Raft groups
in one process can be filtered/correlated — a direct consequence of the
per-shard multi-Raft architecture in [[raft]].
