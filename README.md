# loony-redis

A Redis-compatible, distributed, fault-tolerant in-memory key-value store written entirely in Rust.

Built from scratch across 10 engineering phases — from a single-node KV engine to a fully distributed cluster with Raft consensus, dynamic membership, automatic fault recovery, a complete Redis data-structure surface, and production-grade observability.

## Features

| Capability | Details |
|---|---|
| **Redis RESP protocol** | Compatible with `redis-cli` and any Redis client |
| **Data types** | Strings, Lists, Hashes, Sets, Sorted Sets (ZSet) |
| **Compact encoding** | Listpack-equivalent small-collection encoding; auto-promotes at 128 entries / 64-byte values |
| **Persistence** | Append-Only File (AOF) with crash recovery |
| **Replication** | Async leader–follower streaming with full snapshot sync |
| **Consensus** | Raft-based leader election and log replication |
| **Clustering** | 16 384 hash slots, transparent proxying, hash tags |
| **Dynamic membership** | `CLUSTER MEET` / `CLUSTER FORGET` with live data migration |
| **Fault detection** | Heartbeat + gossip, automatic slot redistribution on failure |
| **TTL** | Per-key expiry with `EX`, `PX`, `EXPIRE`, `PEXPIRE` |
| **Observability** | Prometheus metrics, `/health`, `/ready` endpoints; `INFO` and `DEBUG` commands |

## Quick Start

### Build

```bash
cargo build --release
```

### Single node

```bash
./target/release/loony-redis --port 6379
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

### With persistence (AOF)

```bash
./target/release/loony-redis --port 6379 --aof /var/data/loony.aof
```

Data survives restarts — the AOF is replayed automatically on startup.

### Leader–follower replication

```bash
# Leader
./target/release/loony-redis --port 6379

# Followers
./target/release/loony-redis --port 6380 --replicaof 127.0.0.1:6379
./target/release/loony-redis --port 6381 --replicaof 127.0.0.1:6379
```

Followers connect, receive a full snapshot, then stream live writes.

### Raft consensus cluster (automatic failover)

```bash
./target/release/loony-redis --port 7500 --raft-addr 127.0.0.1:8500 \
    --peers 127.0.0.1:8501,127.0.0.1:8502

./target/release/loony-redis --port 7501 --raft-addr 127.0.0.1:8501 \
    --peers 127.0.0.1:8500,127.0.0.1:8502

./target/release/loony-redis --port 7502 --raft-addr 127.0.0.1:8502 \
    --peers 127.0.0.1:8500,127.0.0.1:8501
```

Kill any node — the remaining two elect a new leader within ~300 ms.

### Sharded cluster (hash-slot distribution)

```bash
NODES="127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481"

./target/release/loony-redis --port 6479 \
    --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6479 &

./target/release/loony-redis --port 6480 \
    --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6480 &

./target/release/loony-redis --port 6481 \
    --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6481 &
```

Use `redis-cli -c -p 6479` (cluster mode) or any port — requests are transparently proxied to the correct shard.

## CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `6379` | Client TCP port |
| `--aof` | _(disabled)_ | Path to Append-Only File |
| `--replicaof` | _(disabled)_ | `host:port` of leader to replicate from |
| `--raft-addr` | _(disabled)_ | This node's Raft address, e.g. `127.0.0.1:8379` |
| `--peers` | _(disabled)_ | Comma-separated Raft peer addresses |
| `--cluster-nodes` | _(disabled)_ | All cluster node addresses (comma-separated) |
| `--cluster-self` | _(disabled)_ | This node's own address (must appear in `--cluster-nodes`) |
| `--metrics-addr` | `127.0.0.1:9090` | Address for the Prometheus + health HTTP server |
| `--no-metrics` | _(off)_ | Disable the metrics HTTP server |

Logging verbosity is controlled by the `RUST_LOG` environment variable (default: `info`).

## Supported Commands

### Connection
`PING`, `ECHO`, `QUIT`, `SELECT`, `CLIENT`, `COMMAND`, `CONFIG`

### Observability
`INFO` _(server / stats / replication / cluster / keyspace / all)_, `DEBUG` _(object / sleep / cluster / health)_

### Keyspace
`DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`

### Strings
`GET`, `SET` _(EX / PX / NX / XX)_, `MGET`, `MSET`, `STRLEN`, `APPEND`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `GETSET`, `SETNX`

### Lists
`LPUSH`, `RPUSH`, `LPOP` _(count)_, `RPOP` _(count)_, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LMOVE`

### Hashes
`HSET`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HLEN`, `HEXISTS`, `HKEYS`, `HVALS`, `HSETNX`, `HINCRBY`, `HINCRBYFLOAT`

### Sets
`SADD`, `SMEMBERS`, `SISMEMBER`, `SREM`, `SCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`

### Sorted Sets
`ZADD` _(NX/XX/GT/LT/CH)_, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZREM`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`

### Cluster
`CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER MYID`, `CLUSTER KEYSLOT`, `CLUSTER MEET`, `CLUSTER FORGET`, `CLUSTER HEALTH`, `MIGRATE`

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                      Client (redis-cli)                  │
└─────────────────────────┬───────────────────────────────┘
                          │ RESP over TCP
┌─────────────────────────▼───────────────────────────────┐
│  network/   TCP server · connection handler · follower   │
│             loop · heartbeat task                        │
└──────┬──────────────────┬──────────────────┬────────────┘
       │                  │                  │
┌──────▼──────┐  ┌────────▼───────┐  ┌──────▼───────────┐
│ commands/   │  │  consensus/    │  │   cluster/       │
│ dispatch ·  │  │  Raft engine · │  │  slot routing ·  │
│ CRUD ·      │  │  leader elect  │  │  membership ·    │
│ CLUSTER     │  └────────────────┘  │  heartbeat ·     │
│ subcommands │                      │  gossip           │
└──────┬──────┘                      └──────────────────┘
       │
┌──────▼─────────────────────────────────────────────────┐
│  storage/   DashMap · String · List · Hash · Set · TTL  │
└──────┬─────────────────────────────────────────────────┘
       │
┌──────▼──────────────────────────────────────────────────┐
│  persistence/   AOF writer · frame loader                │
└─────────────────────────────────────────────────────────┘
```

See [docs/Architecture.md](docs/Architecture.md) for full design details.

## Running Tests

```bash
cargo test
```

29 unit tests covering CRC16 vectors, hash tags, slot distribution, cluster config, ZSet score ordering, rank/range queries, compact encoding, set algebra, and list mutations.

## Project Layout

```
src/
  main.rs            CLI argument parsing, startup sequencing
  network/           Async TCP server, connection handler, replication stream
  commands/          Command dispatcher, all command implementations
  storage/
    mod.rs           Store, Value, SnapshotEntry, all store methods
    list.rs          List — compact Vec / VecDeque dual encoding
    hash.rs          Hash — compact Vec-of-pairs / HashMap dual encoding
    set.rs           Set — compact Vec / HashSet dual encoding + algebra
    zset.rs          ZSet — BTreeMap score index + HashMap member→score
  protocol/          RESP parser and serializer
  persistence/       AOF write-ahead log, replay on startup
  replication/       Leader broadcast channel, follower sync loop
  consensus/         Raft state machine, leader election, log replication
  cluster/           Hash slots, routing, migration, heartbeat, gossip
  observability/     Metrics (Prometheus), /health, /ready endpoints
docs/
  Architecture.md    Deep-dive design and trade-off notes
  ClusterGuide.md    Cluster operations: meet, forget, health, fault recovery
  Commands.md        Full command reference with examples
scripts/
  test.sh            End-to-end integration test script
```

## Design Highlights

- **No unsafe Rust** — all concurrency through `Arc`, `tokio::sync::RwLock`, and `DashMap`.
- **Compact encoding** — small collections use flat `Vec`-based storage matching Redis's listpack layout; automatically promoted to `VecDeque` / `HashMap` / `HashSet` when either the entry count (128) or element size (64 bytes) threshold is exceeded.
- **ZSet dual index** — `HashMap<member→score>` for O(1) score lookup; `BTreeMap<ScoreKey,()>` for O(log n) rank and range queries. `ScoreKey` reinterprets `f64` bits as `u64` with a sign-magnitude fix so the unsigned integer ordering matches float ordering exactly.
- **Deterministic routing** — every node independently derives the same slot table from a sorted node list; no gossip round-trip needed for routing decisions.
- **Transparent proxying** — ordinary `redis-cli` works against any shard without `MOVED` handling.
- **Gossip is pessimistic** — node state only moves toward `Failed` via gossip; recovery requires a direct successful heartbeat.
- **Ordering invariant in FORGET** — routing tables are updated on all surviving nodes *before* triggering the departing node's drain, preventing proxy loops.
- **Zero-dependency metrics** — Prometheus text format is emitted by a hand-rolled HTTP listener (tokio + std atomics + DashMap); no heavy observability crate is added to the dependency tree.
- **DEBUG as a live wire** — `DEBUG CLUSTER` and `DEBUG HEALTH` dump the in-memory routing table and peer health table at any moment without restarting the node.
