# Cluster Operations Guide

This guide covers running and operating a loony-redis cluster, including setup, scaling, and fault recovery.

---

## Starting a Cluster

Every node must be told the full node list (`--cluster-nodes`) and its own address (`--cluster-self`). The node list must be identical on all nodes — the sort order determines slot assignment.

### 3-node cluster example

```bash
BIN=./target/release/loony-redis
NODES="127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481"

$BIN --port 6479 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6479 &
$BIN --port 6480 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6480 &
$BIN --port 6481 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6481 &
```

Each node logs its slot ownership on startup:
```
INFO Cluster mode: node 127.0.0.1:6479 owns slots [(0, 5460)]
INFO Cluster mode: node 127.0.0.1:6480 owns slots [(5461, 10922)]
INFO Cluster mode: node 127.0.0.1:6481 owns slots [(10923, 16383)]
```

---

## Hash Slots

The 16 384 slots are distributed evenly, sorted by node address. All nodes derive the same table independently — no coordination needed.

```
slot = CRC16(hash_tag(key)) % 16384
```

### Hash tags

Keys containing `{tag}` hash only the `tag` portion. This lets you co-locate related keys on one shard:

```
> CLUSTER KEYSLOT {user}.name
(integer) 5474
> CLUSTER KEYSLOT {user}.email
(integer) 5474    ← same slot → same node
```

Both keys will be on the same node regardless of which node you write to.

### Transparent proxying

Any node accepts any command. If the target key lives on a different node, the request is forwarded transparently:

```bash
# SET foo on node 0 — foo hashes to slot 12182 (node 2), proxied automatically
redis-cli -p 6479 SET foo bar
OK
```

Use `redis-cli -c` for cluster-aware mode (handles MOVED redirects), or rely on transparent proxying.

### CROSSSLOT

Multi-key commands (`DEL`, `MGET`, `MSET`) require all keys to hash to the same slot. Otherwise the server returns:
```
(error) CROSSSLOT Keys in request don't hash to the same slot
```

Use hash tags to co-locate keys that must be operated on together.

---

## Inspecting the Cluster

```bash
# Topology and slot ownership
redis-cli -p 6479 CLUSTER NODES

# Slot ranges per node
redis-cli -p 6479 CLUSTER SLOTS

# Overall cluster status
redis-cli -p 6479 CLUSTER INFO

# Which slot does a key land on?
redis-cli -p 6479 CLUSTER KEYSLOT mykey

# This node's ID
redis-cli -p 6479 CLUSTER MYID
```

---

## Adding a Node (CLUSTER MEET)

Start the new node with only itself in the node list:

```bash
./target/release/loony-redis --port 6482 \
    --cluster-nodes "127.0.0.1:6482" --cluster-self 127.0.0.1:6482 &
```

Then tell any existing node to admit it:

```bash
redis-cli -p 6479 CLUSTER MEET 127.0.0.1 6482
```

What happens:
1. The coordinator computes the new 4-node slot distribution.
2. It sends `CLUSTER SYNC` to the new node so it can accept incoming data.
3. It sends `CLUSTER SYNC` to all existing peers — each peer migrates its own outbound slots.
4. The coordinator migrates its own outbound slots.
5. All nodes commit the new routing table.

After MEET, check each node's new slot ownership:
```bash
redis-cli -p 6479 CLUSTER NODES
redis-cli -p 6482 CLUSTER NODES
```

Data integrity is maintained during migration — keys are atomically removed from the source and pushed to the target.

---

## Removing a Node (CLUSTER FORGET)

Get the ID of the node to remove:

```bash
redis-cli -p 6482 CLUSTER MYID
# "000000000000000000000000000000000000cafe"
```

Tell any other node to remove it:

```bash
redis-cli -p 6479 CLUSTER FORGET 000000000000000000000000000000000000cafe
```

What happens (in strict order to avoid proxy loops):
1. **This node's routing is updated first** — it can now accept keys for the departing node's slots.
2. **All surviving peers receive `CLUSTER SYNC`** — they update routing before any data arrives.
3. **The departing node receives `CLUSTER DRAIN`** — it pushes all its keys to the new owners (who now have correct routing).

After FORGET, the node list shrinks and slot distribution rebalances across the remaining nodes.

---

## Health Monitoring

When running in cluster mode, each node automatically:
- PINGs every peer every 500 ms (200 ms timeout).
- Tracks consecutive misses: 1–2 = `suspected`, 3+ = `failed` (≈1.5 s detection time).
- Sends gossip on each successful PING, propagating health state to all peers.

Check the health state of peers from any node:

```bash
redis-cli -p 6479 CLUSTER HEALTH
# "127.0.0.1:6480 alive
#  127.0.0.1:6481 alive"
```

States:
- `alive` — heartbeat responding normally.
- `suspected` — 1–2 consecutive missed heartbeats.
- `failed` — 3+ misses; auto-heal has been triggered.

---

## Automatic Fault Recovery (auto_heal)

When a peer is declared `failed`, each surviving node independently runs `auto_heal`:

1. Removes the failed node from the local routing table.
2. Broadcasts `CLUSTER SYNC` to remaining alive peers so they all converge on the new topology.

**Important**: data on the failed node is lost. The system redistributes slot *ownership* (routing), not the data. To protect against data loss, combine cluster sharding with per-shard replication (not yet implemented — future phase).

### Observing recovery

```bash
# Kill node 1
kill <node1_pid>

# Within ~1.5 s, surviving nodes log:
# WARN Peer 127.0.0.1:6480 failed (3 misses) — initiating auto-heal
# WARN auto_heal complete: 127.0.0.1:6480 removed, ~5461 slots redistributed

# Cluster continues serving requests on 2 nodes
redis-cli -p 6479 CLUSTER HEALTH
# "127.0.0.1:6481 alive"

redis-cli -p 6479 CLUSTER NODES
# (shows 2-node topology)
```

---

## Gossip Protocol

Health state propagates via gossip piggybacked on heartbeat PINGs:

- On each successful PING to peer A, this node sends `CLUSTER GOSSIP addr=state,...` to A.
- Gossip is **pessimistic**: state only worsens (`alive → suspected`, `alive → failed`, `suspected → failed`).
- A node returns to `alive` only via a direct successful heartbeat — gossip cannot resurrect it.

This prevents split-brain scenarios where stale gossip overrides accurate local observations.

---

## Combining Modes

Cluster sharding and Raft consensus are separate modes — you can run either independently. Combining them (Raft per shard) is a future phase.

| Mode | Flag | Use case |
|---|---|---|
| Standalone | _(no flags)_ | Single node, development |
| AOF | `--aof` | Crash recovery |
| Replication | `--replicaof` | Read scaling, data redundancy |
| Raft | `--raft-addr` + `--peers` | Automatic failover, strong consistency |
| Cluster | `--cluster-nodes` + `--cluster-self` | Horizontal sharding, dynamic membership |

---

## Logging

Set verbosity with the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug ./target/release/loony-redis --port 6479 ...
RUST_LOG=warn  ./target/release/loony-redis --port 6479 ...
```

Key log messages to watch:

| Level | Message | Meaning |
|---|---|---|
| INFO | `Cluster mode: node X owns slots [...]` | Startup, slot assignment confirmed |
| INFO | `MEET X: migrated N keys outbound` | Node added, data moved |
| INFO | `DRAIN: pushed N keys to new owners` | Node removed, data evacuated |
| WARN | `Peer X heartbeat missed (suspected)` | 1–2 missed heartbeats |
| WARN | `Peer X failed (3 misses) — initiating auto-heal` | Node declared dead |
| WARN | `auto_heal complete: X removed` | Cluster topology updated |
| WARN | `Replica lagged by N messages — disconnecting` | Replication follower falling behind |
