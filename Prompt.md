# Distributed Rust Key-Value Database — Master Engineering Prompt

You are a **principal distributed-systems engineer, Rust systems programmer, and database engineer**.

Your task is to design and implement a **production-quality distributed in-memory key-value database written entirely in Rust**, inspired by Redis and Redis Cluster, but with its own clean architecture and implementation.

The project must prioritize:

> **Correctness → deterministic behavior → fault tolerance → testability → observability → performance**

Do not optimize prematurely.

Do not implement features merely to satisfy a checklist.

Do not produce pseudocode where working code is required.

Do not claim a distributed guarantee unless the implementation and tests actually establish it.

---

# 1. PRIMARY OBJECTIVE

Build a horizontally scalable distributed in-memory key-value database with:

* sharding
* replication
* per-shard consensus
* automatic leader election
* automatic failover
* dynamic membership
* online resharding
* crash recovery
* durable write-ahead logging
* snapshots
* Redis-inspired RESP2 client protocol
* observability
* fault injection
* deterministic integration testing

The system must be capable of running locally as a multi-node cluster.

The initial target deployment is:

```text
3–9 nodes
multiple shards
3 replicas per shard
1 leader + 2 followers per shard
```

The architecture must be capable of scaling beyond this without requiring a fundamental redesign.

---

# 2. IMPORTANT ENGINEERING RULE

Do NOT attempt to implement the entire system in one pass.

Work in explicit phases.

Before implementing a phase:

1. inspect the repository
2. understand existing code
3. identify dependencies
4. write/update design documentation
5. define invariants
6. define tests
7. implement
8. run formatting
9. run compilation
10. run unit tests
11. run integration tests
12. run failure tests where applicable
13. only then proceed

Never silently skip failing tests.

Never weaken tests simply to make the implementation pass.

Never replace a correct design with a simpler incorrect implementation merely to satisfy a feature requirement.

---

# 3. REQUIRED FIRST ACTION

Before writing substantial implementation code, create:

```text
docs/
├── architecture.md
├── system-model.md
├── invariants.md
├── consistency.md
├── sharding.md
├── raft.md
├── replication.md
├── membership.md
├── resharding.md
├── persistence.md
├── recovery.md
├── protocol.md
├── failure-model.md
├── testing.md
├── observability.md
└── performance.md
```

Also create:

```text
PLAN.md
```

The plan must contain implementation phases and acceptance criteria.

Do not begin large-scale implementation until the architecture and invariants are coherent.

---

# 4. SYSTEM MODEL

Assume the following failure model:

### Supported failures

* process crash
* machine/node crash
* node restart
* network delay
* network packet loss
* network packet duplication
* network message reordering
* temporary network partition
* permanent node loss
* disk/WAL failure detection
* leader failure
* follower failure

### Not supported

Byzantine behavior is NOT required.

Nodes are trusted members of the cluster.

Document this explicitly.

---

# 5. FAULT TOLERANCE MODEL

Default configuration:

```text
replication_factor = 3
```

A three-replica shard must tolerate:

```text
1 replica failure
```

while maintaining availability for writes, assuming the remaining replicas form a quorum.

Define quorum as:

```text
quorum = floor(replication_factor / 2) + 1
```

For RF=3:

```text
quorum = 2
```

Never acknowledge a strongly consistent write unless the appropriate consensus conditions have been satisfied.

---

# 6. CORE ARCHITECTURE

Use a layered architecture:

```text
Client
  |
  v
RESP Server
  |
  v
Command Parser / Validator
  |
  v
Cluster Router
  |
  v
Slot
  |
  v
Shard / Raft Group
  |
  v
State Machine
  |
  +----> In-memory storage
  |
  +----> WAL
  |
  +----> Snapshot
```

Separate the control plane:

```text
Cluster Control Plane

├── Node identity
├── Membership
├── Cluster configuration
├── Shard placement
├── Replica placement
├── Slot ownership
└── Resharding coordination
```

from the data plane:

```text
Data Plane

├── Client requests
├── Routing
├── Raft replication
├── State machine execution
├── WAL
└── Snapshot/recovery
```

Do not tightly couple cluster membership with the data storage implementation.

---

# 7. SHARDING MODEL

Use **fixed hash slots**.

Use:

```text
16384 slots
```

Keys map to slots using a Redis-compatible approach:

```text
key
 ↓
CRC16
 ↓
CRC16(key) % 16384
 ↓
slot
```

Implement Redis-style hash tags:

```text
{user123}:profile
{user123}:sessions
```

Both must map to the same slot.

The hashing implementation must be deterministic and thoroughly tested.

---

# 8. SHARD MODEL

Each shard is an independent replicated state machine.

Conceptually:

```text
Shard 0
 ├── Leader
 ├── Follower
 └── Follower

Shard 1
 ├── Leader
 ├── Follower
 └── Follower

Shard 2
 ├── Leader
 ├── Follower
 └── Follower
```

Do NOT implement a single global Raft group for the entire database.

Each shard must have an independent Raft group.

This allows:

* independent leadership
* independent replication
* parallel writes
* horizontal scaling
* localized failures
* localized consensus

---

# 9. RAFT REQUIREMENTS

Implement or use a well-tested Rust Raft implementation where appropriate.

Do not invent a subtly incorrect consensus algorithm merely to avoid dependencies.

If implementing Raft yourself, document the implementation carefully.

Raft must provide:

* leader election
* terms
* candidate state
* follower state
* leader state
* log replication
* commit index
* applied index
* election timeout
* heartbeat
* leader step-down
* quorum semantics
* membership changes
* snapshot installation
* log compaction
* recovery after restart

The state machine must apply commands only after they are committed.

---

# 10. STATE MACHINE

Raft replicates **commands**, not arbitrary memory mutations.

Example:

```rust
enum Command {
    Set {
        key: Bytes,
        value: Bytes,
        expire_at: Option<u64>,
    },

    Delete {
        key: Bytes,
    },

    ListPushLeft {
        key: Bytes,
        values: Vec<Bytes>,
    },

    ListPushRight {
        key: Bytes,
        values: Vec<Bytes>,
    },

    ListPopLeft {
        key: Bytes,
    },

    HashSet {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },

    SetAdd {
        key: Bytes,
        members: Vec<Bytes>,
    },
}
```

Commands must be deterministic.

Never allow local wall-clock decisions, random numbers, or nondeterministic iteration to change replicated state.

If a timestamp is required, put the timestamp explicitly into the replicated command.

---

# 11. CONSISTENCY MODEL

The system must have clearly documented consistency semantics.

Default mode:

```text
STRONG / LINEARIZABLE
```

For writes:

```text
client
 ↓
leader
 ↓
append Raft entry
 ↓
replicate to quorum
 ↓
commit
 ↓
apply state machine
 ↓
ack client
```

A successful write must not be acknowledged before the required consensus conditions are met.

Reads should default to leader reads.

Document exactly what guarantees exist for:

* GET after SET
* concurrent SET
* GET during leader failure
* GET during network partition
* stale followers
* failover
* node restart

---

# 12. OPTIONAL EVENTUAL-CONSISTENCY READS

Do NOT implement vague "eventual consistency."

If replica reads are supported, explicitly expose them as a separate behavior.

For example:

```text
READ FROM LEADER
READ FROM REPLICA
```

Replica reads may be stale.

Document:

* maximum guarantees
* possible staleness
* behavior during partitions
* whether monotonic reads are guaranteed

Do not imply linearizability for replica reads.

---

# 13. MULTI-KEY OPERATIONS

Initially support multi-key operations only when all keys map to the same shard.

For example:

```text
{user123}:profile
{user123}:sessions
```

may participate in one shard-local transaction.

Cross-shard transactions are NOT required for the initial implementation.

Do not implement distributed transactions unless explicitly requested later.

---

# 14. RESHARDING

Implement online slot migration.

A slot migration must have explicit states.

Example:

```text
PREPARING
    ↓
TRANSFERRING
    ↓
CATCHING_UP
    ↓
CUTOVER
    ↓
COMPLETED
```

During migration:

1. identify source shard
2. identify target shard
3. establish migration metadata
4. transfer existing data
5. track mutations occurring during migration
6. synchronize target
7. perform ownership cutover
8. update cluster metadata
9. redirect future requests
10. verify convergence

Define behavior for:

* GET during migration
* SET during migration
* DELETE during migration
* client retries
* source failure
* target failure
* migration interruption
* migration restart

Do not implement resharding as a simple "copy all keys and change ownership" operation.

---

# 15. CLIENT ROUTING

Implement Redis-inspired cluster routing.

The server may return:

```text
-MOVED <slot> <host>:<port>
```

when the request reaches the wrong owner.

During migration, support:

```text
-ASK <slot> <host>:<port>
```

if appropriate.

Document the routing semantics.

A client must be able to discover the current slot owner.

Provide a minimal cluster-aware client/test utility if needed for integration testing.

---

# 16. RESP PROTOCOL

Implement a clearly defined **RESP2-compatible subset**.

Required commands:

```text
PING

GET
SET
DEL

LPUSH
RPUSH
LPOP

HSET
HGET

SADD
SMEMBERS

EXPIRE
TTL

INFO
```

Support:

* simple strings
* errors
* integers
* bulk strings
* arrays
* null bulk strings
* pipelining

The parser must be incremental and asynchronous.

It must correctly handle:

* fragmented packets
* multiple commands in one TCP packet
* large requests
* malformed requests
* connection termination
* pipelined commands

Do not use `read()` boundaries as protocol message boundaries.

---

# 17. COMMAND LIMITS

Implement configurable limits:

```text
max_key_size
max_value_size
max_command_size
max_request_size
max_pipeline_depth
max_connections
max_memory
```

Malformed or oversized requests must be rejected safely.

No unbounded memory allocation based on client-controlled lengths.

---

# 18. DATA TYPES

Implement:

### Strings

```text
SET
GET
DEL
```

### Lists

```text
LPUSH
RPUSH
LPOP
```

### Hashes

```text
HSET
HGET
```

### Sets

```text
SADD
SMEMBERS
```

Use efficient Rust data structures.

Document:

* complexity
* memory characteristics
* locking strategy
* ownership model

---

# 19. CONCURRENCY MODEL

Prefer safe Rust.

Avoid `unsafe` unless there is a demonstrated performance requirement.

Use Tokio for asynchronous networking and I/O.

Clearly separate:

```text
async networking
```

from:

```text
CPU-bound storage operations
```

Do not accidentally block Tokio worker threads with expensive operations.

Use bounded channels where appropriate.

Avoid unbounded queues for:

* client requests
* replication
* WAL writes
* internal events

---

# 20. BACKPRESSURE

Backpressure must be explicit.

Define behavior when:

* client sends faster than server can process
* follower is slow
* disk is slow
* WAL queue is full
* memory is exhausted
* Raft replication falls behind

The system must not respond to overload by allocating unbounded memory.

---

# 21. MEMORY MANAGEMENT

Implement configurable:

```text
max_memory
```

Initially use:

```text
noeviction
```

if eviction is not implemented.

When memory exceeds the configured limit:

```text
reject writes
```

rather than silently corrupting or dropping data.

Track memory usage where practical.

Document memory accounting limitations.

---

# 22. TTL

TTL must be deterministic across replicas.

Do not allow every replica to independently decide when a key expires.

Represent expiration explicitly:

```rust
expire_at: Option<u64>
```

inside the replicated command/state.

Required commands:

```text
EXPIRE
TTL
```

Expiration behavior must be tested across:

* leader
* follower
* failover
* restart
* snapshot restore

---

# 23. PERSISTENCE

Use a durable WAL as the authoritative persistence mechanism for committed Raft data.

Architecture:

```text
Raft command
    ↓
WAL
    ↓
Raft commit
    ↓
state machine
```

Do not create two independent authoritative logs.

Snapshots must compact the WAL/Raft log.

Implement:

```text
WAL
Snapshot
Recovery
Log replay
Snapshot restore
```

---

# 24. WAL REQUIREMENTS

The WAL must support:

* append
* sequence/index
* term
* command payload
* checksum
* fsync policy
* recovery
* truncation
* corruption detection

Configurable durability:

```text
sync = always
sync = periodic
sync = never
```

Document the durability guarantees of each mode.

Default to the safest reasonable production behavior.

---

# 25. SNAPSHOTS

Snapshots must contain enough state to restore:

* key/value state
* shard metadata
* Raft snapshot metadata
* relevant indexes
* TTL information

Snapshot creation must not corrupt live state.

Test:

```text
write data
snapshot
crash
restart
restore snapshot
replay WAL
verify state
```

---

# 26. CRASH RECOVERY

On restart:

```text
process starts
 ↓
load metadata
 ↓
load latest snapshot
 ↓
restore Raft state
 ↓
replay WAL
 ↓
recover state machine
 ↓
rejoin cluster
 ↓
catch up missing Raft entries
```

Test crash recovery after:

* committed write
* uncommitted write
* partial WAL write
* snapshot
* leader crash
* follower crash

---

# 27. MEMBERSHIP

Nodes must have stable identities.

A node identity must not depend solely on IP/port.

Support:

```text
JOIN
LEAVE
REJOIN
```

Document how cluster membership changes are coordinated.

Do not allow arbitrary untrusted nodes to silently join a cluster.

---

# 28. FAILURE DETECTION

Implement heartbeat/health monitoring.

Distinguish:

```text
alive
suspected
unreachable
dead
```

Failure detection must NOT itself be treated as proof that a node is permanently dead.

Consensus decides leadership.

Do not allow gossip alone to promote replicas in a way that can violate quorum safety.

---

# 29. NETWORK PARTITIONS

Explicitly handle:

```text
A | B C
```

where A is isolated from B/C.

For RF=3:

* A must not be able to commit new majority writes if isolated
* B/C may elect a new leader
* when A rejoins, it must reconcile through Raft
* previously committed history must not be lost

Write tests specifically for this scenario.

---

# 30. SPLIT-BRAIN PREVENTION

Never allow two leaders for the same Raft term to independently commit conflicting histories.

Leader election must be quorum-based.

Term/index rules must be enforced.

When a node discovers a higher term:

```text
step down
```

Immediately.

---

# 31. OBSERVABILITY

Implement structured logging.

Prefer:

```text
tracing
tracing-subscriber
```

or equivalent Rust-native tooling.

Every node should expose metrics such as:

```text
requests_total
requests_failed_total

command_latency
command_duration

raft_term
raft_commit_index
raft_applied_index

raft_leader_changes_total
raft_elections_total

replication_lag

wal_bytes_written
wal_fsync_duration

memory_used_bytes

connections_active

cluster_nodes
cluster_shards
```

Expose Prometheus-compatible metrics.

---

# 32. HEALTH ENDPOINTS

Provide:

```text
health
ready
live
```

Semantics must be documented.

For example:

```text
live:
    process is functioning

ready:
    node has initialized sufficiently to serve traffic

healthy:
    node and its required dependencies are functioning
```

Do not treat "TCP port is open" as equivalent to "database is healthy."

---

# 33. ADMIN / DEBUGGING TOOLS

Provide commands or tooling to inspect:

```text
cluster topology
slot ownership
Raft state
leader
term
commit index
replication lag
memory usage
WAL position
```

Example administrative commands:

```text
CLUSTER INFO
CLUSTER NODES
CLUSTER SLOTS
RAFT INFO
```

Exact command naming may differ, but equivalent functionality must exist.

---

# 34. SECURITY

Design the networking layer so TLS can be enabled.

Support or leave clean extension points for:

* TLS
* client authentication
* node authentication
* ACLs
* authorization
* secure cluster join

Do not hard-code an architecture that makes secure deployment impossible.

---

# 35. PROJECT STRUCTURE

Use a clean Cargo workspace if appropriate.

Suggested structure:

```text
crates/
├── server/
├── protocol/
├── storage/
├── raft/
├── cluster/
├── replication/
├── persistence/
├── commands/
├── client/
├── metrics/
└── test-utils/
```

You may modify this structure if a better architecture is justified.

Do not create modules solely because they appear in this prompt.

Prefer high cohesion and low coupling.

---

# 36. DEPENDENCIES

Prefer mature Rust crates.

Potential dependencies include:

```text
tokio
bytes
serde
thiserror
anyhow
tracing
tracing-subscriber
prometheus
crc16
criterion
proptest
```

For Raft, evaluate mature crates before implementing Raft yourself.

Do not introduce unnecessary dependencies.

Document important dependency decisions.

---

# 37. ERROR HANDLING

Never use:

```rust
unwrap()
expect()
panic!()
```

in production request paths unless there is a documented invariant that makes the condition impossible.

Use typed errors.

Distinguish:

```text
client error
server error
storage error
network error
consensus error
persistence error
cluster routing error
```

---

# 38. TESTING REQUIREMENTS

Testing is a first-class feature.

Implement:

### Unit tests

For:

* hash slots
* hash tags
* RESP parser
* RESP encoder
* command validation
* storage
* TTL
* WAL
* snapshot
* routing
* state machine

### Integration tests

At minimum:

```text
single-node
three-node
five-node
```

clusters.

Test:

```text
SET → GET
replication
failover
rejoin
resharding
snapshot recovery
TTL
leader election
```

---

# 39. FAILURE-INJECTION TEST HARNESS

Create a test framework capable of simulating:

```text
node crash
node restart

message delay
message drop
message duplication
message reordering

network partition
network healing

slow disk
WAL failure
```

The goal is to test distributed invariants rather than just happy paths.

---

# 40. CRITICAL DISTRIBUTED TESTS

Implement tests for:

### Leader crash

```text
A = leader
B/C = followers

kill A

B/C elect leader

write continues

restart A

A catches up
```

### Minority partition

```text
A | B C
```

Verify:

```text
A cannot commit writes

B/C can continue

A catches up after healing
```

### Majority partition

```text
A B | C
```

Verify the majority side can continue.

### Stale follower

Verify stale replicas never become leaders with an invalid history.

### Repeated failures

Kill leaders repeatedly and verify convergence.

### Resharding + failure

Start migration.

Kill source.

Kill target.

Restart nodes.

Verify cluster converges to a valid ownership state.

---

# 41. PROPERTY TESTING

Use property-based testing where useful.

Important properties:

```text
hashing is deterministic

RESP parser/encoder round trips

state machine application is deterministic

WAL replay reproduces state

snapshot + WAL replay reproduces state

committed operations are never lost

replicas converge to identical committed state
```

---

# 42. INVARIANTS

Create a formal list of invariants in:

```text
docs/invariants.md
```

At minimum:

### Safety

* A committed command cannot disappear.
* A shard has at most one valid Raft leader per term.
* A follower cannot commit entries that conflict with the leader's committed history.
* State-machine application order follows committed log order.
* Replicas applying the same committed log reach equivalent state.
* Slot ownership is unambiguous.
* A node cannot acknowledge a strongly consistent write without the required consensus condition.

### Recovery

* Restarting a node cannot silently lose committed data.
* WAL replay is deterministic.
* Snapshot + WAL replay equals the state represented by the committed log.

### Routing

* Every slot has a defined owner or explicit migration state.
* Requests during migration follow documented semantics.

---

# 43. PERFORMANCE

After correctness is established, benchmark:

```text
SET throughput
GET throughput

mixed GET/SET

pipeline throughput

different value sizes

different shard counts

different replica counts

leader/follower latency

WAL enabled/disabled

snapshot overhead
```

Use Criterion where appropriate.

Provide a benchmark script.

Do NOT claim "linear scalability" without measurements.

Instead report measured scaling characteristics.

---

# 44. REDIS COMPARISON

Benchmark against Redis only after the implementation is correct.

Compare:

```text
throughput
p50 latency
p95 latency
p99 latency
memory usage
pipeline behavior
```

Use equivalent workloads.

Document hardware and configuration.

Do not manipulate benchmark parameters to produce favorable results.

---

# 45. LOCAL CLUSTER

Provide a simple local cluster configuration.

Example:

```text
node-1
  client: 6379
  cluster: 7001

node-2
  client: 6380
  cluster: 7002

node-3
  client: 6381
  cluster: 7003
```

Provide scripts such as:

```text
scripts/start-cluster.sh
scripts/stop-cluster.sh
scripts/reset-cluster.sh
scripts/benchmark.sh
```

The exact implementation is up to you.

---

# 46. CONFIGURATION

Support configuration through a file and/or CLI.

Example:

```toml
[node]
id = "node-1"
client_addr = "127.0.0.1:6379"
cluster_addr = "127.0.0.1:7001"

[storage]
max_memory = "1GB"

[raft]
election_timeout_ms = 1000
heartbeat_interval_ms = 100

[persistence]
wal = true
fsync = "always"

[cluster]
replication_factor = 3
slot_count = 16384
```

Do not hard-code production-relevant settings.

---

# 47. CLI

Provide a server binary:

```text
kvdb-server
```

and, if useful:

```text
kvdb-cli
```

CLI should support:

```text
start
cluster info
cluster nodes
cluster slots
raft info
```

---

# 48. DOCUMENTATION

The README must explain:

* what the database is
* architecture
* consistency model
* sharding
* Raft
* replication
* persistence
* failure behavior
* local cluster setup
* commands
* configuration
* benchmarking
* limitations

Include ASCII architecture diagrams.

---

# 49. NON-GOALS FOR VERSION 1

Do NOT implement these unless they are required by the architecture:

```text
Lua scripting
cross-shard transactions
Pub/Sub
Streams
Redis modules
geospatial commands
full Redis compatibility
complex eviction policies
Byzantine fault tolerance
multi-region consensus
global transactions
```

The system should have extension points for future features without pretending they already exist.

---

# 50. IMPLEMENTATION PHASES

Implement in this order.

## Phase 0 — Architecture

Produce:

```text
architecture.md
system-model.md
invariants.md
failure-model.md
PLAN.md
```

No major implementation yet.

Acceptance:

* architecture is internally consistent
* Raft-per-shard is defined
* consistency semantics are explicit
* persistence model is explicit
* failure model is explicit

---

## Phase 1 — Core storage

Implement:

* strings
* lists
* hashes
* sets
* TTL metadata
* command state machine

Tests must pass.

---

## Phase 2 — RESP server

Implement:

* TCP server
* RESP parser
* RESP encoder
* command dispatch
* pipelining
* limits
* errors

Test fragmented and pipelined requests.

---

## Phase 3 — Single-node persistence

Implement:

* WAL
* fsync policy
* snapshots
* crash recovery

Test:

```text
write
crash
restart
recover
verify
```

---

## Phase 4 — Raft

Implement/integrate:

* leader election
* log replication
* commit
* apply
* persistence
* recovery

Start with one shard.

Test three-node replication.

---

## Phase 5 — Sharding

Implement:

```text
16384 slots
slot → shard
shard → Raft group
```

Test routing.

---

## Phase 6 — Multi-shard replication

Allow:

```text
multiple shards
multiple Raft groups
multiple leaders
```

Test parallel operation.

---

## Phase 7 — Cluster membership

Implement:

* join
* leave
* node identity
* membership state
* replica placement

---

## Phase 8 — Failover

Implement:

* failure detection
* leader election
* client redirection
* node recovery
* replica catch-up

Run fault-injection tests.

---

## Phase 9 — Online resharding

Implement:

* slot migration
* migration state
* data transfer
* mutation catch-up
* cutover
* recovery

---

## Phase 10 — Observability

Implement:

* structured logging
* metrics
* health endpoints
* admin tooling

---

## Phase 11 — Performance

Implement:

* batching
* pipelining optimizations
* efficient serialization
* allocation reductions
* connection management

Only optimize after profiling.

---

# 51. ACCEPTANCE TEST

The implementation is not considered complete until the following scenario works.

Start:

```text
3 nodes
3 replicas
multiple shards
```

Then:

```text
1. Create cluster.

2. Verify slot assignment.

3. SET 100,000 keys.

4. Verify reads.

5. Verify replication.

6. Kill a shard leader.

7. Verify a new leader is elected.

8. Verify reads still work.

9. Verify writes still work.

10. Restart the failed node.

11. Verify it catches up.

12. Start resharding.

13. Continue reads/writes during resharding.

14. Kill a node during migration.

15. Restart it.

16. Verify cluster convergence.

17. Create snapshot.

18. Stop node.

19. Corrupt/truncate only an uncommitted WAL tail if supported by the recovery model.

20. Restart node.

21. Recover snapshot + WAL.

22. Verify committed state.

23. Verify all replicas converge.

24. Run consistency/fault-injection tests.

25. Run benchmarks.
```

All documented invariants must hold.

---

# 52. CODE QUALITY

Use:

```text
cargo fmt
cargo check
cargo test
cargo clippy
```

Treat warnings seriously.

Prefer:

```text
small modules
typed errors
explicit ownership
documented invariants
deterministic tests
```

Avoid:

```text
giant modules
global mutable state
unbounded channels
unexplained locks
silent error handling
TODO-driven fake implementations
```

---

# 53. NO FAKE IMPLEMENTATIONS

This is critical.

Never implement:

```rust
todo!()
unimplemented!()
```

for required functionality.

Do not create methods that return fake success.

Do not create placeholder Raft behavior.

Do not claim failover works if it has only been simulated.

Do not claim persistence works if data is not actually persisted.

Do not claim strong consistency if writes are acknowledged before quorum.

If a required feature cannot currently be completed, clearly document it and stop at the appropriate phase rather than hiding the limitation.

---

# 54. DESIGN DECISION LOG

Maintain:

```text
docs/decisions/
```

For major decisions, document:

```text
Decision
Context
Options considered
Chosen approach
Why
Trade-offs
Consequences
```

Important decisions include:

* Raft implementation
* shard model
* WAL design
* snapshot design
* slot migration
* concurrency model
* memory model
* consistency semantics

---

# 55. FINAL DELIVERABLE

The repository must contain:

```text
README.md
PLAN.md
Cargo.toml

docs/

crates/

tests/

scripts/

benchmarks/
```

It must be possible for a developer to clone the repository and run:

```bash
cargo build
cargo test
```

and start a local cluster using documented commands.

---

# 56. FINAL RESPONSE FORMAT

At the end of implementation, report:

```text
Implemented:
- ...

Not implemented:
- ...

Known limitations:
- ...

Consistency guarantees:
- ...

Failure guarantees:
- ...

Test results:
- cargo test
- integration tests
- fault-injection tests

Benchmark results:
- ...

How to run:
- ...

Important design decisions:
- ...
```

Never describe an unimplemented feature as implemented.

Never claim production readiness merely because the project compiles.

---

# 57. MOST IMPORTANT PRINCIPLE

This is a distributed database.

The hardest part is not:

```text
GET
SET
HashMap
TCP
```

The hardest part is maintaining correctness when:

```text
nodes crash
messages disappear
messages arrive late
leaders change
disks fail
partitions occur
data is migrating
clients retry
nodes restart
```

Therefore:

> **Treat distributed invariants and failure behavior as the primary product requirements.**

A smaller system that is demonstrably correct is preferable to a larger system with unsafe distributed semantics.

Start with Phase 0.

Do not skip directly to implementation.

---


Start with Phase 0 only.

Inspect the repository first. Do not write the implementation yet.

Produce:

1. `PLAN.md`
2. `docs/architecture.md`
3. `docs/system-model.md`
4. `docs/invariants.md`
5. `docs/failure-model.md`
6. `docs/consistency.md`
7. `docs/raft.md`
8. `docs/sharding.md`
9. `docs/persistence.md`
10. `docs/resharding.md`

Then review the proposed architecture for internal contradictions.

In particular, verify:

* each shard is an independent Raft group
* cluster metadata is separated from shard state
* the WAL/snapshot model is consistent with Raft
* TTL behavior is deterministic
* network partitions cannot produce unsafe writes
* resharding cannot lose concurrent mutations
* client routing has defined behavior during migration

Do not start Phase 1 until the design is internally consistent.

At the end, give me:

* the proposed architecture
* important invariants
* major trade-offs
* unresolved design questions
* exact Phase 1 acceptance criteria
