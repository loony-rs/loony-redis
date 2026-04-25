You are a senior distributed systems engineer and Rust expert. Design and implement a **scalable, fault-tolerant, distributed in-memory key-value database inspired by Redis**, written entirely in Rust.

## Core Requirements

### 1. Architecture

- Build a **distributed system** supporting horizontal scaling across multiple nodes.
- Use a **clustered architecture** with sharding (consistent hashing or slot-based partitioning).
- Support **leader-follower replication** for fault tolerance.
- Include **automatic failover** with leader election (Raft or similar consensus algorithm preferred).
- Nodes must support dynamic membership (join/leave).

### 2. Networking

- Use asynchronous Rust (`tokio`) for all I/O.
- Implement a **custom TCP protocol compatible with Redis RESP** (basic compatibility required).
- Support client connections, inter-node communication, and replication channels.
- Include connection pooling and backpressure handling.

### 3. Data Model

- Implement core Redis data types:
  - Strings
  - Lists
  - Hashes
  - Sets

- Use efficient in-memory structures with optional persistence layer.
- Ensure thread-safe concurrent access (lock-free or fine-grained locking preferred).

### 4. Persistence

- Implement:
  - **Append-Only File (AOF)** logging
  - Optional snapshotting (RDB-like)

- Ensure crash recovery using logs.
- Design for durability vs performance trade-offs.

### 5. Fault Tolerance

- Node failure detection via heartbeat/gossip protocol.
- Automatic failover:
  - Promote replica to leader
  - Reassign slots/shards

- Handle network partitions (split-brain scenarios).

### 6. Scalability

- Support:
  - Data sharding across nodes
  - Rebalancing when nodes join/leave

- Minimize data movement during re-sharding.
- Ensure linear scalability under load.

### 7. Consistency

- Provide tunable consistency:
  - Strong consistency (via consensus)
  - Eventual consistency (faster mode)

- Clearly define guarantees.

### 8. API & Commands

- Implement subset of Redis commands:
  - GET, SET, DEL
  - LPUSH, RPUSH, LPOP
  - HSET, HGET
  - SADD, SMEMBERS

- Add cluster-aware routing logic.

### 9. Observability

- Add:
  - Structured logging
  - Metrics (Prometheus-compatible)
  - Health checks

- Provide debugging tools for cluster state.

### 10. Performance

- Optimize for:
  - Low latency
  - High throughput

- Use zero-copy where possible.
- Benchmark against Redis for baseline comparison.

---

## Implementation Details

- Language: Rust (stable)
- Async runtime: Tokio
- Serialization: efficient binary protocol (avoid JSON internally)
- Storage:
  - In-memory primary
  - Disk-backed persistence

---

## Code Organization

Structure the project into modules:

- `network/` – protocol handling, connections
- `storage/` – in-memory data structures
- `cluster/` – sharding, node management
- `replication/` – leader-follower sync
- `consensus/` – Raft implementation
- `persistence/` – AOF + snapshot
- `api/` – command parsing and execution

---

## Deliverables

1. Fully working Rust project
2. Clear documentation:
   - Architecture diagram (textual if needed)
   - Design decisions

3. Example:
   - Running a multi-node cluster locally
   - Basic benchmarking script

---

## Constraints

- Avoid unsafe Rust unless absolutely necessary.
- Prioritize correctness before micro-optimizations.
- Ensure code is idiomatic and production-quality.

---

## Bonus Features (Optional)

- TTL (key expiration)
- Pub/Sub system
- Lua scripting support
- Multi-key transactions
- TLS support for secure communication

---

## Output Expectations

- Provide complete code (not pseudocode).
- Include comments explaining key design decisions.
- Ensure the system can be compiled and run.

---

Think carefully about trade-offs in distributed systems (CAP theorem, latency vs consistency). Build a robust, production-grade system—not a prototype.
