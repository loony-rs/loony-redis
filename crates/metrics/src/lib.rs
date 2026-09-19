//! Prometheus metrics + the three health endpoints (docs/observability.md),
//! reused by every binary that hosts real client/Raft/WAL activity
//! (`crates/server`, `test-utils`'s `test_node`, and eventually a
//! production node binary). Deliberately generic: this crate knows
//! nothing about Raft, WAL, or RESP -- it owns only the `prometheus`
//! registry plumbing and the `/live`/`/ready`/`/health` HTTP surface.
//! Callers register their own metric types (raft_term, wal_bytes_written,
//! requests_total, ...) into the `Registry` they pass to `serve`.

pub mod health;
pub mod server;

pub use health::Health;
pub use prometheus;
pub use prometheus::Registry;
pub use server::serve;
