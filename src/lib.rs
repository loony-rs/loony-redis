// Phase 11: expose all modules as a library crate so criterion benchmarks
// and future integration tests can import them without re-compiling the binary.
pub mod cluster;
pub mod commands;
pub mod consensus;
pub mod network;
pub mod observability;
pub mod persistence;
pub mod protocol;
pub mod replication;
pub mod storage;
