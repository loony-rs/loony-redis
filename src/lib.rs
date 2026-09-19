// Phase 11: expose all modules as a library crate so criterion benchmarks
// and future integration tests can import them without re-compiling the binary.
//
// `storage` and `protocol` are no longer modules of this crate -- they live
// in crates/storage and crates/protocol (see docs/architecture.md) and are
// used here as ordinary path dependencies.
pub mod cluster;
pub mod commands;
pub mod consensus;
pub mod network;
pub mod observability;
pub mod persistence;
pub mod replication;
