//! Online slot migration (Phase 9, docs/resharding.md): the real data
//! transfer between two shard Raft groups and the coordinator that
//! drives PREPARING -> TRANSFERRING -> CATCHING_UP -> CUTOVER ->
//! COMPLETED via committed `membership::MetaCommand`s. Replaces the
//! prototype's unstaged copy-then-flip.

pub mod coordinator;
pub mod rpc;

pub use coordinator::{run_migration, MigrationSpec};
