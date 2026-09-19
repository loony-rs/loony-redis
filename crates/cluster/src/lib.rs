//! Slot hashing and routing (docs/sharding.md). Phase 5 scope: the slot
//! math and the MOVED/CROSSSLOT routing decision, wired into
//! `crates/server`, ahead of there being a second real shard to route to
//! (Phase 6) or a consensus-backed `ClusterState` to populate the table
//! from (Phase 7). Phase 9 adds slot migration state
//! (`SlotMigration`/`MigrationState`) and the `-ASK` routing decision it
//! drives during `Cutover` -- see docs/resharding.md.

mod migration;
mod routing;
mod slots;

pub use migration::{migration_for_slot, MigrationState, SlotMigration};
pub use routing::{
    ask_error, command_slot, crossslot_error, moved_error, route, CommandSlot, RoutingDecision,
    ShardId, SlotTable,
};
pub use slots::{crc16, slot_for_key, SLOT_COUNT};
