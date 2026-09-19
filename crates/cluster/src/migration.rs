//! Slot migration state (docs/resharding.md, decision-driven replacement
//! for the prototype's unstaged copy-then-flip). These are pure data
//! types plus the routing decision that consults them; the actual data
//! transfer and state-transition driving live in `crates/resharding`
//! (Phase 9), and the durable, consensus-backed storage of this state is
//! `membership::ClusterState.migrations` (decision 0002) -- this crate
//! only defines the shape everyone agrees on and the read-only decision
//! logic, matching its existing role for `SlotTable`/`RoutingDecision`.

/// A slot range's migration state, per docs/resharding.md.
///
/// `Cutover` is the only state that changes client-facing routing
/// behavior (source replies `-ASK` instead of serving the key) --
/// `Preparing`/`Transferring`/`CatchingUp` are all "behave exactly as
/// steady-state ownership by the source" per the docs. There is no
/// separate `Completed` variant: per docs/resharding.md, completing a
/// migration is the same replicated command that updates `SlotTable`
/// ownership and removes this record, so "completed" is simply the
/// absence of a `SlotMigration` for that range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MigrationState {
    Preparing,
    Transferring,
    CatchingUp,
    Cutover,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SlotMigration {
    pub start: u16,
    pub end: u16,
    pub source: crate::ShardId,
    pub target: crate::ShardId,
    /// Where a client redirected via `-ASK` during `Cutover` should go.
    pub target_leader_addr: String,
    pub state: MigrationState,
}

impl SlotMigration {
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }
}

/// Find the migration (if any) covering `slot` in `migrations`. Ranges
/// are never expected to overlap (docs/resharding.md's coordinator only
/// ever has one migration active per range at a time), so the first
/// match is authoritative.
pub fn migration_for_slot(migrations: &[SlotMigration], slot: u16) -> Option<&SlotMigration> {
    migrations.iter().find(|m| m.contains(slot))
}
