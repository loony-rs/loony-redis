//! The metadata group's replicated command vocabulary (decision 0002,
//! docs/membership.md). Mirrors `persistence::Command`'s shape and
//! determinism discipline (docs/invariants.md S5) but for cluster-control
//! facts -- node existence/address, slot ownership -- never key/value
//! data.

use crate::identity::NodeIdentity;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MetaCommand {
    /// Record a new node's existence and address (JOIN, docs/membership.md).
    AddNode { node: NodeIdentity, address: String },
    /// Remove a node's record (LEAVE, or an operator-declared permanent
    /// loss).
    RemoveNode { node: NodeIdentity },
    /// A node's address changed (REJOIN with a new address).
    UpdateNodeAddress { node: NodeIdentity, address: String },
    /// Record which shard (and that shard's current leader address) owns
    /// a slot range in steady state (no migration in progress).
    SetSlotOwner {
        start: u16,
        end: u16,
        shard: cluster::ShardId,
        leader_addr: String,
    },
    /// Begin migrating `[start, end]` from `source` to `target`
    /// (docs/resharding.md PREPARING). A no-op if a migration record for
    /// this exact range already exists -- one migration at a time per
    /// range, per the coordinator's own responsibility, not enforced
    /// here beyond not clobbering an in-flight one.
    StartMigration {
        start: u16,
        end: u16,
        source: cluster::ShardId,
        target: cluster::ShardId,
        target_leader_addr: String,
    },
    /// Advance an existing migration record to `state`
    /// (PREPARING->TRANSFERRING->CATCHING_UP->CUTOVER). A no-op if no
    /// record exists for `[start, end]`.
    AdvanceMigration {
        start: u16,
        end: u16,
        state: cluster::MigrationState,
    },
    /// The CUTOVER commit (docs/resharding.md): atomically hand slot
    /// ownership to the migration's target and remove the migration
    /// record -- this single command is what makes a slot's ownership
    /// change and the migration's completion the same instant, so there
    /// is never a window where a slot has no owner or an ambiguous one
    /// (invariant S6/T1). A no-op if no record exists for `[start, end]`.
    CompleteMigration { start: u16, end: u16 },
}

/// Apply `cmd` to `state`. Deterministic: same command, same prior state,
/// same result on every replica (docs/invariants.md S5) -- no clock
/// reads, no randomness.
pub fn apply(state: &mut crate::ClusterState, cmd: &MetaCommand) {
    match cmd {
        MetaCommand::AddNode { node, address } => {
            state.nodes.insert(*node, address.clone());
        }
        MetaCommand::RemoveNode { node } => {
            state.nodes.remove(node);
        }
        MetaCommand::UpdateNodeAddress { node, address } => {
            state.nodes.insert(*node, address.clone());
        }
        MetaCommand::SetSlotOwner {
            start,
            end,
            shard,
            leader_addr,
        } => {
            state
                .slots
                .set_owner(*start..=*end, *shard, leader_addr.clone());
        }
        MetaCommand::StartMigration {
            start,
            end,
            source,
            target,
            target_leader_addr,
        } => {
            let already_active = state
                .migrations
                .iter()
                .any(|m| m.start == *start && m.end == *end);
            if !already_active {
                state.migrations.push(cluster::SlotMigration {
                    start: *start,
                    end: *end,
                    source: *source,
                    target: *target,
                    target_leader_addr: target_leader_addr.clone(),
                    state: cluster::MigrationState::Preparing,
                });
            }
        }
        MetaCommand::AdvanceMigration {
            start,
            end,
            state: new_state,
        } => {
            if let Some(m) = state
                .migrations
                .iter_mut()
                .find(|m| m.start == *start && m.end == *end)
            {
                m.state = *new_state;
            }
        }
        MetaCommand::CompleteMigration { start, end } => {
            if let Some(pos) = state
                .migrations
                .iter()
                .position(|m| m.start == *start && m.end == *end)
            {
                let m = state.migrations.remove(pos);
                state
                    .slots
                    .set_owner(m.start..=m.end, m.target, m.target_leader_addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClusterState;

    #[test]
    fn test_add_and_remove_node_is_deterministic_across_replicas() {
        let node = NodeIdentity::generate();
        let cmds = [
            MetaCommand::AddNode {
                node,
                address: "127.0.0.1:7001".into(),
            },
            MetaCommand::UpdateNodeAddress {
                node,
                address: "127.0.0.1:7002".into(),
            },
        ];

        let mut a = ClusterState::new();
        let mut b = ClusterState::new();
        for cmd in &cmds {
            apply(&mut a, cmd);
            apply(&mut b, cmd);
        }

        assert_eq!(a.nodes.get(&node), Some(&"127.0.0.1:7002".to_string()));
        assert_eq!(a.nodes.get(&node), b.nodes.get(&node));
    }

    #[test]
    fn test_remove_node_clears_record() {
        let node = NodeIdentity::generate();
        let mut state = ClusterState::new();
        apply(
            &mut state,
            &MetaCommand::AddNode {
                node,
                address: "a:1".into(),
            },
        );
        assert!(state.nodes.contains_key(&node));
        apply(&mut state, &MetaCommand::RemoveNode { node });
        assert!(!state.nodes.contains_key(&node));
    }

    #[test]
    fn test_set_slot_owner_updates_slot_table() {
        let mut state = ClusterState::new();
        apply(
            &mut state,
            &MetaCommand::SetSlotOwner {
                start: 0,
                end: 100,
                shard: 1,
                leader_addr: "127.0.0.1:7001".into(),
            },
        );
        assert_eq!(
            state.slots.owner(50),
            Some(&(1, "127.0.0.1:7001".to_string()))
        );
        assert_eq!(state.slots.owner(200), None);
    }

    #[test]
    fn test_migration_lifecycle_start_advance_complete() {
        let mut state = ClusterState::new();
        apply(
            &mut state,
            &MetaCommand::SetSlotOwner {
                start: 0,
                end: 100,
                shard: 1,
                leader_addr: "127.0.0.1:7001".into(),
            },
        );

        apply(
            &mut state,
            &MetaCommand::StartMigration {
                start: 0,
                end: 100,
                source: 1,
                target: 2,
                target_leader_addr: "127.0.0.1:7002".into(),
            },
        );
        assert_eq!(state.migrations.len(), 1);
        assert_eq!(
            state.migrations[0].state,
            cluster::MigrationState::Preparing
        );
        // Ownership must not change yet.
        assert_eq!(
            state.slots.owner(50),
            Some(&(1, "127.0.0.1:7001".to_string()))
        );

        for next in [
            cluster::MigrationState::Transferring,
            cluster::MigrationState::CatchingUp,
            cluster::MigrationState::Cutover,
        ] {
            apply(
                &mut state,
                &MetaCommand::AdvanceMigration {
                    start: 0,
                    end: 100,
                    state: next,
                },
            );
            assert_eq!(state.migrations[0].state, next);
            // Still no ownership change before CompleteMigration.
            assert_eq!(
                state.slots.owner(50),
                Some(&(1, "127.0.0.1:7001".to_string()))
            );
        }

        apply(
            &mut state,
            &MetaCommand::CompleteMigration { start: 0, end: 100 },
        );
        assert!(
            state.migrations.is_empty(),
            "migration record must be removed on completion"
        );
        assert_eq!(
            state.slots.owner(50),
            Some(&(2, "127.0.0.1:7002".to_string())),
            "ownership must transfer atomically with completion"
        );
    }

    #[test]
    fn test_start_migration_does_not_clobber_an_active_one() {
        let mut state = ClusterState::new();
        apply(
            &mut state,
            &MetaCommand::StartMigration {
                start: 0,
                end: 100,
                source: 1,
                target: 2,
                target_leader_addr: "127.0.0.1:7002".into(),
            },
        );
        apply(
            &mut state,
            &MetaCommand::AdvanceMigration {
                start: 0,
                end: 100,
                state: cluster::MigrationState::Transferring,
            },
        );
        // A second StartMigration for the same range must not reset it
        // back to Preparing.
        apply(
            &mut state,
            &MetaCommand::StartMigration {
                start: 0,
                end: 100,
                source: 1,
                target: 3,
                target_leader_addr: "127.0.0.1:7003".into(),
            },
        );
        assert_eq!(state.migrations.len(), 1);
        assert_eq!(
            state.migrations[0].state,
            cluster::MigrationState::Transferring
        );
        assert_eq!(state.migrations[0].target, 2);
    }

    #[test]
    fn test_advance_and_complete_are_no_ops_without_a_matching_record() {
        let mut state = ClusterState::new();
        apply(
            &mut state,
            &MetaCommand::AdvanceMigration {
                start: 0,
                end: 100,
                state: cluster::MigrationState::Cutover,
            },
        );
        apply(
            &mut state,
            &MetaCommand::CompleteMigration { start: 0, end: 100 },
        );
        assert!(state.migrations.is_empty());
        assert_eq!(state.slots.owner(50), None);
    }
}
