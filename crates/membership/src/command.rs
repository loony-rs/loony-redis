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
    /// a slot range. Slot migration's staged states (Phase 9) will add
    /// more commands here; this one covers steady-state ownership only.
    SetSlotOwner {
        start: u16,
        end: u16,
        shard: cluster::ShardId,
        leader_addr: String,
    },
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
}
