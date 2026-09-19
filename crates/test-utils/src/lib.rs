//! Failure-injection harness (Phase 8, docs/testing.md): real node
//! processes (`bin/test_node`), a real network-fault proxy layer
//! (`proxy`), OS-level process control (`process`), and an out-of-band
//! admin protocol (`admin`) for driving a running node from outside its
//! Raft RPC port. See each module's doc comment.

pub mod admin;
pub mod process;
pub mod proxy;
