//! The three distinct health states from docs/observability.md.
//!
//! `/live` needs no state at all -- if the HTTP handler in `server.rs`
//! runs to completion, the process and its async runtime are responsive,
//! which is the entire definition of liveness. `/ready` and `/health` are
//! genuinely stateful (recovery may still be in progress; a node can lose
//! quorum after having been healthy), so they're tracked here and set by
//! whoever owns the actual dependency being reported on -- this type has
//! no opinion on what "ready" or "healthy" means for a given binary.

use std::sync::atomic::{AtomicBool, Ordering};

pub struct Health {
    ready: AtomicBool,
    healthy: AtomicBool,
}

impl Default for Health {
    fn default() -> Self {
        Health {
            ready: AtomicBool::new(false),
            healthy: AtomicBool::new(false),
        }
    }
}

impl Health {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set once local recovery has completed and the node is accepting
    /// client connections (docs/observability.md's `/ready` definition).
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Set based on whether the dependencies needed to actually serve
    /// traffic are currently functioning (e.g. WAL writable, quorum
    /// reachable for at least one hosted group) -- docs/observability.md's
    /// `/health` definition. Distinct from `ready`: a node can go from
    /// healthy to unhealthy without ever becoming un-ready again.
    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Relaxed);
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_to_not_ready_not_healthy() {
        let h = Health::new();
        assert!(!h.is_ready());
        assert!(!h.is_healthy());
    }

    #[test]
    fn test_flags_are_independent() {
        let h = Health::new();
        h.set_ready(true);
        assert!(h.is_ready());
        assert!(!h.is_healthy());
        h.set_healthy(true);
        assert!(h.is_healthy());
        h.set_ready(false);
        assert!(!h.is_ready());
        assert!(h.is_healthy());
    }
}
