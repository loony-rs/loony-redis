//! The replicated command vocabulary (Prompt.md section 10 / docs/raft.md).
//!
//! This is what gets written to the WAL and, from Phase 4 on, what Raft
//! treats as an opaque log entry payload. Every variant is a pure data
//! value -- no wall-clock reads, no randomness -- so that `apply` is
//! deterministic across replicas (docs/invariants.md S5). Any timestamp a
//! command needs (`expire_at`) is carried explicitly, computed once by
//! whoever proposes the command, never recomputed here.

use bytes::Bytes;
use storage::{Store, Value};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Set {
        key: String,
        value: Bytes,
        expire_at: Option<u64>,
    },
    Delete {
        key: String,
    },
    Expire {
        key: String,
        expire_at: u64,
    },
    ListPushLeft {
        key: String,
        values: Vec<Bytes>,
    },
    ListPushRight {
        key: String,
        values: Vec<Bytes>,
    },
    ListPopLeft {
        key: String,
    },
    HashSet {
        key: String,
        field: Bytes,
        value: Bytes,
    },
    SetAdd {
        key: String,
        members: Vec<Bytes>,
    },
}

/// Apply `cmd` to `store`. Never panics: a WRONGTYPE-style failure (e.g.
/// `ListPushLeft` against a key holding a String) is itself deterministic
/// given the prior state, so every replica applying the same committed
/// command sequence reaches the same result whether or not an individual
/// command's underlying storage call errors -- the error is simply
/// dropped rather than mutating anything, consistently everywhere.
pub fn apply(store: &Store, cmd: &Command) {
    match cmd {
        Command::Set {
            key,
            value,
            expire_at,
        } => {
            store.set(key.clone(), Value::String(value.clone()), *expire_at);
        }
        Command::Delete { key } => {
            store.del(std::slice::from_ref(key));
        }
        Command::Expire { key, expire_at } => {
            store.expire(key, *expire_at);
        }
        Command::ListPushLeft { key, values } => {
            let _ = store.lpush(key.clone(), values.clone());
        }
        Command::ListPushRight { key, values } => {
            let _ = store.rpush(key.clone(), values.clone());
        }
        Command::ListPopLeft { key } => {
            let _ = store.lpop(key, 1);
        }
        Command::HashSet { key, field, value } => {
            let _ = store.hset(key.clone(), field.clone(), value.clone());
        }
        Command::SetAdd { key, members } => {
            let _ = store.sadd(key.clone(), members.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::now_ms;

    #[test]
    fn test_apply_is_deterministic_across_stores() {
        let cmds = vec![
            Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_at: Some(now_ms() + 10_000),
            },
            Command::ListPushRight {
                key: "l".into(),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            },
            Command::HashSet {
                key: "h".into(),
                field: Bytes::from("f"),
                value: Bytes::from("v"),
            },
            Command::SetAdd {
                key: "s".into(),
                members: vec![Bytes::from("m1")],
            },
            Command::Expire {
                key: "k".into(),
                expire_at: now_ms() + 20_000,
            },
        ];

        let a = Store::new();
        let b = Store::new();
        for cmd in &cmds {
            apply(&a, cmd);
            apply(&b, cmd);
        }

        assert_eq!(a.get("k").is_some(), b.get("k").is_some());
        assert_eq!(a.llen("l").unwrap(), b.llen("l").unwrap());
        assert_eq!(
            a.hget("h", &Bytes::from("f")).unwrap(),
            b.hget("h", &Bytes::from("f")).unwrap()
        );
        assert_eq!(a.smembers("s").unwrap(), b.smembers("s").unwrap());
    }

    #[test]
    fn test_apply_wrongtype_is_deterministic_not_a_panic() {
        let store = Store::new();
        apply(
            &store,
            &Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_at: None,
            },
        );
        // ListPushLeft against a String key -- must not panic, must not
        // mutate the value.
        apply(
            &store,
            &Command::ListPushLeft {
                key: "k".into(),
                values: vec![Bytes::from("x")],
            },
        );
        assert!(matches!(store.get("k"), Some(Value::String(b)) if b == "v"));
    }
}
