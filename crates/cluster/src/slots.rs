//! Hash slots (docs/sharding.md): 16384 slots, Redis-compatible CRC16 and
//! hash-tag extraction, ported unchanged from the prototype's
//! `src/cluster/mod.rs` -- this part was already correct and already
//! tested against real Redis vectors (see the gap analysis in
//! docs/architecture.md).

pub const SLOT_COUNT: u16 = 16384;

/// CRC16-CCITT, polynomial 0x1021, initial value 0 -- matches Redis
/// Cluster's slot hashing exactly.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Extract the hash tag from a key. If the key contains `{non-empty}`,
/// only that portion is hashed -- otherwise the whole key is used. This
/// lets callers co-locate related keys (e.g. `{user}:profile` and
/// `{user}:sessions`) on the same slot (docs/sharding.md).
fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(close) = key[open + 1..].iter().position(|&b| b == b'}') {
            let tag = &key[open + 1..open + 1 + close];
            if !tag.is_empty() {
                return tag;
            }
        }
    }
    key
}

pub fn slot_for_key(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % SLOT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_known_values() {
        // Redis test vectors.
        assert_eq!(slot_for_key(b"foo"), 12182);
        assert_eq!(slot_for_key(b"bar"), 5061);
        assert_eq!(slot_for_key(b"hello"), 866);
    }

    #[test]
    fn test_hash_tag_colocates_matching_tags() {
        assert_eq!(slot_for_key(b"{user}.name"), slot_for_key(b"{user}.email"));
    }

    #[test]
    fn test_empty_hash_tag_falls_back_to_whole_key() {
        assert_ne!(slot_for_key(b"{}key"), slot_for_key(b"{}other"));
    }

    #[test]
    fn test_only_first_tag_group_is_used() {
        // Redis's documented rule: only the first `{...}` group counts,
        // even if the key contains more brace pairs later.
        assert_eq!(slot_for_key(b"{a}{b}"), slot_for_key(b"{a}x"));
    }

    #[test]
    fn test_unclosed_brace_falls_back_to_whole_key() {
        // No closing '}' -- not a valid tag, hash the whole key.
        let whole = crc16(b"{user.name") % SLOT_COUNT;
        assert_eq!(slot_for_key(b"{user.name"), whole);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_slot_for_key_is_deterministic(key in prop::collection::vec(any::<u8>(), 0..64)) {
            let a = slot_for_key(&key);
            let b = slot_for_key(&key);
            prop_assert_eq!(a, b);
        }

        #[test]
        fn prop_slot_is_always_in_range(key in prop::collection::vec(any::<u8>(), 0..64)) {
            prop_assert!(slot_for_key(&key) < SLOT_COUNT);
        }

        #[test]
        fn prop_matching_hash_tags_colocate(
            tag in prop::collection::vec(any::<u8>().prop_filter("no braces", |&b| b != b'{' && b != b'}'), 1..16),
            suffix_a in prop::collection::vec(any::<u8>(), 0..16),
            suffix_b in prop::collection::vec(any::<u8>(), 0..16),
        ) {
            let mut key_a = vec![b'{'];
            key_a.extend_from_slice(&tag);
            key_a.push(b'}');
            key_a.extend_from_slice(&suffix_a);

            let mut key_b = vec![b'{'];
            key_b.extend_from_slice(&tag);
            key_b.push(b'}');
            key_b.extend_from_slice(&suffix_b);

            prop_assert_eq!(slot_for_key(&key_a), slot_for_key(&key_b));
        }
    }
}
