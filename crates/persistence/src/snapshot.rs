//! Snapshots (docs/persistence.md). A snapshot captures every live key's
//! value and its expiry (`storage::KeyEntry`, which is why it exists
//! separately from the legacy `storage::SnapshotEntry` used by the old
//! async-replication full-sync path) plus enough metadata to know which
//! WAL records it already reflects.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_term: u64,
    pub last_included_index: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub entries: Vec<storage::KeyEntry>,
}

fn corrupt_err(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn to_io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

fn snapshot_filename(index: u64) -> String {
    // Zero-padded so lexicographic and numeric ordering agree.
    format!("snapshot-{index:020}.snap")
}

/// Write `snapshot` to `dir`, atomically (temp file + rename) so a crash
/// mid-write never corrupts or removes a previously-good snapshot.
pub fn save_snapshot(dir: &Path, snapshot: &Snapshot) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let final_path = dir.join(snapshot_filename(snapshot.metadata.last_included_index));
    let tmp_path = final_path.with_extension("snap.tmp");

    let payload = bincode::serialize(snapshot).map_err(to_io_err)?;
    let checksum = crc32c::crc32c(&payload);

    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(&(payload.len() as u64).to_le_bytes())?;
        f.write_all(&payload)?;
        f.write_all(&checksum.to_le_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    Ok(final_path)
}

fn load_snapshot_file(path: &Path) -> io::Result<Snapshot> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < 12 {
        return Err(corrupt_err("snapshot file too short"));
    }
    let len = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    if bytes.len() < 8 + len + 4 {
        return Err(corrupt_err("snapshot file truncated"));
    }
    let payload = &bytes[8..8 + len];
    let stored_checksum = u32::from_le_bytes(bytes[8 + len..8 + len + 4].try_into().unwrap());
    if crc32c::crc32c(payload) != stored_checksum {
        return Err(corrupt_err("snapshot checksum mismatch"));
    }
    bincode::deserialize(payload).map_err(to_io_err)
}

/// Load the newest snapshot in `dir`. Per docs/recovery.md, a corrupt
/// snapshot falls back to the next-older one rather than failing outright
/// or silently skipping straight to "no snapshot".
pub fn load_latest_snapshot(dir: &Path) -> io::Result<Option<Snapshot>> {
    if !dir.exists() {
        return Ok(None);
    }
    let mut candidates: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(idx_str) = name
            .strip_prefix("snapshot-")
            .and_then(|s| s.strip_suffix(".snap"))
        {
            if let Ok(idx) = idx_str.parse::<u64>() {
                candidates.push((idx, entry.path()));
            }
        }
    }
    candidates.sort_by_key(|(idx, _)| std::cmp::Reverse(*idx));

    for (idx, path) in candidates {
        match load_snapshot_file(&path) {
            Ok(snap) => return Ok(Some(snap)),
            Err(e) => {
                tracing::warn!("snapshot {idx} at {path:?} unreadable ({e}), trying next-older");
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use storage::{KeyEntry, Value};

    fn sample_snapshot(index: u64) -> Snapshot {
        Snapshot {
            metadata: SnapshotMetadata {
                last_included_term: 1,
                last_included_index: index,
            },
            entries: vec![
                KeyEntry {
                    key: "a".into(),
                    value: Value::String(Bytes::from("1")),
                    expires_at: None,
                },
                KeyEntry {
                    key: "b".into(),
                    value: Value::String(Bytes::from("2")),
                    expires_at: Some(123),
                },
            ],
        }
    }

    #[test]
    fn test_save_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        save_snapshot(dir.path(), &sample_snapshot(5)).unwrap();

        let loaded = load_latest_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.metadata.last_included_index, 5);
        assert_eq!(loaded.entries.len(), 2);
    }

    #[test]
    fn test_load_latest_picks_highest_index() {
        let dir = tempfile::tempdir().unwrap();
        save_snapshot(dir.path(), &sample_snapshot(1)).unwrap();
        save_snapshot(dir.path(), &sample_snapshot(10)).unwrap();
        save_snapshot(dir.path(), &sample_snapshot(5)).unwrap();

        let loaded = load_latest_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.metadata.last_included_index, 10);
    }

    #[test]
    fn test_corrupt_latest_falls_back_to_next_older() {
        let dir = tempfile::tempdir().unwrap();
        save_snapshot(dir.path(), &sample_snapshot(1)).unwrap();
        let newest_path = save_snapshot(dir.path(), &sample_snapshot(10)).unwrap();

        // Corrupt the newest snapshot's checksum region.
        let mut bytes = std::fs::read(&newest_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&newest_path, &bytes).unwrap();

        let loaded = load_latest_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.metadata.last_included_index, 1);
    }

    #[test]
    fn test_no_snapshot_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(load_latest_snapshot(&missing).unwrap().is_none());
    }
}
