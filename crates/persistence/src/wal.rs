//! Write-ahead log (docs/persistence.md). This is the single authoritative
//! on-disk log -- there is no separate AOF (decision 0004 already retired
//! the old async-replication path; this retires the old AOF too).
//!
//! Record format, in order:
//!   magic (1 byte) | version (1 byte) | term (u64 LE) | index (u64 LE)
//!   | payload_len (u32 LE) | payload (payload_len bytes)
//!   | crc32c checksum (u32 LE, over everything before it)
//!
//! Recovery scans records from the start and stops at the first one that
//! is either incomplete (hasn't fully arrived on disk) or fails its
//! checksum, then truncates the file to end exactly at the last good
//! record (docs/recovery.md: "truncate at first bad record"). Per R4 in
//! docs/invariants.md, this is safe because a record is only ever counted
//! as durably committed after its own fsync succeeds -- so a corrupt tail
//! can only ever be uncommitted data.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAGIC: u8 = 0xAA;
const VERSION: u8 = 1;
const HEADER_LEN: usize = 1 + 1 + 8 + 8 + 4;
const CHECKSUM_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    pub term: u64,
    pub index: u64,
    pub payload: Vec<u8>,
}

/// Durability policy for `Wal::append` (docs/persistence.md).
#[derive(Debug, Clone, Copy)]
pub enum SyncPolicy {
    /// fsync after every append. Default; safest.
    Always,
    /// fsync at most once per `Duration`, batching appends between syncs.
    Periodic(Duration),
    /// Never explicitly fsync; rely on the OS page cache only. Least safe
    /// -- for benchmarking and non-durable test scenarios (docs/persistence.md).
    Never,
}

pub struct Wal {
    file: File,
    path: PathBuf,
    next_index: u64,
    sync: SyncPolicy,
    last_fsync: Instant,
}

impl Wal {
    /// Open (creating if absent) the WAL at `path`, scanning and
    /// recovering it per docs/recovery.md, and return the usable `Wal`
    /// handle plus every valid record found (for the caller to replay).
    pub fn open(path: impl AsRef<Path>, sync: SyncPolicy) -> io::Result<(Wal, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let existing = if path.exists() {
            std::fs::read(&path)?
        } else {
            Vec::new()
        };
        let (records, valid_len) = scan(&existing);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        // Truncate any corrupt/incomplete tail found during the scan.
        file.set_len(valid_len as u64)?;

        let next_index = records.last().map(|r| r.index + 1).unwrap_or(0);
        let wal = Wal {
            file,
            path,
            next_index,
            sync,
            last_fsync: Instant::now(),
        };
        Ok((wal, records))
    }

    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Append one record and apply the configured fsync policy. Returns
    /// the assigned index.
    pub fn append(&mut self, term: u64, payload: &[u8]) -> io::Result<u64> {
        let index = self.next_index;
        let buf = encode_record(term, index, payload);
        self.file.write_all(&buf)?;

        match self.sync {
            SyncPolicy::Always => {
                self.file.sync_data()?;
                self.last_fsync = Instant::now();
            }
            SyncPolicy::Periodic(interval) => {
                if self.last_fsync.elapsed() >= interval {
                    self.file.sync_data()?;
                    self.last_fsync = Instant::now();
                }
            }
            SyncPolicy::Never => {}
        }

        self.next_index += 1;
        Ok(index)
    }

    /// Force an fsync regardless of policy (e.g. before treating a batch
    /// of `Periodic`/`Never` appends as durable for an external purpose
    /// such as taking a snapshot).
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        self.last_fsync = Instant::now();
        Ok(())
    }

    /// Discard every record with `index <= cutoff_index` (log compaction
    /// after a snapshot at that index, per docs/persistence.md). Rewrites
    /// the file via a temp-file-then-rename so a crash mid-compaction
    /// never leaves a half-written WAL.
    pub fn compact_before(&mut self, cutoff_index: u64) -> io::Result<()> {
        let existing = std::fs::read(&self.path)?;
        let (records, _valid_len) = scan(&existing);

        let tmp_path = self.path.with_extension("compact_tmp");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            for r in records.iter().filter(|r| r.index > cutoff_index) {
                tmp.write_all(&encode_record(r.term, r.index, &r.payload))?;
            }
            tmp.sync_all()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        Ok(())
    }
}

fn encode_record(term: u64, index: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
    buf.push(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&term.to_le_bytes());
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    let checksum = crc32c::crc32c(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

/// Scan `buf` for consecutive valid records, stopping at the first
/// incomplete or checksum-failing one. Returns the records found and the
/// byte offset just past the last good record (i.e. where a corrupt tail,
/// if any, begins and must be truncated).
fn scan(buf: &[u8]) -> (Vec<WalRecord>, usize) {
    let mut pos = 0usize;
    let mut records = Vec::new();
    while let Some((record, consumed)) = try_read_record(&buf[pos..]) {
        pos += consumed;
        records.push(record);
    }
    (records, pos)
}

fn try_read_record(buf: &[u8]) -> Option<(WalRecord, usize)> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    if buf[0] != MAGIC || buf[1] != VERSION {
        return None;
    }
    let term = u64::from_le_bytes(buf[2..10].try_into().unwrap());
    let index = u64::from_le_bytes(buf[10..18].try_into().unwrap());
    let payload_len = u32::from_le_bytes(buf[18..22].try_into().unwrap()) as usize;
    let total = HEADER_LEN + payload_len + CHECKSUM_LEN;
    if buf.len() < total {
        return None;
    }
    let payload = buf[HEADER_LEN..HEADER_LEN + payload_len].to_vec();
    let stored_checksum =
        u32::from_le_bytes(buf[HEADER_LEN + payload_len..total].try_into().unwrap());
    let computed = crc32c::crc32c(&buf[..HEADER_LEN + payload_len]);
    if computed != stored_checksum {
        return None;
    }
    Some((
        WalRecord {
            term,
            index,
            payload,
        },
        total,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_reopen_replays_all_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, records) = Wal::open(&path, SyncPolicy::Always).unwrap();
            assert!(records.is_empty());
            wal.append(1, b"a").unwrap();
            wal.append(1, b"b").unwrap();
            wal.append(2, b"c").unwrap();
        }
        let (_wal, records) = Wal::open(&path, SyncPolicy::Always).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0],
            WalRecord {
                term: 1,
                index: 0,
                payload: b"a".to_vec()
            }
        );
        assert_eq!(
            records[1],
            WalRecord {
                term: 1,
                index: 1,
                payload: b"b".to_vec()
            }
        );
        assert_eq!(
            records[2],
            WalRecord {
                term: 2,
                index: 2,
                payload: b"c".to_vec()
            }
        );
    }

    #[test]
    fn test_corrupt_tail_is_truncated_committed_prefix_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, _) = Wal::open(&path, SyncPolicy::Always).unwrap();
            wal.append(1, b"good-1").unwrap();
            wal.append(1, b"good-2").unwrap();
        }
        // Simulate a torn write: append garbage that looks like the start
        // of a record but never completes.
        {
            use std::io::Write as _;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[MAGIC, VERSION, 9, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(); // truncated header
        }

        let (_wal, records) = Wal::open(&path, SyncPolicy::Always).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].payload, b"good-2");

        // The corrupt tail must actually have been truncated on disk, not
        // just skipped in memory -- reopening again must not re-discover
        // (or fail on) the garbage.
        let on_disk = std::fs::read(&path).unwrap();
        let (records2, valid_len) = scan(&on_disk);
        assert_eq!(records2.len(), 2);
        assert_eq!(valid_len, on_disk.len());
    }

    #[test]
    fn test_checksum_mismatch_detected_and_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, _) = Wal::open(&path, SyncPolicy::Always).unwrap();
            wal.append(1, b"good").unwrap();
            wal.append(1, b"will-be-corrupted").unwrap();
        }
        // Flip a byte inside the second record's payload without touching
        // its checksum -- this must be detected, not silently accepted.
        {
            let mut bytes = std::fs::read(&path).unwrap();
            let corrupt_at = bytes.len() - 6; // inside the second record's payload/checksum region
            bytes[corrupt_at] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }
        let (_wal, records) = Wal::open(&path, SyncPolicy::Always).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"good");
    }

    #[test]
    fn test_compact_before_discards_only_covered_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = {
            let (wal, _) = Wal::open(&path, SyncPolicy::Always).unwrap();
            wal
        };
        for i in 0..5u8 {
            wal.append(1, &[i]).unwrap();
        }
        wal.compact_before(2).unwrap(); // drop index 0,1,2 -- keep 3,4

        let (_wal2, records) = Wal::open(&path, SyncPolicy::Always).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].index, 3);
        assert_eq!(records[1].index, 4);
    }

    #[test]
    fn test_sync_policy_never_still_persists_across_reopen() {
        // "Never" means no explicit fsync, not "don't write" -- a clean
        // reopen (no real crash) must still see the data, since the OS
        // will have flushed the page cache well before the process exits.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let (mut wal, _) = Wal::open(&path, SyncPolicy::Never).unwrap();
            wal.append(1, b"x").unwrap();
        }
        let (_wal, records) = Wal::open(&path, SyncPolicy::Never).unwrap();
        assert_eq!(records.len(), 1);
    }
}
