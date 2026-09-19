//! Tiny helper for persisting a single small piece of Raft state (the
//! current vote, the last-purged log id) to its own file: length-prefixed,
//! checksummed, written via temp-file-then-rename. This is the same
//! discipline `persistence::snapshot` uses, just for values much smaller
//! than a full snapshot, so it isn't worth sharing that module's code.

use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Write};
use std::path::Path;

pub fn save<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let payload = bincode::serialize(value).map_err(to_io_err)?;
    let checksum = crc32c::crc32c(&payload);

    let tmp_path = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(&(payload.len() as u32).to_le_bytes())?;
        f.write_all(&payload)?;
        f.write_all(&checksum.to_le_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub fn load<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < 8 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + len + 4 {
        return Ok(None);
    }
    let payload = &bytes[4..4 + len];
    let stored_checksum = u32::from_le_bytes(bytes[4 + len..4 + len + 4].try_into().unwrap());
    if crc32c::crc32c(payload) != stored_checksum {
        return Ok(None);
    }
    bincode::deserialize(payload).map(Some).map_err(to_io_err)
}

fn to_io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}
