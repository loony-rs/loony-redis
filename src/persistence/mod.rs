use anyhow::Context;
use bytes::Bytes;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::protocol::{parse_frame, Frame};

pub struct Aof {
    file: Mutex<tokio::fs::File>,
    pub path: String,
}

impl Aof {
    pub async fn open(path: &str) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("failed to open AOF file: {path}"))?;
        Ok(Aof {
            file: Mutex::new(file),
            path: path.to_string(),
        })
    }

    async fn write_resp(&self, parts: &[&[u8]]) -> anyhow::Result<()> {
        let mut buf = Vec::with_capacity(parts.iter().map(|p| p.len() + 16).sum::<usize>() + 8);
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part);
            buf.extend_from_slice(b"\r\n");
        }
        let mut file = self.file.lock().await;
        file.write_all(&buf).await?;
        file.flush().await?;
        Ok(())
    }

    pub async fn log_set(&self, key: &str, value: &Bytes) -> anyhow::Result<()> {
        self.write_resp(&[b"SET", key.as_bytes(), value]).await
    }

    pub async fn log_del(&self, keys: &[String]) -> anyhow::Result<()> {
        let mut parts: Vec<&[u8]> = vec![b"DEL"];
        for key in keys {
            parts.push(key.as_bytes());
        }
        self.write_resp(&parts).await
    }

    pub async fn log_list_push(
        &self,
        cmd: &str,
        key: &str,
        values: &[Bytes],
    ) -> anyhow::Result<()> {
        let mut parts: Vec<&[u8]> = vec![cmd.as_bytes(), key.as_bytes()];
        for v in values {
            parts.push(v.as_ref());
        }
        self.write_resp(&parts).await
    }

    pub async fn log_hset(&self, key: &str, field: &Bytes, value: &Bytes) -> anyhow::Result<()> {
        self.write_resp(&[b"HSET", key.as_bytes(), field.as_ref(), value.as_ref()])
            .await
    }

    pub async fn log_sadd(&self, key: &str, members: &[Bytes]) -> anyhow::Result<()> {
        let mut parts: Vec<&[u8]> = vec![b"SADD", key.as_bytes()];
        for m in members {
            parts.push(m.as_ref());
        }
        self.write_resp(&parts).await
    }

    pub async fn log_generic(&self, parts: &[&[u8]]) -> anyhow::Result<()> {
        self.write_resp(parts).await
    }
}

/// Read all RESP frames from the AOF file at `path`. Used during startup replay.
pub async fn load_frames(path: &str) -> anyhow::Result<Vec<Frame>> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };

    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;

    let mut frames = Vec::new();
    let mut pos = 0usize;

    loop {
        match parse_frame(&buf[pos..]) {
            Ok(Some((frame, consumed))) => {
                pos += consumed;
                frames.push(frame);
            }
            Ok(None) => break,
            Err(e) => {
                warn!(
                    "AOF parse error at offset {pos} (ignoring remainder): {e}"
                );
                break;
            }
        }
    }

    info!("AOF loaded {} commands from {path}", frames.len());
    Ok(frames)
}
