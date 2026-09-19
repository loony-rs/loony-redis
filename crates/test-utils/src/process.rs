//! Real OS process management (docs/testing.md): "crash" is a genuine
//! `SIGKILL`, "restart" is genuinely re-executing the node binary
//! against the same on-disk data directory -- not an in-process stand-in.

use std::path::PathBuf;
use tokio::process::{Child, Command};

pub struct TestProcess {
    bin_path: PathBuf,
    args: Vec<String>,
    child: Option<Child>,
}

impl TestProcess {
    /// Spawn the node binary at `bin_path` with `args` and start it
    /// running. Keeps `args` so `restart` can re-exec identically.
    pub fn spawn(bin_path: impl Into<PathBuf>, args: Vec<String>) -> anyhow::Result<Self> {
        let bin_path = bin_path.into();
        let child = Command::new(&bin_path)
            .args(&args)
            .kill_on_drop(true)
            .spawn()?;
        Ok(TestProcess {
            bin_path,
            args,
            child: Some(child),
        })
    }

    /// Send `SIGKILL` (via `Child::kill`, which on Unix is a hard kill,
    /// not a graceful shutdown request) and wait for the process to
    /// actually exit before returning.
    pub async fn kill(&mut self) -> anyhow::Result<()> {
        if let Some(mut child) = self.child.take() {
            child.kill().await?;
        }
        Ok(())
    }

    /// Re-exec the same binary with the same arguments -- including the
    /// same `--dir`, so this only makes sense to call after `kill()`,
    /// against disk state the killed process left behind.
    pub fn restart(&mut self) -> anyhow::Result<()> {
        let child = Command::new(&self.bin_path)
            .args(&self.args)
            .kill_on_drop(true)
            .spawn()?;
        self.child = Some(child);
        Ok(())
    }
}

/// Find a currently-free TCP port on 127.0.0.1 by binding to port 0 and
/// immediately releasing it. Small TOCTOU race (something else could
/// grab the port before the caller binds it) is an accepted risk for a
/// test harness, not production code.
pub fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}
