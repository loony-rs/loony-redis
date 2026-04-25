mod commands;
mod network;
mod persistence;
mod protocol;
mod storage;

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "loony-redis", about = "A Redis-compatible in-memory KV store")]
struct Args {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// TCP port
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Path to Append-Only File for persistence (omit to disable)
    #[arg(long)]
    aof: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let store = Arc::new(storage::Store::new());

    // Replay AOF before accepting connections.
    let aof = if let Some(ref path) = args.aof {
        let frames = persistence::load_frames(path).await?;
        if !frames.is_empty() {
            info!("replaying {} AOF commands", frames.len());
            let replay_ctx = commands::CommandContext {
                store: store.clone(),
                aof: None, // no re-logging during replay
            };
            for frame in frames {
                commands::execute(frame, &replay_ctx).await;
            }
        }
        Some(Arc::new(persistence::Aof::open(path).await?))
    } else {
        None
    };

    let server = network::Server::new(store, aof);
    server.run(&format!("{}:{}", args.host, args.port)).await
}
