use clap::Parser;
use server::{Limits, Server};
use std::sync::Arc;
use storage::Store;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "loony-redis-server", about = "Phase 2 minimal RESP server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 6379)]
    port: u16,

    #[arg(long, default_value_t = Limits::default().max_key_size)]
    max_key_size: usize,

    #[arg(long, default_value_t = Limits::default().max_value_size)]
    max_value_size: usize,

    #[arg(long, default_value_t = Limits::default().max_command_size)]
    max_command_size: usize,

    #[arg(long, default_value_t = Limits::default().max_request_size)]
    max_request_size: usize,

    #[arg(long, default_value_t = Limits::default().max_pipeline_depth)]
    max_pipeline_depth: usize,

    #[arg(long, default_value_t = Limits::default().max_connections)]
    max_connections: usize,

    /// Serves `/metrics`, `/live`, `/ready`, `/health` (docs/observability.md).
    #[arg(long, default_value = "127.0.0.1:9121")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let limits = Limits {
        max_key_size: args.max_key_size,
        max_value_size: args.max_value_size,
        max_command_size: args.max_command_size,
        max_request_size: args.max_request_size,
        max_pipeline_depth: args.max_pipeline_depth,
        max_connections: args.max_connections,
    };

    let store = Arc::new(Store::new());
    let server = Server::new(store, limits);

    let metrics_listener = tokio::net::TcpListener::bind(&args.metrics_addr).await?;
    tracing::info!(
        "metrics http://{}/metrics  live/ready/health also served there",
        args.metrics_addr
    );
    metrics::serve(metrics_listener, server.registry(), server.health());

    let addr = format!("{}:{}", args.host, args.port);
    server.run(&addr).await
}
