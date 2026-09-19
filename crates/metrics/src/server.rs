//! Minimal hand-rolled HTTP/1.1 server for `/metrics`, `/live`, `/ready`,
//! `/health` (docs/observability.md) -- same pattern as the prototype's
//! `observability::start_metrics_server` (one-shot request/response per
//! connection, no keep-alive), reused rather than pulling in a full HTTP
//! framework for four static-shaped endpoints.
//!
//! "TCP port is open" is never treated as equivalent to any endpoint's
//! semantics: `/live` answers unconditionally (reaching this handler at
//! all *is* the liveness check), `/ready` and `/health` each consult the
//! `Health` flag the caller maintains.

use std::sync::Arc;

use prometheus::{Encoder, Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::error;

use crate::health::Health;

/// Serve `listener` forever, answering `/metrics` from `registry` and
/// `/live`/`/ready`/`/health` from `health`. Runs in its own task; drop
/// the returned `JoinHandle` if you don't need to await/abort it.
pub fn serve(
    listener: TcpListener,
    registry: Registry,
    health: Arc<Health>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (mut stream, _peer) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let registry = registry.clone();
            let health = health.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };
                let request = std::str::from_utf8(&buf[..n]).unwrap_or("");
                let path = request
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");

                let (status, reason, ct, body) = match path {
                    "/metrics" => {
                        let metric_families = registry.gather();
                        let mut out = Vec::new();
                        let encoder = TextEncoder::new();
                        if let Err(e) = encoder.encode(&metric_families, &mut out) {
                            error!("failed to encode metrics: {e}");
                        }
                        (
                            200u16,
                            "OK",
                            "text/plain; version=0.0.4; charset=utf-8",
                            String::from_utf8_lossy(&out).into_owned(),
                        )
                    }
                    // Reaching this handler at all proves the process is
                    // live and its async runtime is responsive -- nothing
                    // further to check (docs/observability.md).
                    "/live" => (
                        200,
                        "OK",
                        "application/json",
                        "{\"status\":\"live\"}\n".to_string(),
                    ),
                    "/ready" => {
                        if health.is_ready() {
                            (
                                200,
                                "OK",
                                "application/json",
                                "{\"status\":\"ready\"}\n".to_string(),
                            )
                        } else {
                            (
                                503,
                                "Service Unavailable",
                                "application/json",
                                "{\"status\":\"not_ready\"}\n".to_string(),
                            )
                        }
                    }
                    "/health" => {
                        if health.is_ready() && health.is_healthy() {
                            (
                                200,
                                "OK",
                                "application/json",
                                "{\"status\":\"healthy\"}\n".to_string(),
                            )
                        } else {
                            (
                                503,
                                "Service Unavailable",
                                "application/json",
                                "{\"status\":\"unhealthy\"}\n".to_string(),
                            )
                        }
                    }
                    _ => (404, "Not Found", "text/plain", "not found\n".to_string()),
                };

                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::IntCounter;
    use tokio::net::TcpStream;

    async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match sock.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&buf).into_owned();
        let status: u16 = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    async fn start() -> (std::net::SocketAddr, Registry, Arc<Health>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry = Registry::new();
        let health = Arc::new(Health::new());
        serve(listener, registry.clone(), health.clone());
        (addr, registry, health)
    }

    #[tokio::test]
    async fn test_metrics_endpoint_reports_registered_counter() {
        let (addr, registry, _health) = start().await;
        let counter = IntCounter::new("test_counter_total", "a test counter").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc_by(3);

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(body.contains("test_counter_total 3"), "body: {body}");
    }

    #[tokio::test]
    async fn test_live_is_always_ok_regardless_of_health_flags() {
        let (addr, _registry, health) = start().await;
        health.set_ready(false);
        health.set_healthy(false);
        let (status, _) = get(addr, "/live").await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn test_ready_reflects_flag() {
        let (addr, _registry, health) = start().await;
        let (status, _) = get(addr, "/ready").await;
        assert_eq!(status, 503);

        health.set_ready(true);
        let (status, _) = get(addr, "/ready").await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn test_health_requires_both_ready_and_healthy() {
        let (addr, _registry, health) = start().await;
        health.set_ready(true);
        health.set_healthy(false);
        let (status, _) = get(addr, "/health").await;
        assert_eq!(status, 503, "ready but not healthy must still fail /health");

        health.set_healthy(true);
        let (status, _) = get(addr, "/health").await;
        assert_eq!(status, 200);

        health.set_ready(false);
        let (status, _) = get(addr, "/health").await;
        assert_eq!(status, 503, "healthy but not ready must still fail /health");
    }

    #[tokio::test]
    async fn test_unknown_path_is_404() {
        let (addr, _registry, _health) = start().await;
        let (status, _) = get(addr, "/nope").await;
        assert_eq!(status, 404);
    }
}
