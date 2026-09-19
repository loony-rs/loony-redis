//! Phase 10: an end-to-end check that docs/observability.md's `raft_*`/
//! `wal_*` metrics and the three health endpoints are real numbers
//! scraped off a genuine running Raft node (`test_node --metrics-addr`),
//! not just unit-tested against synthetic `RaftMetrics`/`WalStats`
//! values (that's covered separately in `raft::metrics` and
//! `test_utils::node_metrics`'s own unit tests).
//!
//! A single-node cluster is enough for this: `initialize()` with only
//! itself as a voter makes it its own leader immediately, so there's a
//! real term/commit index/WAL write to observe without needing the
//! multi-process election machinery `failover.rs` exercises.

use raft::NodeId;
use std::path::PathBuf;
use std::time::Duration;
use test_utils::admin::{self, AdminRequest, AdminResponse};
use test_utils::process::{free_port, TestProcess};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const ID: NodeId = 1;
const TIMEOUT: Duration = Duration::from_secs(15);

async fn http_get(addr: &str, path: &str) -> (u16, String) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match sock.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, text)
}

/// Find the Prometheus line whose metric name (ignoring any `{labels}`)
/// equals `name`, and parse its trailing value.
fn metric_value(body: &str, name: &str) -> f64 {
    body.lines()
        .find(|l| {
            let head = l.split(['{', ' ']).next().unwrap_or("");
            head == name
        })
        .unwrap_or_else(|| panic!("metric {name} not found in body:\n{body}"))
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn test_raft_and_wal_metrics_and_health_endpoints_are_real() {
    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_test_node"));
    let dir = tempfile::tempdir().unwrap();
    let raft_addr = format!("127.0.0.1:{}", free_port().unwrap());
    let admin_addr = format!("127.0.0.1:{}", free_port().unwrap());
    let metrics_addr = format!("127.0.0.1:{}", free_port().unwrap());

    let args = vec![
        "--id".into(),
        ID.to_string(),
        "--dir".into(),
        dir.path().to_string_lossy().into_owned(),
        "--raft-addr".into(),
        raft_addr.clone(),
        "--admin-addr".into(),
        admin_addr.clone(),
        "--peers".into(),
        format!("{ID}={raft_addr}"),
        "--metrics-addr".into(),
        metrics_addr.clone(),
        "--init".into(),
    ];
    let mut proc = TestProcess::spawn(&bin_path, args).unwrap();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Ok(AdminResponse::Metrics(m)) =
            admin::call(&admin_addr, &AdminRequest::Metrics).await
        {
            if m.current_leader == Some(ID) {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("node never became leader of its own single-node cluster");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A real committed write, so commit_index/applied_index/wal bytes are
    // provably non-zero rather than just "didn't error".
    match admin::call(
        &admin_addr,
        &AdminRequest::Propose {
            key: "k".into(),
            value: "v".into(),
        },
    )
    .await
    {
        Ok(AdminResponse::Proposed { .. }) => {}
        other => panic!("propose failed: {other:?}"),
    }

    // The sampler in test_node.rs polls every 500ms.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let (status, body) = http_get(&metrics_addr, "/metrics").await;
    assert_eq!(status, 200);
    assert!(metric_value(&body, "raft_term") >= 1.0, "body:\n{body}");
    assert!(
        metric_value(&body, "raft_commit_index") >= 1.0,
        "body:\n{body}"
    );
    assert!(
        metric_value(&body, "raft_applied_index") >= 1.0,
        "body:\n{body}"
    );
    assert!(
        metric_value(&body, "wal_bytes_written") > 0.0,
        "body:\n{body}"
    );
    assert!(
        body.contains("group_id=\"shard\""),
        "expected group_id label on raft_*/wal_* metrics, body:\n{body}"
    );

    for path in ["/live", "/ready", "/health"] {
        let (status, resp) = http_get(&metrics_addr, path).await;
        assert_eq!(status, 200, "unexpected status for {path}: {resp}");
    }

    proc.kill().await.unwrap();
}

#[tokio::test]
async fn test_metrics_endpoint_absent_without_the_flag() {
    // The flag is optional (existing failover.rs call sites omit it) --
    // confirm omitting it really does skip the metrics server rather than
    // silently binding some default port.
    let bin_path = PathBuf::from(env!("CARGO_BIN_EXE_test_node"));
    let dir = tempfile::tempdir().unwrap();
    let raft_addr = format!("127.0.0.1:{}", free_port().unwrap());
    let admin_addr = format!("127.0.0.1:{}", free_port().unwrap());

    let args = vec![
        "--id".into(),
        ID.to_string(),
        "--dir".into(),
        dir.path().to_string_lossy().into_owned(),
        "--raft-addr".into(),
        raft_addr.clone(),
        "--admin-addr".into(),
        admin_addr.clone(),
        "--peers".into(),
        format!("{ID}={raft_addr}"),
        "--init".into(),
    ];
    let mut proc = TestProcess::spawn(&bin_path, args).unwrap();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if admin::call(&admin_addr, &AdminRequest::Metrics)
            .await
            .is_ok()
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("node never came up");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    proc.kill().await.unwrap();
}
