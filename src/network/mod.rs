use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::commands::{execute, CommandContext};
use crate::persistence::Aof;
use crate::protocol::{parse_frame, serialize_frame};
use crate::storage::Store;

pub struct Server {
    store: Arc<Store>,
    aof: Option<Arc<Aof>>,
}

impl Server {
    pub fn new(store: Arc<Store>, aof: Option<Arc<Aof>>) -> Self {
        Server { store, aof }
    }

    pub async fn run(self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("loony-redis listening on {addr}");

        let store = self.store;
        let aof = self.aof;

        loop {
            let (socket, peer) = listener.accept().await?;
            debug!("new connection from {peer}");

            let store = Arc::clone(&store);
            let aof = aof.clone();

            tokio::spawn(async move {
                let ctx = CommandContext { store, aof };
                if let Err(e) = handle_connection(socket, ctx).await {
                    if !is_connection_reset(&e) {
                        error!("connection {peer} error: {e}");
                    }
                }
                debug!("connection {peer} closed");
            });
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    ctx: CommandContext,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);

    loop {
        let n = socket.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(()); // clean close
        }

        // Process as many complete frames as are available in the buffer.
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    // Advance past the consumed bytes before we await execute,
                    // so the borrow of `buf` is released.
                    let _ = buf.split_to(consumed);
                    let response = execute(frame, &ctx).await;
                    let bytes = serialize_frame(&response);
                    socket.write_all(&bytes).await?;
                }
                Ok(None) => break, // need more data
                Err(e) => {
                    let err = serialize_frame(&crate::protocol::Frame::error(format!(
                        "ERR protocol error: {e}"
                    )));
                    socket.write_all(&err).await?;
                    buf.clear();
                    break;
                }
            }
        }
    }
}

fn is_connection_reset(e: &anyhow::Error) -> bool {
    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
        matches!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
        )
    } else {
        false
    }
}
