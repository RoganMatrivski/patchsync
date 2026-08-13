use std::str::FromStr;

use color_eyre::Report;
use iroh::endpoint::presets;
use patchsync::{ALPN, RecvProtocol};
mod init;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    init::initialize()?;
    println!("Hello, world!");

    let base = std::path::PathBuf::from_str("C:/delete_after/test_dirtree/")?;
    let old_root = base.join("old_tree");
    let new_root = base.join("new_tree");

    let old_ep = iroh::Endpoint::builder(presets::N0).bind().await?;
    let new_ep = iroh::Endpoint::builder(presets::N0).bind().await?;

    let (send_evtx, send_evrx) = flume::unbounded();
    let (recv_evtx, recv_evrx) = flume::unbounded();

    // Receiver node: uses Router to automatically accept incoming connections and handle stream
    let recv_protocol = RecvProtocol::new(new_root, recv_evtx);
    let router = iroh::protocol::Router::builder(new_ep.clone())
        .accept(ALPN, recv_protocol)
        .spawn();

    let _send_evloop = tokio::spawn(async move {
        for x in send_evrx {
            println!("SEND: {x:?}")
        }
    });

    let _recv_evloop = tokio::spawn(async move {
        for x in recv_evrx {
            println!("RECV: {x:?}")
        }
    });

    // Sender node: connects to receiver and initiates sync stream
    let send_conn = old_ep.connect(new_ep.addr(), ALPN).await?;
    let (send_tx, send_rx) = send_conn.open_bi().await?;
    let send_handler = patchsync::SendHandler::new(send_tx, send_rx, old_root).await?;

    send_handler.send_loop(send_evtx).await?;

    router.shutdown().await?;

    Ok(())
}
