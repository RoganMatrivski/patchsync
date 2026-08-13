use std::{collections::HashMap, str::FromStr};

use iroh::endpoint::presets;

#[tokio::main]
pub async fn main() -> eyre::Result<()> {
    let base = std::path::PathBuf::from_str("C:/delete_after/test_dirtree/")?;
    let old_root = base.join("old_tree");
    let new_root = base.join("new_tree");

    let old_ep = iroh::Endpoint::builder(presets::N0).bind().await?;
    let new_ep = iroh::Endpoint::builder(presets::N0).bind().await?;

    let send_conn = old_ep.connect(new_ep.addr(), patchsync::ALPN).await?;
    let recv_conn = new_ep.connect(old_ep.addr(), patchsync::ALPN).await?;

    let (send_tx, send_rx) = send_conn.open_bi().await?;
    let send_handler = patchsync::sync::Handler::new(send_tx, send_rx, old_root).await?;
    let (recv_tx, recv_rx) = recv_conn.accept_bi().await?;
    let recv_handler = patchsync::sync::Handler::new(recv_tx, recv_rx, new_root).await?;

    let (send_evtx, send_evrx) = flume::unbounded();
    let (recv_evtx, recv_evrx) = flume::unbounded();

    tokio::try_join!(
        send_handler.send_loop(send_evtx),
        recv_handler.recv_loop(recv_evtx)
    )?;

    Ok(())
}
