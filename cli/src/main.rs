use std::str::FromStr;

use color_eyre::Report;
use iroh::endpoint::presets;
use patchsync::RecvProtocol;
mod init;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;
    println!("Hello, world!");

    let root = args.root_dir;

    match args.command {
        init::Command::Send { ticket } => handle_send(root, ticket).await?,
        init::Command::Receive => handle_recv(root).await?,
    }

    Ok(())
}

async fn handle_recv(root: std::path::PathBuf) -> eyre::Result<()> {
    let ep = iroh::Endpoint::builder(presets::N0).bind().await?;
    let ticket = iroh_tickets::endpoint::EndpointTicket::new(ep.addr());
    println!("TICKET: {ticket}");
    let (tx, rx) = flume::unbounded();
    let proto = RecvProtocol::new(root, tx);
    let router = iroh::protocol::Router::builder(ep)
        .accept(patchsync::ALPN, proto)
        .spawn();

    let _recv_evloop = tokio::spawn(async move {
        for e in rx {
            // no-op
        }
    });

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;

    Ok(())
}

async fn handle_send(
    root: std::path::PathBuf,
    ticket: iroh_tickets::endpoint::EndpointTicket,
) -> eyre::Result<()> {
    let ep = iroh::Endpoint::builder(presets::N0).bind().await?;
    let (evtx, evrx) = flume::unbounded();
    let conn = ep.connect(ticket, patchsync::ALPN).await?;

    let (tx, rx) = conn.open_bi().await?;
    let handler = patchsync::SendHandler::new(tx, rx, root).await?;

    let pb = indicatif::ProgressBar::new_spinner();
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let _send_evloop = tokio::spawn(async move {
        for x in evrx {
            match x {
                patchsync::sync::SendEvent::DiffComputed { total_bytes, .. } => {
                    pb.disable_steady_tick();
                    pb.set_style(indicatif::ProgressStyle::default_bar());
                    pb.set_length(total_bytes);
                    pb.tick();
                }
                patchsync::sync::SendEvent::Progress { bytes } => {
                    pb.inc(bytes as u64);
                }
                patchsync::sync::SendEvent::Finished => {
                    pb.finish();
                }
                ev => pb.println(format!("SEND: {ev:?}")),
            }
        }
    });

    handler.send_loop(evtx).await?;

    Ok(())
}
