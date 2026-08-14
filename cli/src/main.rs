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
    let key_path = root.join(".patchsync_key");
    let key = if key_path.exists() {
        let key_bytes = tokio::fs::read(&key_path).await?;
        iroh::SecretKey::from_bytes(
            &key_bytes
                .try_into()
                .map_err(|_| eyre::eyre!("Invalid key file length"))?,
        )
    } else {
        let key = iroh::SecretKey::generate();
        if let Err(e) = tokio::fs::write(&key_path, key.to_bytes()).await {
            tracing::warn!("Failed to persist secret key to {}: {e}", key_path.display());
        }
        key
    };

    let ep = iroh::Endpoint::builder(presets::N0)
        .secret_key(key)
        .bind()
        .await?;
    let ticket = iroh_tickets::endpoint::EndpointTicket::new(ep.addr());
    println!("TICKET: {ticket}");
    let (tx, rx) = flume::unbounded();
    let proto = RecvProtocol::new(root, tx);
    let router = iroh::protocol::Router::builder(ep)
        .accept(patchsync::ALPN, proto)
        .spawn();

    let _recv_evloop = tokio::spawn(async move {
        for _e in rx {
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
                    pb.set_style(indicatif::ProgressStyle::with_template(
                        "{spinner:.green} {msg:20} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {eta}"
                    ).expect("Failed to set PB style")
                    .progress_chars("=>-"));
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

    let res = handler.send_loop(evtx).await;
    let _ = ep.close().await;
    res?;

    Ok(())
}
