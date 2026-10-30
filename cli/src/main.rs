use color_eyre::Report;
use iroh::endpoint::presets;
use patchsync::RecvProtocol;
mod init;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct AppConfig {
    root: std::path::PathBuf,
    keep_open: bool,
}

impl init::Args {
    pub fn split_appcfg_and_command(self) -> (AppConfig, init::Command) {
        (
            AppConfig {
                root: self.root_dir,
                keep_open: self.keep_open,
            },
            self.command,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;

    let (appcfg, command) = args.split_appcfg_and_command();

    match command {
        init::Command::Send { ticket } => handle_send(appcfg, ticket).await?,
        init::Command::Receive => handle_recv(appcfg).await?,
    }

    Ok(())
}

async fn handle_recv(AppConfig { root, keep_open }: AppConfig) -> eyre::Result<()> {
    // TODO: Set key to project data dir
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
            tracing::warn!(
                "Failed to persist secret key to {}: {e}",
                key_path.display()
            );
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
    let proto = RecvProtocol::new(root, move |ev| {
        let _ = tx.send(ev);
    });
    let router = iroh::protocol::Router::builder(ep)
        .accept(patchsync::ALPN, proto)
        .spawn();

    let result: eyre::Result<()> = tokio::select! {
        r = async {
            while let Ok(ev) = rx.recv_async().await {
                match ev {
                    patchsync::sync::RecvEvent::Finished => {
                        println!("Sync completed successfully.");
                        if !keep_open {
                            break;
                        }
                    }
                    patchsync::sync::RecvEvent::Error(err) => {
                        eprintln!("Sync failed: {err}");
                        if !keep_open {
                            return Err(eyre::eyre!("Sync failed: {err}"));
                        }
                    }
                    ev => {
                        // Handle progress or other events if desired
                        tracing::debug!("RECV event: {ev:?}");
                    }
                }
            }
            Ok(())
        } => r,
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutdown requested by user (Ctrl+C)...");
            Ok(())
        }
    };

    router.shutdown().await?;

    result
}

async fn handle_send(
    AppConfig { root, keep_open }: AppConfig,
    ticket: iroh_tickets::endpoint::EndpointTicket,
) -> eyre::Result<()> {
    let ep = iroh::Endpoint::builder(presets::N0).bind().await?;
    let conn = ep.connect(ticket, patchsync::ALPN).await?;

    let (run_tx, run_rx) = flume::unbounded::<()>();

    let root_clone = root.clone();
    let mut runner = tokio::spawn(async move {
        while run_rx.recv_async().await.is_ok() {
            if let Err(e) = run_once(&conn, root_clone.clone()).await {
                if keep_open {
                    tracing::error!("Sync failed: {e:?}");
                } else {
                    return Err(e);
                }
            }
        }

        eyre::Ok(())
    });

    // Must outlive the runner await below, otherwise the watcher is dropped immediately.
    let _debouncer = if keep_open {
        use notify_debouncer_full::{DebounceEventResult, new_debouncer, notify::*};

        let run_tx = run_tx.clone();

        let mut debouncer = new_debouncer(
            std::time::Duration::from_secs(2),
            None,
            move |d_res: DebounceEventResult| match d_res {
                Ok(_e) => {
                    if let Err(e) = run_tx.send(()) {
                        tracing::error!("Error sending run signal: {e:?}");
                    }
                }
                Err(errs) => {
                    for e in errs {
                        tracing::warn!("Watch event error: {e:?}");
                    }
                }
            },
        )?;

        debouncer.watch(&root, RecursiveMode::Recursive)?;
        Some(debouncer)
    } else {
        // One-shot: send a single run signal, then close the channel so the runner exits.
        run_tx.send_async(()).await?;
        drop(run_tx);
        None
    };

    if keep_open {
        tokio::select! {
            r = &mut runner => r??,
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutdown requested by user (Ctrl+C)...");
                runner.abort();
            }
        }
    } else {
        runner.await??;
    }

    Ok(())
}

/// Performs a single sync run over a fresh bi-directional stream,
/// with its own event channel and progress bar.
async fn run_once(conn: &iroh::endpoint::Connection, root: std::path::PathBuf) -> eyre::Result<()> {
    let (evtx, evrx) = flume::unbounded();
    let (tx, rx) = conn.open_bi().await?;
    let handler = patchsync::SendHandler::new(tx, rx, root).await?;

    let pb = indicatif::ProgressBar::new_spinner();
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let progress_task = tokio::spawn(async move {
        while let Ok(x) = evrx.recv_async().await {
            match x {
                patchsync::sync::SendEvent::DiffComputed { total_bytes, .. } => {
                    pb.disable_steady_tick();
                    pb.set_style(
                        indicatif::ProgressStyle::with_template(
                            "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {eta}",
                        )
                        .expect("Failed to set PB style")
                        .progress_chars("=>-"),
                    );
                    pb.set_length(total_bytes);
                    pb.tick();
                }
                patchsync::sync::SendEvent::Progress { bytes } => {
                    pb.inc(bytes as u64);
                }
                patchsync::sync::SendEvent::Finished => {
                    pb.finish_and_clear();
                }
                ev => pb.println(format!("SEND: {ev:?}")),
            }
        }
        // Channel closed without a Finished event (e.g. on error): clean up the bar.
        pb.finish_and_clear();
    });

    // `evtx` is moved into the closure; when send_loop returns it is dropped,
    // which ends the progress task naturally.
    let res = handler
        .send_loop(move |ev| {
            let _ = evtx.send(ev);
        })
        .await;

    let _ = progress_task.await;

    res?;
    Ok(())
}
