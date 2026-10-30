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
    // Keep-alives stop idle timeouts from killing the connection between syncs,
    // and a generous idle timeout lets it ride out short network drops.
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(5))
        .max_idle_timeout(Some(
            std::time::Duration::from_secs(60)
                .try_into()
                .expect("idle timeout fits in IdleTimeout"),
        ))
        .build();

    let ep = iroh::Endpoint::builder(presets::N0)
        .transport_config(transport_config)
        .bind()
        .await?;

    // One-shot: give up after a few attempts. Watch mode: keep trying forever.
    let max_attempts = if keep_open { None } else { Some(5) };
    let mut conn = connect_with_retry(&ep, &ticket, max_attempts).await?;

    let (run_tx, run_rx) = flume::unbounded::<()>();

    let root_clone = root.clone();
    let ep_task = ep.clone();
    let mut runner = tokio::spawn(async move {
        while run_rx.recv_async().await.is_ok() {
            // Collapse any queued signals into this one run.
            while run_rx.try_recv().is_ok() {}

            let mut retried = false;
            loop {
                if conn.close_reason().is_some() {
                    tracing::warn!("Connection closed, reconnecting...");
                    conn = connect_with_retry(&ep_task, &ticket, max_attempts).await?;
                }

                match run_once(&conn, root_clone.clone()).await {
                    Ok(()) => break,
                    // Connection died mid-sync: reconnect and retry the run once.
                    Err(e) if conn.close_reason().is_some() && !retried => {
                        tracing::warn!("Sync interrupted by connection loss: {e:?}");
                        retried = true;
                    }
                    Err(e) if keep_open => {
                        tracing::error!("Sync failed: {e:?}");
                        break;
                    }
                    Err(e) => return Err(e),
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

    // Close gracefully so in-flight data is flushed.
    ep.close().await;

    Ok(())
}

/// Connects to the receiver, retrying with exponential backoff (1s up to 30s).
/// `max_attempts = None` retries forever.
async fn connect_with_retry(
    ep: &iroh::Endpoint,
    ticket: &iroh_tickets::endpoint::EndpointTicket,
    max_attempts: Option<u32>,
) -> eyre::Result<iroh::endpoint::Connection> {
    let mut attempt = 0u32;
    let mut delay = std::time::Duration::from_secs(1);

    loop {
        attempt += 1;
        match ep.connect(ticket.clone(), patchsync::ALPN).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                if max_attempts.is_some_and(|max| attempt >= max) {
                    return Err(eyre::eyre!(
                        "Failed to connect after {attempt} attempts: {e}"
                    ));
                }
                tracing::warn!("Connect attempt {attempt} failed: {e}; retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(30));
            }
        }
    }
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
