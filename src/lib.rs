use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    event::Dispatcher,
    snapshot::PathKey,
    sync::{
        EventStreamHandler, PatchItem, ReceiverToSender, RecvEvent, SendEvent, SenderToReceiver,
    },
};
use cap_std::{ambient_authority, fs::Dir};

pub mod chunker;
pub mod dirwalker;
pub mod error;
pub mod event;
pub mod meta;
pub mod snapshot;
pub mod sync;

pub use error::{Error, Result};
pub use event::EventHandler;
pub use meta::{Metadata, MetadataProvider, Value};

pub const ALPN: &[u8] = b"/id/my/rgmtrv/patchsync/4/";

#[derive(Clone)]
pub struct RecvProtocol {
    pub root: std::path::PathBuf,
    handler: Arc<dyn EventHandler<RecvEvent>>,
}

impl std::fmt::Debug for RecvProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvProtocol")
            .field("root", &self.root)
            .finish()
    }
}

impl RecvProtocol {
    pub fn new(root: std::path::PathBuf, handler: impl EventHandler<RecvEvent>) -> Self {
        Self {
            root,
            handler: Arc::new(handler),
        }
    }
}

impl iroh::protocol::ProtocolHandler for RecvProtocol {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let dispatcher = Dispatcher::spawn(Arc::clone(&self.handler));
        let sink = dispatcher.sink();

        sink.emit(RecvEvent::Started);

        let result = async {
            tracing::info!(peer = %connection.remote_id(), "Accepting incoming connection for RecvProtocol");
            let (send, recv) = connection.accept_bi().await?;

            let mut ev_handler =
                EventStreamHandler::<ReceiverToSender, SenderToReceiver, RecvEvent>::new(
                    send,
                    recv,
                    sink.clone(),
                );

            let mut destination: Option<(Arc<Dir>, PathBuf)> = None;
            loop {
                let msg = match ev_handler.recv().await {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!("Connection closed or failed to receive message: {e}");
                        sink.emit(RecvEvent::Error(e.to_string()));
                        break;
                    }
                };

                match msg {
                    SenderToReceiver::RequestSnapshot { subpath } => {
                        tracing::trace!("Received RequestSnapshot from sender");
                        let relative = match resolve_destination(&subpath) {
                            Ok(path) => path,
                            Err(e) => {
                                let err_msg = format!("Invalid destination subpath: {e}");
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                break;
                            }
                        };

                        if let Err(e) = std::fs::create_dir_all(&self.root) {
                            let err_msg = format!(
                                "Failed to create receiver root {}: {e}",
                                self.root.display()
                            );
                            tracing::error!("{err_msg}");
                            sink.emit(RecvEvent::Error(err_msg.clone()));
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        }

                        let root_dir =
                            match Dir::open_ambient_dir(&self.root, ambient_authority()) {
                                Ok(dir) => dir,
                                Err(e) => {
                                    let err_msg = format!(
                                        "Failed to open receiver root {}: {e}",
                                        self.root.display()
                                    );
                                    tracing::error!("{err_msg}");
                                    sink.emit(RecvEvent::Error(err_msg.clone()));
                                    let _ =
                                        ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                    break;
                                }
                            };
                        let target_dir = if relative.as_os_str().is_empty() {
                            root_dir
                        } else {
                            if let Err(e) = root_dir.create_dir_all(&relative) {
                                let err_msg = format!(
                                    "Failed to create destination directory {}: {e}",
                                    relative.display()
                                );
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ =
                                    ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                break;
                            }
                            match root_dir.open_dir(&relative) {
                                Ok(dir) => dir,
                                Err(e) => {
                                    let err_msg = format!(
                                        "Failed to open destination directory {}: {e}",
                                        relative.display()
                                    );
                                    tracing::error!("{err_msg}");
                                    sink.emit(RecvEvent::Error(err_msg.clone()));
                                    let _ =
                                        ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                    break;
                                }
                            }
                        };
                        let target_dir = Arc::new(target_dir);

                        let destination_path = self.root.join(&relative);
                        let old_snapshot = match crate::dirwalker::walkdir_cap(
                            Arc::clone(&target_dir),
                            &destination_path,
                        ) {
                            Ok(s) => s,
                            Err(e) => {
                                let err_msg = format!("Failed to walk root directory: {e}");
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                break;
                            }
                        };

                        destination = Some((target_dir, relative.clone()));
                        let old = match old_snapshot
                            .into_iter()
                            .map(|x| {
                                PathKey::from_pathentry(&destination_path, &x).map(|k| (k, x))
                            })
                            .collect::<Result<HashMap<_, _>, crate::Error>>()
                        {
                            Ok(map) => map,
                            Err(e) => {
                                let err_msg = format!("Failed to build snapshot map: {e}");
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                break;
                            }
                        };

                        let entry_count = old.len();
                        tracing::debug!(entry_count, "Generated snapshot, sending to peer");

                        if let Err(e) = ev_handler.send(ReceiverToSender::Snapshot(old)).await {
                            tracing::error!("Failed to send snapshot to peer: {e}");
                            sink.emit(RecvEvent::Error(e.to_string()));
                            break;
                        }

                        sink.emit(RecvEvent::SnapshotSent {
                            entry_count,
                            subpath: relative,
                        });
                    }
                    SenderToReceiver::SendPatch {
                        items,
                        pre_apply,
                        post_apply,
                    } => {
                        tracing::info!(item_count = items.len(), "Receiving patches from sender");

                        let Some((destination, subpath)) = destination.as_ref() else {
                            let err_msg =
                                "Received patches before requesting a destination snapshot"
                                    .to_string();
                            tracing::error!("{err_msg}");
                            sink.emit(RecvEvent::Error(err_msg.clone()));
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        };

                        if !sink
                            .emit_and_wait(RecvEvent::EntryPreApply {
                                subpath: subpath.clone(),
                                meta: pre_apply,
                            })
                            .await
                        {
                            let err_msg =
                                "Pre-apply event handler did not complete successfully".to_string();
                            tracing::error!("{err_msg}");
                            sink.emit(RecvEvent::Error(err_msg.clone()));
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        }

                        let mut apply_failed = false;
                        for PatchItem {
                            entry,
                            receiving,
                            applied,
                        } in items
                        {
                            let path = entry.path().to_path_buf();
                            tracing::debug!(path = %path.display(), "Receiving entry patch");

                            sink.emit(RecvEvent::EntryReceiving {
                                path: path.clone(),
                                subpath: subpath.clone(),
                                meta: receiving,
                            });

                            if let Err(e) = entry.apply_in(destination) {
                                let err_msg =
                                    format!("Failed to apply patch entry for {}: {e}", path.display());
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                apply_failed = true;
                                break;
                            }

                            tracing::debug!(path = %path.display(), "Patch entry applied successfully");
                            sink.emit(RecvEvent::EntryApplied {
                                path,
                                subpath: subpath.clone(),
                                meta: applied,
                            });
                        }

                        if apply_failed {
                            break;
                        }

                        sink.emit(RecvEvent::EntryPostApply {
                            subpath: subpath.clone(),
                            meta: post_apply,
                        });

                        tracing::info!("All patches applied successfully, sending Ack");
                        if let Err(e) = ev_handler.send(ReceiverToSender::Ack).await {
                            tracing::warn!("Failed to send Ack: {e}");
                        }
                        if let Err(e) = ev_handler.finish().await {
                            tracing::warn!("Failed to finish stream: {e}");
                        }

                        sink.emit(RecvEvent::Finished);

                        // Wait for sender to read Ack and close stream
                        tracing::trace!("Waiting for sender to close stream after Ack");
                        let _ = ev_handler.recv().await;
                        tracing::info!("RecvProtocol stream completed cleanly");
                        break;
                    }
                    SenderToReceiver::UpToDate { meta } => {
                        tracing::trace!("Received UpToDate from sender");

                        let Some((_, subpath)) = destination.as_ref() else {
                            let err_msg =
                                "Received UpToDate before requesting a destination snapshot"
                                    .to_string();
                            tracing::error!("{err_msg}");
                            sink.emit(RecvEvent::Error(err_msg.clone()));
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        };

                        sink.emit(RecvEvent::UpToDate {
                            subpath: subpath.clone(),
                            meta,
                        });
                        let _ = ev_handler.finish().await;
                        sink.emit(RecvEvent::Finished);
                        break;
                    }
                }
            }
            Ok::<(), iroh::protocol::AcceptError>(())
        }
        .await;

        match &result {
            Ok(()) => {}
            Err(e) => {
                sink.emit(RecvEvent::Error(e.to_string()));
            }
        }

        drop(sink);
        if let Err(e) = tokio::task::spawn_blocking(move || dispatcher.shutdown()).await {
            tracing::error!("Failed to join dispatcher thread: {e}");
        }

        result
    }
}

pub struct SendHandler {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
    root: std::path::PathBuf,
    subpath: String,
    meta: Option<Box<dyn MetadataProvider>>,
}

impl SendHandler {
    pub async fn new(
        send: iroh::endpoint::SendStream,
        recv: iroh::endpoint::RecvStream,
        root: std::path::PathBuf,
        subpath: String,
    ) -> crate::Result<Self> {
        Ok(Self {
            send,
            recv,
            root,
            subpath,
            meta: None,
        })
    }

    /// Supply per-event metadata for the receiver's `RecvEvent::Entry*`
    /// events. See [`MetadataProvider`] for when each method is called.
    pub fn with_metadata_provider(mut self, provider: impl MetadataProvider) -> Self {
        self.meta = Some(Box::new(provider));
        self
    }

    pub async fn send_loop(self, handler: impl EventHandler<SendEvent>) -> crate::Result<()> {
        let dispatcher = Dispatcher::spawn(handler);
        let sink = dispatcher.sink();

        sink.emit(SendEvent::Started);

        let result = async {
            tracing::info!(root = %self.root.display(), "Starting SendHandler sync loop");
            let mut provider = self.meta;

            let mut ev_handler =
                EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                    self.send,
                    self.recv,
                    sink.clone(),
                );

            tracing::trace!("Sending RequestSnapshot to receiver");
            ev_handler
                .send(SenderToReceiver::RequestSnapshot {
                    subpath: self.subpath.clone(),
                })
                .await?;

            loop {
                match ev_handler.recv().await? {
                    ReceiverToSender::Snapshot(old) => {
                        tracing::debug!(entry_count = old.len(), "Received snapshot from receiver");
                        sink.emit(SendEvent::SnapshotReceived {
                            entry_count: old.len(),
                        });

                        tracing::trace!("Walking local directory and computing diff");
                        let new_snapshot = crate::dirwalker::walkdir(&self.root)?;
                        let new = new_snapshot
                            .into_iter()
                            .map(|x| Ok((PathKey::from_pathentry(&self.root, &x)?, x)))
                            .collect::<Result<HashMap<_, _>, crate::Error>>()?;
                        let diff = crate::snapshot::diff(old, new)?;

                        if diff.is_empty() {
                            tracing::info!(
                                "No changes detected; notifying receiver and finishing sync"
                            );
                            sink.emit(SendEvent::DiffComputed {
                                total_entries: 0,
                                total_bytes: 0,
                            });

                            let meta = provider
                                .as_mut()
                                .map(|p| p.up_to_date())
                                .unwrap_or_default();
                            ev_handler.send(SenderToReceiver::UpToDate { meta }).await?;
                            ev_handler.finish().await?;
                            sink.emit(SendEvent::UpToDate);
                            sink.emit(SendEvent::Finished);
                            break;
                        }

                        let total_entries = diff.len();
                        let total_bytes = diff
                            .iter()
                            .map(|item| match item {
                                snapshot::SnapshotEntry::Create { bytes, .. } => bytes.len() as u64,
                                snapshot::SnapshotEntry::Update { instructs, .. } => {
                                    instructs.iter().map(|x| x.get_length()).sum::<u64>()
                                }
                                snapshot::SnapshotEntry::Delete { .. } => 0,
                            })
                            .sum();

                        tracing::info!(
                            total_entries = total_entries,
                            total_bytes = total_bytes,
                            "Computed diff for patch sync"
                        );

                        sink.emit(SendEvent::DiffComputed {
                            total_entries,
                            total_bytes,
                        });

                        let (pre_apply, entry_meta, post_apply) = match provider.as_mut() {
                            Some(p) => {
                                let pre = p.pre_apply(&diff);
                                let per_entry = diff
                                    .iter()
                                    .map(|e| (p.receiving(e), p.applied(e)))
                                    .collect::<Vec<_>>();
                                let post = p.post_apply(&diff);
                                (pre, per_entry, post)
                            }
                            None => (
                                Metadata::new(),
                                std::iter::repeat_with(Default::default)
                                    .take(diff.len())
                                    .collect(),
                                Metadata::new(),
                            ),
                        };
                        let items = diff
                            .into_iter()
                            .zip(entry_meta)
                            .map(|(entry, (receiving, applied))| PatchItem {
                                entry,
                                receiving,
                                applied,
                            })
                            .collect();

                        tracing::debug!("Sending patch items to receiver");
                        ev_handler
                            .send(SenderToReceiver::SendPatch {
                                items,
                                pre_apply,
                                post_apply,
                            })
                            .await?;
                    }
                    ReceiverToSender::Ack => {
                        tracing::info!("Received Ack from receiver, sync finished successfully");
                        ev_handler.finish().await?;
                        sink.emit(SendEvent::Finished);
                        break;
                    }
                    ReceiverToSender::Error(err) => {
                        tracing::error!(error = %err, "Received error from receiver");
                        sink.emit(SendEvent::Error(err.clone()));
                        return Err(crate::Error::Receiver(err));
                    }
                }
            }

            Ok::<(), crate::Error>(())
        }
        .await;

        match &result {
            Ok(()) => {}
            Err(e) => {
                sink.emit(SendEvent::Error(e.to_string()));
            }
        }

        drop(sink);
        if let Err(e) = tokio::task::spawn_blocking(move || dispatcher.shutdown()).await {
            tracing::error!("Failed to join dispatcher thread: {e}");
        }

        result
    }
}

fn resolve_destination(subpath: &str) -> crate::Result<PathBuf> {
    if subpath.starts_with(['/', '\\']) {
        return Err(crate::Error::InvalidRelativePath(PathBuf::from(subpath)));
    }

    let mut destination = PathBuf::new();
    for component in subpath.split(['/', '\\']) {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." || component.contains(':') {
            return Err(crate::Error::InvalidRelativePath(PathBuf::from(subpath)));
        }
        destination.push(component);
    }

    Ok(destination)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::endpoint::{Endpoint, presets};
    use iroh::protocol::ProtocolHandler;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn test_resolve_destination() {
        assert_eq!(
            resolve_destination(r"subdir\").unwrap(),
            PathBuf::from("subdir")
        );
        assert!(resolve_destination(r"..\outside").is_err());
        assert!(resolve_destination(r"C:\outside").is_err());
        assert!(resolve_destination(r"\outside").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_create_destination_rejects_symlink_escape() -> crate::Result<()> {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        symlink(outside.path(), root.path().join("link"))?;

        let root_dir = Dir::open_ambient_dir(root.path(), ambient_authority())?;
        assert!(root_dir.create_dir_all("link/nested").is_err());
        assert!(!outside.path().join("nested").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_end_to_end_sync_protocol() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;
        let recv_subdir = recv_dir.path().join("nested");
        std::fs::create_dir_all(&recv_subdir)?;

        // Receiver state: nested/a.txt ("old a"), nested/b.txt ("will delete")
        let recv_a = recv_subdir.join("a.txt");
        let recv_b = recv_subdir.join("b.txt");
        std::fs::write(&recv_a, "old a content")?;
        std::fs::write(&recv_b, "to be deleted")?;

        // Sender state: a.txt ("new a content!"), c.txt ("newly created c")
        let send_a = send_dir.path().join("a.txt");
        let send_c = send_dir.path().join("c.txt");
        std::fs::write(&send_a, "new a content!")?;
        std::fs::write(&send_c, "newly created c")?;

        // Bind endpoints
        let recv_ep = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let send_ep = Endpoint::builder(presets::N0).bind().await?;

        let recv_events = Arc::new(Mutex::new(Vec::new()));
        let recv_events_c = Arc::clone(&recv_events);
        let recv_handler = move |e: RecvEvent| {
            recv_events_c.lock().unwrap().push(e);
        };

        let send_events = Arc::new(Mutex::new(Vec::new()));
        let send_events_c = Arc::clone(&send_events);
        let send_handler_fn = move |e: SendEvent| {
            send_events_c.lock().unwrap().push(e);
        };

        let recv_protocol = RecvProtocol::new(recv_dir.path().to_path_buf(), recv_handler);

        // Spawn receiver accept task
        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        // Connect sender
        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;

        let send_handler = SendHandler::new(
            send_stream,
            recv_stream,
            send_dir.path().to_path_buf(),
            "nested".to_string(),
        )
        .await?;
        send_handler.send_loop(send_handler_fn).await?;

        recv_handle.await?;

        // Verify receiver files match sender state
        assert_eq!(
            std::fs::read_to_string(recv_subdir.join("a.txt"))?,
            "new a content!"
        );
        assert_eq!(
            std::fs::read_to_string(recv_subdir.join("c.txt"))?,
            "newly created c"
        );
        assert!(!recv_subdir.join("b.txt").exists());

        // Verify events emitted
        let recv_recorded = recv_events.lock().unwrap();
        let send_recorded = send_events.lock().unwrap();
        let expected_subpath = PathBuf::from("nested");

        assert!(
            recv_recorded
                .iter()
                .any(|e| matches!(e, RecvEvent::Finished))
        );
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::SnapshotSent { subpath, .. } if subpath == &expected_subpath
        )));
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::EntryPreApply { subpath, meta } if subpath == &expected_subpath && meta.is_empty()
        )));
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::EntryReceiving { subpath, meta, .. } if subpath == &expected_subpath && meta.is_empty()
        )));
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::EntryApplied { subpath, meta, .. } if subpath == &expected_subpath && meta.is_empty()
        )));
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::EntryPostApply { subpath, meta } if subpath == &expected_subpath && meta.is_empty()
        )));
        assert!(
            send_recorded
                .iter()
                .any(|e| matches!(e, SendEvent::Finished))
        );
        assert!(
            recv_recorded
                .iter()
                .any(|e| matches!(e, RecvEvent::Progress { .. }))
        );
        assert!(
            send_recorded
                .iter()
                .any(|e| matches!(e, SendEvent::Progress { .. }))
        );
        assert!(
            !recv_recorded
                .iter()
                .any(|e| matches!(e, RecvEvent::UpToDate { .. }))
        );
        assert!(
            !send_recorded
                .iter()
                .any(|e| matches!(e, SendEvent::UpToDate))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_no_changes() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;

        std::fs::write(recv_dir.path().join("same.txt"), "identical content")?;
        std::fs::write(send_dir.path().join("same.txt"), "identical content")?;

        let recv_ep = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let send_ep = Endpoint::builder(presets::N0).bind().await?;

        let recv_events = Arc::new(Mutex::new(Vec::new()));
        let recv_events_c = Arc::clone(&recv_events);
        let recv_handler = move |e: RecvEvent| {
            recv_events_c.lock().unwrap().push(e);
        };
        let send_events = Arc::new(Mutex::new(Vec::new()));
        let send_events_c = Arc::clone(&send_events);
        let send_handler_fn = move |e: SendEvent| {
            send_events_c.lock().unwrap().push(e);
        };

        let recv_protocol = RecvProtocol::new(recv_dir.path().to_path_buf(), recv_handler);

        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;

        let send_handler = SendHandler::new(
            send_stream,
            recv_stream,
            send_dir.path().to_path_buf(),
            String::new(),
        )
        .await?;

        send_handler.send_loop(send_handler_fn).await?;

        recv_handle.await?;

        let send_recorded = send_events.lock().unwrap();
        let diff_event = send_recorded
            .iter()
            .find(|e| matches!(e, SendEvent::DiffComputed { .. }));
        if let Some(SendEvent::DiffComputed { total_entries, .. }) = diff_event {
            assert_eq!(*total_entries, 0);
        } else {
            panic!("Expected DiffComputed event");
        }

        assert!(
            send_recorded
                .iter()
                .any(|e| matches!(e, SendEvent::Progress { .. }))
        );
        assert!(
            send_recorded
                .iter()
                .any(|e| matches!(e, SendEvent::UpToDate))
        );

        let recv_recorded = recv_events.lock().unwrap();
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::UpToDate { subpath, meta } if subpath.as_os_str().is_empty() && meta.is_empty()
        )));
        assert!(
            recv_recorded
                .iter()
                .any(|e| matches!(e, RecvEvent::Finished))
        );
        assert!(!recv_recorded.iter().any(|e| matches!(
            e,
            RecvEvent::EntryPreApply { .. }
                | RecvEvent::EntryReceiving { .. }
                | RecvEvent::EntryApplied { .. }
                | RecvEvent::EntryPostApply { .. }
        )));

        Ok(())
    }

    struct TestProvider;

    impl MetadataProvider for TestProvider {
        fn pre_apply(&mut self, entries: &[snapshot::SnapshotEntry]) -> Metadata {
            Metadata::from([
                ("kind".into(), Value::from("pre")),
                ("count".into(), Value::from(entries.len() as u64)),
            ])
        }

        fn receiving(&mut self, entry: &snapshot::SnapshotEntry) -> Metadata {
            Metadata::from([
                ("kind".into(), Value::from("recv")),
                (
                    "path".into(),
                    Value::from(entry.path().to_string_lossy().into_owned()),
                ),
            ])
        }

        fn applied(&mut self, entry: &snapshot::SnapshotEntry) -> Metadata {
            Metadata::from([
                ("kind".into(), Value::from("applied")),
                (
                    "path".into(),
                    Value::from(entry.path().to_string_lossy().into_owned()),
                ),
            ])
        }

        fn post_apply(&mut self, _entries: &[snapshot::SnapshotEntry]) -> Metadata {
            Metadata::from([("kind".into(), Value::from("post"))])
        }
    }

    #[tokio::test]
    async fn test_metadata_provider_per_event() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;
        std::fs::write(send_dir.path().join("one.txt"), "1")?;
        std::fs::write(send_dir.path().join("two.txt"), "2")?;

        let recv_ep = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let send_ep = Endpoint::builder(presets::N0).bind().await?;

        let recv_events = Arc::new(Mutex::new(Vec::new()));
        let recv_events_c = Arc::clone(&recv_events);
        let recv_protocol =
            RecvProtocol::new(recv_dir.path().to_path_buf(), move |e: RecvEvent| {
                recv_events_c.lock().unwrap().push(e);
            });

        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;
        let send_handler = SendHandler::new(
            send_stream,
            recv_stream,
            send_dir.path().to_path_buf(),
            String::new(),
        )
        .await?
        .with_metadata_provider(TestProvider);

        send_handler.send_loop(|_: SendEvent| {}).await?;
        recv_handle.await?;

        let kind = |m: &Metadata| m.get("kind").and_then(Value::as_str).map(str::to_owned);
        let meta_path = |m: &Metadata| m.get("path").and_then(Value::as_str).map(str::to_owned);

        let recv_recorded = recv_events.lock().unwrap();
        let (mut pre, mut post, mut receiving, mut applied) = (0, 0, 0, 0);
        for e in recv_recorded.iter() {
            match e {
                RecvEvent::EntryPreApply { meta, .. } => {
                    pre += 1;
                    assert_eq!(kind(meta).as_deref(), Some("pre"));
                    assert_eq!(meta.get("count").and_then(Value::as_u64), Some(2));
                }
                RecvEvent::EntryPostApply { meta, .. } => {
                    post += 1;
                    assert_eq!(kind(meta).as_deref(), Some("post"));
                }
                RecvEvent::EntryReceiving { path, meta, .. } => {
                    receiving += 1;
                    assert_eq!(kind(meta).as_deref(), Some("recv"));
                    assert_eq!(meta_path(meta), Some(path.to_string_lossy().into_owned()));
                }
                RecvEvent::EntryApplied { path, meta, .. } => {
                    applied += 1;
                    assert_eq!(kind(meta).as_deref(), Some("applied"));
                    assert_eq!(meta_path(meta), Some(path.to_string_lossy().into_owned()));
                }
                _ => {}
            }
        }
        assert_eq!((pre, post, receiving, applied), (1, 1, 2, 2));
        Ok(())
    }

    struct UpToDateProvider {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl MetadataProvider for UpToDateProvider {
        fn pre_apply(&mut self, _entries: &[snapshot::SnapshotEntry]) -> Metadata {
            self.calls.lock().unwrap().push("pre_apply");
            Metadata::new()
        }

        fn post_apply(&mut self, _entries: &[snapshot::SnapshotEntry]) -> Metadata {
            self.calls.lock().unwrap().push("post_apply");
            Metadata::new()
        }

        fn up_to_date(&mut self) -> Metadata {
            self.calls.lock().unwrap().push("up_to_date");
            Metadata::from([("kind".into(), Value::from("up_to_date"))])
        }
    }

    #[tokio::test]
    async fn test_up_to_date_metadata() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;
        std::fs::write(recv_dir.path().join("same.txt"), "identical content")?;
        std::fs::write(send_dir.path().join("same.txt"), "identical content")?;

        let recv_ep = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let send_ep = Endpoint::builder(presets::N0).bind().await?;

        let recv_events = Arc::new(Mutex::new(Vec::new()));
        let recv_events_c = Arc::clone(&recv_events);
        let recv_protocol =
            RecvProtocol::new(recv_dir.path().to_path_buf(), move |e: RecvEvent| {
                recv_events_c.lock().unwrap().push(e);
            });

        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;
        let send_handler = SendHandler::new(
            send_stream,
            recv_stream,
            send_dir.path().to_path_buf(),
            String::new(),
        )
        .await?
        .with_metadata_provider(UpToDateProvider {
            calls: Arc::clone(&calls),
        });

        send_handler.send_loop(|_: SendEvent| {}).await?;
        recv_handle.await?;

        assert_eq!(*calls.lock().unwrap(), vec!["up_to_date"]);

        let expected = Metadata::from([("kind".into(), Value::from("up_to_date"))]);
        let recv_recorded = recv_events.lock().unwrap();
        assert!(recv_recorded.iter().any(|e| matches!(
            e, RecvEvent::UpToDate { meta, .. } if *meta == expected
        )));
        Ok(())
    }

    #[tokio::test]
    async fn test_create_missing_destination_when_source_empty() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;
        let destination = recv_dir.path().join("created/when/empty");

        let recv_ep = Endpoint::builder(presets::N0)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await?;
        let send_ep = Endpoint::builder(presets::N0).bind().await?;
        let recv_protocol = RecvProtocol::new(recv_dir.path().to_path_buf(), |_: RecvEvent| {});

        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;
        let send_handler = SendHandler::new(
            send_stream,
            recv_stream,
            send_dir.path().to_path_buf(),
            "created/when/empty".to_string(),
        )
        .await?;

        send_handler.send_loop(|_: SendEvent| {}).await?;
        recv_handle.await?;

        assert!(destination.is_dir());
        Ok(())
    }
}
