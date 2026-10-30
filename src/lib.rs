use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    event::Dispatcher,
    snapshot::PathKey,
    sync::{EventStreamHandler, ReceiverToSender, RecvEvent, SendEvent, SenderToReceiver},
};

pub mod chunker;
pub mod dirwalker;
pub mod error;
pub mod event;
pub mod snapshot;
pub mod sync;

pub use error::{Error, Result};
pub use event::EventHandler;

pub const ALPN: &[u8] = b"/id/my/rgmtrv/patchsync/0/";

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
                    SenderToReceiver::RequestSnapshot => {
                        tracing::trace!("Received RequestSnapshot from sender");
                        let old_snapshot = match crate::dirwalker::walkdir(&self.root) {
                            Ok(s) => s,
                            Err(e) => {
                                let err_msg = format!("Failed to walk root directory: {e}");
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                break;
                            }
                        };

                        let old = match old_snapshot
                            .into_iter()
                            .map(|x| PathKey::from_pathentry(&self.root, &x).map(|k| (k, x)))
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

                        sink.emit(RecvEvent::SnapshotSent { entry_count });
                    }
                    SenderToReceiver::SendPatch(items) => {
                        tracing::info!(item_count = items.len(), "Receiving patches from sender");

                        sink.emit(RecvEvent::EntryPreApply);

                        let mut apply_failed = false;
                        for item in items {
                            let path = item.path().to_path_buf();
                            tracing::debug!(path = %path.display(), "Receiving entry patch");

                            sink.emit(RecvEvent::EntryReceiving { path: path.clone() });

                            if let Err(e) = item.apply(&self.root) {
                                let err_msg =
                                    format!("Failed to apply patch entry for {}: {e}", path.display());
                                tracing::error!("{err_msg}");
                                sink.emit(RecvEvent::Error(err_msg.clone()));
                                let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                                apply_failed = true;
                                break;
                            }

                            tracing::debug!(path = %path.display(), "Patch entry applied successfully");
                            sink.emit(RecvEvent::EntryApplied { path });
                        }

                        if apply_failed {
                            break;
                        }

                        sink.emit(RecvEvent::EntryPostApply);

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
                    SenderToReceiver::Ack => {
                        tracing::trace!("Received Ack from peer");
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
}

impl SendHandler {
    pub async fn new(
        send: iroh::endpoint::SendStream,
        recv: iroh::endpoint::RecvStream,
        root: std::path::PathBuf,
    ) -> crate::Result<Self> {
        Ok(Self { send, recv, root })
    }

    pub async fn send_loop(self, handler: impl EventHandler<SendEvent>) -> crate::Result<()> {
        let dispatcher = Dispatcher::spawn(handler);
        let sink = dispatcher.sink();

        sink.emit(SendEvent::Started);

        let result = async {
            tracing::info!(root = %self.root.display(), "Starting SendHandler sync loop");

            let mut ev_handler =
                EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                    self.send,
                    self.recv,
                    sink.clone(),
                );

            tracing::trace!("Sending RequestSnapshot to receiver");
            ev_handler.send(SenderToReceiver::RequestSnapshot).await?;

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

                            ev_handler.send(SenderToReceiver::Ack).await?;
                            ev_handler.finish().await?;
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

                        tracing::debug!("Sending patch items to receiver");
                        ev_handler.send(SenderToReceiver::SendPatch(diff)).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::endpoint::{Endpoint, presets};
    use iroh::protocol::ProtocolHandler;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_end_to_end_sync_protocol() -> crate::Result<()> {
        let recv_dir = tempdir()?;
        let send_dir = tempdir()?;

        // Receiver state: a.txt ("old a"), b.txt ("will delete")
        let recv_a = recv_dir.path().join("a.txt");
        let recv_b = recv_dir.path().join("b.txt");
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

        let send_handler =
            SendHandler::new(send_stream, recv_stream, send_dir.path().to_path_buf()).await?;
        send_handler.send_loop(send_handler_fn).await?;

        recv_handle.await?;

        // Verify receiver files match sender state
        assert_eq!(
            std::fs::read_to_string(recv_dir.path().join("a.txt"))?,
            "new a content!"
        );
        assert_eq!(
            std::fs::read_to_string(recv_dir.path().join("c.txt"))?,
            "newly created c"
        );
        assert!(!recv_dir.path().join("b.txt").exists());

        // Verify events emitted
        let recv_recorded = recv_events.lock().unwrap();
        let send_recorded = send_events.lock().unwrap();

        assert!(
            recv_recorded
                .iter()
                .any(|e| matches!(e, RecvEvent::Finished))
        );
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

        let recv_handler = |_: RecvEvent| {};
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

        let send_handler =
            SendHandler::new(send_stream, recv_stream, send_dir.path().to_path_buf()).await?;
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

        Ok(())
    }
}
