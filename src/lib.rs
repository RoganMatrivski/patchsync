use std::collections::HashMap;

use crate::{
    snapshot::PathKey,
    sync::{EventStreamHandler, ReceiverToSender, RecvEvent, SendEvent, SenderToReceiver},
};

pub mod chunker;
pub mod dirwalker;
pub mod error;
pub mod snapshot;
pub mod sync;

pub use error::{Error, Result};

pub const ALPN: &[u8] = b"/id/my/rgmtrv/patchsync/0/";

#[derive(Debug)]
pub struct RecvProtocol {
    pub root: std::path::PathBuf,
    pub tx: flume::Sender<RecvEvent>,
}

impl RecvProtocol {
    pub fn new(root: std::path::PathBuf, tx: flume::Sender<RecvEvent>) -> Self {
        Self { root, tx }
    }
}

impl iroh::protocol::ProtocolHandler for RecvProtocol {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        tracing::info!(peer = %connection.remote_id(), "Accepting incoming connection for RecvProtocol");
        let (send, recv) = connection.accept_bi().await?;

        if let Err(e) = self.tx.send_async(RecvEvent::Started).await {
            tracing::warn!("Failed to send RecvEvent::Started: {e}");
        }

        let mut ev_handler =
            EventStreamHandler::<ReceiverToSender, SenderToReceiver, RecvEvent>::new(
                send,
                recv,
                self.tx.clone(),
            );

        loop {
            let msg = match ev_handler.recv().await {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::warn!("Connection closed or failed to receive message: {e}");
                    let _ = self.tx.send_async(RecvEvent::Error(e.to_string())).await;
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
                            let _ = self.tx.send_async(RecvEvent::Error(err_msg.clone())).await;
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        }
                    };

                    let old = match old_snapshot
                        .into_iter()
                        .map(|x| {
                            PathKey::from_pathentry(&self.root, &x)
                                .map(|k| (k, x))
                        })
                        .collect::<Result<HashMap<_, _>, crate::Error>>()
                    {
                        Ok(map) => map,
                        Err(e) => {
                            let err_msg = format!("Failed to build snapshot map: {e}");
                            tracing::error!("{err_msg}");
                            let _ = self.tx.send_async(RecvEvent::Error(err_msg.clone())).await;
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            break;
                        }
                    };

                    let entry_count = old.len();
                    tracing::debug!(entry_count, "Generated snapshot, sending to peer");

                    if let Err(e) = ev_handler.send(ReceiverToSender::Snapshot(old)).await {
                        tracing::error!("Failed to send snapshot to peer: {e}");
                        let _ = self.tx.send_async(RecvEvent::Error(e.to_string())).await;
                        break;
                    }

                    if let Err(e) = self
                        .tx
                        .send_async(RecvEvent::SnapshotSent { entry_count })
                        .await
                    {
                        tracing::warn!("Failed to send SnapshotSent event: {e}");
                    }
                }
                SenderToReceiver::SendPatch(items) => {
                    tracing::info!(item_count = items.len(), "Receiving patches from sender");

                    let mut apply_failed = false;
                    for item in items {
                        let path = item.path().to_path_buf();
                        tracing::debug!(path = %path.display(), "Receiving entry patch");

                        if let Err(e) = self
                            .tx
                            .send_async(RecvEvent::EntryReceiving { path: path.clone() })
                            .await
                        {
                            tracing::warn!("Failed to send EntryReceiving event: {e}");
                        }

                        if let Err(e) = item.apply(&self.root) {
                            let err_msg = format!(
                                "Failed to apply patch entry for {}: {e}",
                                path.display()
                            );
                            tracing::error!("{err_msg}");
                            let _ = self.tx.send_async(RecvEvent::Error(err_msg.clone())).await;
                            let _ = ev_handler.send(ReceiverToSender::Error(err_msg)).await;
                            apply_failed = true;
                            break;
                        }

                        tracing::debug!(path = %path.display(), "Patch entry applied successfully");
                        if let Err(e) = self.tx.send_async(RecvEvent::EntryApplied { path }).await {
                            tracing::warn!("Failed to send EntryApplied event: {e}");
                        }
                    }

                    if apply_failed {
                        break;
                    }

                    tracing::info!("All patches applied successfully, sending Ack");
                    if let Err(e) = ev_handler.send(ReceiverToSender::Ack).await {
                        tracing::warn!("Failed to send Ack: {e}");
                    }
                    if let Err(e) = ev_handler.finish().await {
                        tracing::warn!("Failed to finish stream: {e}");
                    }

                    if let Err(e) = self.tx.send_async(RecvEvent::Finished).await {
                        tracing::warn!("Failed to send RecvEvent::Finished: {e}");
                    }

                    // Wait for sender to read Ack and close stream
                    tracing::trace!("Waiting for sender to close stream after Ack");
                    let _ = ev_handler.recv().await;
                    tracing::info!("RecvProtocol stream completed cleanly");
                    break;
                }
                SenderToReceiver::Ack => {
                    tracing::trace!("Received Ack from peer");
                    let _ = ev_handler.finish().await;
                    if let Err(e) = self.tx.send_async(RecvEvent::Finished).await {
                        tracing::warn!("Failed to send RecvEvent::Finished: {e}");
                    }
                    break;
                }
            }
        }

        Ok(())
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

    pub async fn send_loop(self, tx: flume::Sender<SendEvent>) -> crate::Result<()> {
        tracing::info!(root = %self.root.display(), "Starting SendHandler sync loop");
        if let Err(e) = tx.send_async(SendEvent::Started).await {
            tracing::warn!("Failed to send SendEvent::Started: {e}");
        }

        let mut ev_handler =
            EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                self.send,
                self.recv,
                tx.clone(),
            );

        tracing::trace!("Sending RequestSnapshot to receiver");
        ev_handler.send(SenderToReceiver::RequestSnapshot).await?;

        loop {
            match ev_handler.recv().await? {
                ReceiverToSender::Snapshot(old) => {
                    tracing::debug!(entry_count = old.len(), "Received snapshot from receiver");
                    if let Err(e) = tx
                        .send_async(SendEvent::SnapshotReceived {
                            entry_count: old.len(),
                        })
                        .await
                    {
                        tracing::warn!("Failed to send SnapshotReceived event: {e}");
                    }

                    tracing::trace!("Walking local directory and computing diff");
                    let new_snapshot = crate::dirwalker::walkdir(&self.root)?;
                    let new = new_snapshot
                        .into_iter()
                        .map(|x| Ok((PathKey::from_pathentry(&self.root, &x)?, x)))
                        .collect::<Result<HashMap<_, _>, crate::Error>>()?;
                    let diff = crate::snapshot::diff(old, new)?;

                    if diff.is_empty() {
                        tracing::info!("No changes detected; notifying receiver and finishing sync");
                        if let Err(e) = tx
                            .send_async(SendEvent::DiffComputed {
                                total_entries: 0,
                                total_bytes: 0,
                            })
                            .await
                        {
                            tracing::warn!("Failed to send DiffComputed event: {e}");
                        }

                        ev_handler.send(SenderToReceiver::Ack).await?;
                        ev_handler.finish().await?;
                        if let Err(e) = tx.send_async(SendEvent::Finished).await {
                            tracing::warn!("Failed to send SendEvent::Finished: {e}");
                        }
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

                    if let Err(e) = tx
                        .send_async(SendEvent::DiffComputed {
                            total_entries,
                            total_bytes,
                        })
                        .await
                    {
                        tracing::warn!("Failed to send DiffComputed event: {e}");
                    }

                    tracing::debug!("Sending patch items to receiver");
                    ev_handler.send(SenderToReceiver::SendPatch(diff)).await?;
                }
                ReceiverToSender::Ack => {
                    tracing::info!("Received Ack from receiver, sync finished successfully");
                    ev_handler.finish().await?;
                    if let Err(e) = tx.send_async(SendEvent::Finished).await {
                        tracing::warn!("Failed to send SendEvent::Finished: {e}");
                    }
                    break;
                }
                ReceiverToSender::Error(err) => {
                    tracing::error!(error = %err, "Received error from receiver");
                    if let Err(e) = tx.send_async(SendEvent::Error(err.clone())).await {
                        tracing::warn!("Failed to send SendEvent::Error: {e}");
                    }
                    return Err(crate::Error::Receiver(err));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::endpoint::{presets, Endpoint};
    use iroh::protocol::ProtocolHandler;
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

        let (recv_tx, recv_rx) = flume::unbounded();
        let (send_tx, send_rx) = flume::unbounded();

        let recv_protocol = RecvProtocol::new(recv_dir.path().to_path_buf(), recv_tx);

        // Spawn receiver accept task
        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        // Connect sender
        let conn = send_ep
            .connect(recv_ep.addr(), ALPN)
            .await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;

        let send_handler = SendHandler::new(send_stream, recv_stream, send_dir.path().to_path_buf()).await?;
        send_handler.send_loop(send_tx).await?;

        recv_handle.await?;

        // Verify receiver files match sender state
        assert_eq!(std::fs::read_to_string(recv_dir.path().join("a.txt"))?, "new a content!");
        assert_eq!(std::fs::read_to_string(recv_dir.path().join("c.txt"))?, "newly created c");
        assert!(!recv_dir.path().join("b.txt").exists());

        // Verify events emitted
        let recv_events: Vec<_> = rx_drain(recv_rx);
        let send_events: Vec<_> = rx_drain(send_rx);

        assert!(recv_events.iter().any(|e| matches!(e, RecvEvent::Finished)));
        assert!(send_events.iter().any(|e| matches!(e, SendEvent::Finished)));

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

        let (recv_tx, _recv_rx) = flume::unbounded();
        let (send_tx, send_rx) = flume::unbounded();

        let recv_protocol = RecvProtocol::new(recv_dir.path().to_path_buf(), recv_tx);

        let recv_ep_clone = recv_ep.clone();
        let recv_handle = tokio::spawn(async move {
            let incoming = recv_ep_clone.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            recv_protocol.accept(conn).await.unwrap();
        });

        let conn = send_ep.connect(recv_ep.addr(), ALPN).await?;
        let (send_stream, recv_stream) = conn.open_bi().await?;

        let send_handler = SendHandler::new(send_stream, recv_stream, send_dir.path().to_path_buf()).await?;
        send_handler.send_loop(send_tx).await?;

        recv_handle.await?;

        let send_events: Vec<_> = rx_drain(send_rx);
        let diff_event = send_events.iter().find(|e| matches!(e, SendEvent::DiffComputed { .. }));
        if let Some(SendEvent::DiffComputed { total_entries, .. }) = diff_event {
            assert_eq!(*total_entries, 0);
        } else {
            panic!("Expected DiffComputed event");
        }

        Ok(())
    }

    fn rx_drain<T>(rx: flume::Receiver<T>) -> Vec<T> {
        rx.drain().collect()
    }
}


