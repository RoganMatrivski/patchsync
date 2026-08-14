use std::collections::HashMap;

use crate::{
    snapshot::PathKey,
    sync::{EventStreamHandler, ReceiverToSender, RecvEvent, SendEvent, SenderToReceiver},
};

pub mod chunker;
pub mod dirwalker;
pub mod snapshot;
pub mod sync;

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
                                .map_err(eyre::Error::from)
                        })
                        .collect::<Result<HashMap<_, _>, eyre::Error>>()
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
                            let chain = e
                                .chain()
                                .map(|c| c.to_string())
                                .collect::<Vec<_>>()
                                .join(": ");
                            let err_msg = format!(
                                "Failed to apply patch entry for {}: {chain}",
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
    ) -> eyre::Result<Self> {
        Ok(Self { send, recv, root })
    }

    pub async fn send_loop(self, tx: flume::Sender<SendEvent>) -> eyre::Result<()> {
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
                        .map(|x| eyre::Ok((PathKey::from_pathentry(&self.root, &x)?, x)))
                        .collect::<Result<HashMap<_, _>, eyre::Error>>()?;
                    let diff = crate::snapshot::diff(old, new)?;

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
                    eyre::bail!("Receiver error: {err}");
                }
            }
        }

        Ok(())
    }
}
