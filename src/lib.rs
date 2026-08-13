use std::collections::HashMap;

use crate::{
    snapshot::PathKey,
    sync::{EventStreamHandler, ReceiverToSender, RecvEvent, SendEvent, SenderToReceiver},
};

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
        let (send, recv) = connection.accept_bi().await?;

        let _ = self.tx.send_async(RecvEvent::Started).await;

        let mut ev_handler =
            EventStreamHandler::<ReceiverToSender, SenderToReceiver, RecvEvent>::new(
                send,
                recv,
                self.tx.clone(),
            );

        loop {
            match ev_handler.recv().await.unwrap() {
                SenderToReceiver::RequestSnapshot => {
                    let old_snapshot = crate::dirwalker::walkdir(&self.root).unwrap();
                    let old = old_snapshot
                        .into_iter()
                        .map(|x| eyre::Ok((PathKey::from_pathentry(&self.root, &x).unwrap(), x)))
                        .collect::<Result<HashMap<_, _>, eyre::Error>>()
                        .unwrap();

                    let entry_count = old.len();
                    ev_handler
                        .send(ReceiverToSender::Snapshot(old))
                        .await
                        .unwrap();

                    let _ = self
                        .tx
                        .send_async(RecvEvent::SnapshotSent { entry_count })
                        .await;
                }
                SenderToReceiver::SendPatch(items) => {
                    for i in items {
                        let path = i.path().to_path_buf();
                        let _ = self
                            .tx
                            .send_async(RecvEvent::EntryReceiving { path: path.clone() })
                            .await;

                        i.apply(&self.root).unwrap();

                        let _ = self.tx.send_async(RecvEvent::EntryApplied { path }).await;
                    }

                    ev_handler.send(ReceiverToSender::Ack).await.unwrap();
                    ev_handler.finish().await.unwrap();

                    let _ = self.tx.send_async(RecvEvent::Finished).await;

                    // Wait for sender to read Ack and close stream
                    let _ = ev_handler.recv().await;
                    break;
                }
                SenderToReceiver::Ack => {
                    ev_handler.finish().await.unwrap();
                    let _ = self.tx.send_async(RecvEvent::Finished).await;
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
        let _ = tx.send_async(SendEvent::Started).await;

        let mut ev_handler =
            EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                self.send,
                self.recv,
                tx.clone(),
            );

        ev_handler.send(SenderToReceiver::RequestSnapshot).await?;

        loop {
            match ev_handler.recv().await? {
                ReceiverToSender::Snapshot(old) => {
                    let _ = tx
                        .send_async(SendEvent::SnapshotReceived {
                            entry_count: old.len(),
                        })
                        .await;

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
                            snapshot::SnapshotEntry::Update { patch, .. } => patch.len() as u64,
                            snapshot::SnapshotEntry::Delete { .. } => 0,
                        })
                        .sum();

                    let _ = tx
                        .send_async(SendEvent::DiffComputed {
                            total_entries,
                            total_bytes,
                        })
                        .await;

                    ev_handler.send(SenderToReceiver::SendPatch(diff)).await?;
                }
                ReceiverToSender::Ack => {
                    ev_handler.finish().await?;
                    let _ = tx.send_async(SendEvent::Finished).await;
                    break;
                }
                ReceiverToSender::Error(err) => {
                    let _ = tx.send_async(SendEvent::Error(err.clone())).await;
                    eyre::bail!("Receiver error: {err}");
                }
            }
        }

        Ok(())
    }
}
