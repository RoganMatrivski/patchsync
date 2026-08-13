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

                    ev_handler
                        .send(ReceiverToSender::Snapshot(old))
                        .await
                        .unwrap();
                }
                SenderToReceiver::SendPatch(items) => {
                    for i in items {
                        i.apply(&self.root).unwrap();
                    }

                    ev_handler.send(ReceiverToSender::Ack).await.unwrap();
                    break;
                }
                SenderToReceiver::Ack => break,
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
        let mut ev_handler =
            EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                self.send, self.recv, tx,
            );

        ev_handler.send(SenderToReceiver::RequestSnapshot).await?;

        loop {
            match ev_handler.recv().await? {
                ReceiverToSender::Snapshot(old) => {
                    let new_snapshot = crate::dirwalker::walkdir(&self.root)?;
                    let new = new_snapshot
                        .into_iter()
                        .map(|x| eyre::Ok((PathKey::from_pathentry(&self.root, &x)?, x)))
                        .collect::<Result<HashMap<_, _>, eyre::Error>>()?;
                    let diff = crate::snapshot::diff(old, new)?;

                    // Will need devise a way to track patch send progress
                    // But for now, this'll do
                    ev_handler.send(SenderToReceiver::SendPatch(diff)).await?;
                }
                ReceiverToSender::Ack => break,
                ReceiverToSender::Error(_) => todo!(),
            }
        }

        Ok(())
    }
}
