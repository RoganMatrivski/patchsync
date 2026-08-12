use std::{collections::HashMap, path::PathBuf};

use iroh::endpoint::Connection;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    dirwalker::PathEntry,
    snapshot::{PathKey, SnapshotEntry},
};

const ALPN: &[u8] = b"/id/my/rgmtrv/patchsync/0/";

#[derive(Serialize, Deserialize)]
enum SenderToReceiver {
    RequestSnapshot,
    SendPatch(Vec<SnapshotEntry>),
}

#[derive(Serialize, Deserialize)]
enum ReceiverToSender {
    Snapshot(std::collections::HashMap<PathKey, PathEntry>),
    Ack,
    Error(String),
}

pub enum HandlerState {
    Init,
    RequestSnapshot,
    Compare(Vec<PathEntry>),
    SendPatch(Vec<SnapshotEntry>),
    Finish,
}

pub struct Handler {
    conn: Connection,
    root: PathBuf,
}

impl Handler {
    pub async fn new(conn: Connection, root: PathBuf) -> eyre::Result<Self> {
        Ok(Self { conn, root })
    }

    pub async fn send_loop(&mut self) -> eyre::Result<()> {
        let (ev_send, ev_recv) = self.conn.open_bi().await?;

        let mut ev_handler =
            EventStreamHandler::<SenderToReceiver, ReceiverToSender>::new(ev_send, ev_recv);

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

    pub async fn recv_loop(&mut self) -> eyre::Result<()> {
        let (ev_send, ev_recv) = self.conn.accept_bi().await?;
        let mut ev_handler =
            EventStreamHandler::<ReceiverToSender, SenderToReceiver>::new(ev_send, ev_recv);

        loop {
            match ev_handler.recv().await? {
                SenderToReceiver::RequestSnapshot => {
                    let old_snapshot = crate::dirwalker::walkdir(&self.root)?;
                    let old = old_snapshot
                        .into_iter()
                        .map(|x| eyre::Ok((PathKey::from_pathentry(&self.root, &x)?, x)))
                        .collect::<Result<HashMap<_, _>, eyre::Error>>()?;

                    ev_handler.send(ReceiverToSender::Snapshot(old)).await?;
                }
                SenderToReceiver::SendPatch(items) => {
                    todo!() // TODO: Handle things here
                }
            }
        }

        Ok(())
    }
}

struct EventStreamHandler<T, R> {
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,

    _t: std::marker::PhantomData<T>,
    _r: std::marker::PhantomData<R>,
}

impl<T, R> EventStreamHandler<T, R>
where
    T: Serialize,
    R: DeserializeOwned,
{
    pub fn new(send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream) -> Self {
        Self {
            send,
            recv,
            _t: std::marker::PhantomData,
            _r: std::marker::PhantomData,
        }
    }

    pub async fn send(&mut self, msg: T) -> eyre::Result<()> {
        Ok(send_msg(&mut self.send, &msg).await?)
    }

    pub async fn recv(&mut self) -> eyre::Result<R> {
        Ok(recv_msg(&mut self.recv).await?)
    }
}

async fn send_msg<T: Serialize>(
    stream: &mut iroh::endpoint::SendStream,
    msg: &T,
) -> eyre::Result<()> {
    let payload = postcard::to_stdvec(msg)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

async fn recv_msg<T: DeserializeOwned>(stream: &mut iroh::endpoint::RecvStream) -> eyre::Result<T> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(postcard::from_bytes(&payload)?)
}
