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

#[derive(Clone)]
pub enum SendEvent {
    Started,
    SnapshotReceived {
        entry_count: usize,
    },
    DiffComputed {
        total_entries: usize,
        total_bytes: u64,
    },
    EntryStarted {
        index: usize,
        path: PathBuf,
    },
    Progress {
        bytes: usize,
    },
    EntryFinished {
        index: usize,
    },
    Finished,
    Error(String),
}

#[derive(Clone)]
pub enum RecvEvent {
    Started,
    SnapshotSent { entry_count: usize },
    EntryReceiving { path: PathBuf },
    Progress { bytes: usize },
    EntryApplied { path: PathBuf },
    Finished,
    Error(String),
}

pub trait FromProgress {
    fn from_bytes(n: usize) -> Self;
}

impl FromProgress for SendEvent {
    fn from_bytes(n: usize) -> Self {
        SendEvent::Progress { bytes: n }
    }
}

impl FromProgress for RecvEvent {
    fn from_bytes(n: usize) -> Self {
        RecvEvent::Progress { bytes: n }
    }
}

pub struct Handler {
    conn: Connection,
    root: PathBuf,
}

impl Handler {
    pub async fn new(conn: Connection, root: PathBuf) -> eyre::Result<Self> {
        Ok(Self { conn, root })
    }

    pub async fn send_loop(&mut self, tx: flume::Sender<SendEvent>) -> eyre::Result<()> {
        let (ev_send, ev_recv) = self.conn.open_bi().await?;

        let mut ev_handler =
            EventStreamHandler::<SenderToReceiver, ReceiverToSender, SendEvent>::new(
                ev_send, ev_recv, tx,
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

    pub async fn recv_loop(&mut self, tx: flume::Sender<RecvEvent>) -> eyre::Result<()> {
        let (ev_send, ev_recv) = self.conn.accept_bi().await?;

        let mut ev_handler =
            EventStreamHandler::<ReceiverToSender, SenderToReceiver, RecvEvent>::new(
                ev_send, ev_recv, tx,
            );

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

struct EventStreamHandler<T, R, P> {
    send: TrackedStream<iroh::endpoint::SendStream, P>,
    recv: TrackedStream<iroh::endpoint::RecvStream, P>,

    _t: std::marker::PhantomData<T>,
    _r: std::marker::PhantomData<R>,
}

impl<T, R, P> EventStreamHandler<T, R, P>
where
    T: Serialize,
    R: DeserializeOwned,
    P: FromProgress + Clone,
{
    pub fn new(
        send: iroh::endpoint::SendStream,
        recv: iroh::endpoint::RecvStream,
        tracker_tx: flume::Sender<P>,
    ) -> Self {
        Self {
            send: TrackedStream::new(send, tracker_tx.clone()),
            recv: TrackedStream::new(recv, tracker_tx),
            _t: std::marker::PhantomData,
            _r: std::marker::PhantomData,
        }
    }

    pub async fn send(&mut self, msg: T) -> eyre::Result<()> {
        send_msg(&mut self.send, &msg).await
    }

    pub async fn recv(&mut self) -> eyre::Result<R> {
        recv_msg(&mut self.recv).await
    }
}

async fn send_msg<T, R, P>(stream: &mut TrackedStream<R, P>, msg: &T) -> eyre::Result<()>
where
    T: Serialize,
    R: tokio::io::AsyncWrite + Unpin,
    P: FromProgress + Clone,
{
    let payload = postcard::to_stdvec(msg)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

async fn recv_msg<T, R, P>(stream: &mut TrackedStream<R, P>) -> eyre::Result<T>
where
    T: DeserializeOwned,
    R: tokio::io::AsyncRead + Unpin,
    P: FromProgress + Clone,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact_tracked(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact_tracked(&mut payload).await?;
    Ok(postcard::from_bytes(&payload)?)
}

pub struct TrackedStream<S, E> {
    inner: S,
    tx: flume::Sender<E>,
}

impl<S, E> TrackedStream<S, E> {
    pub fn new(inner: S, tx: flume::Sender<E>) -> Self {
        Self { inner, tx }
    }
}

impl<S, E> TrackedStream<S, E>
where
    S: tokio::io::AsyncWrite + Unpin,
    E: FromProgress,
{
    pub async fn write_all(&mut self, data: &[u8]) -> eyre::Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut sent = 0;
        while sent < data.len() {
            let n = self.inner.write(&data[sent..]).await?;

            if n == 0 {
                eyre::bail!("stream closed before all bytes were written");
            }
            sent += n;

            // Swallow the error (probably log it too?)
            let _ = self.tx.send_async(E::from_bytes(n)).await;
        }

        Ok(())
    }
}

impl<S, E> TrackedStream<S, E>
where
    S: tokio::io::AsyncRead + Unpin,
    E: FromProgress,
{
    pub async fn read_exact_tracked(&mut self, buf: &mut [u8]) -> eyre::Result<()> {
        use tokio::io::AsyncReadExt;

        let mut received = 0;
        while received < buf.len() {
            let n = self.inner.read(&mut buf[received..]).await?;
            if n == 0 {
                eyre::bail!("stream closed before all bytes were received");
            }
            received += n;

            // Swallow the error (probably log it too?)
            let _ = self.tx.send_async(E::from_bytes(n)).await;
        }

        Ok(())
    }
}
