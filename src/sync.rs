use std::path::PathBuf;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    dirwalker::PathEntry,
    snapshot::{PathKey, SnapshotEntry},
};

#[derive(Serialize, Deserialize)]
pub enum SenderToReceiver {
    RequestSnapshot,
    SendPatch(Vec<SnapshotEntry>),
    Ack,
}

#[derive(Serialize, Deserialize)]
pub enum ReceiverToSender {
    Snapshot(std::collections::HashMap<PathKey, PathEntry>),
    Ack,
    Error(String),
}

#[derive(Clone, Debug)]
pub enum SendEvent {
    Started,
    SnapshotReceived {
        entry_count: usize,
    },
    DiffComputed {
        total_entries: usize,
        total_bytes: u64,
    },
    Progress {
        bytes: usize,
    },
    Finished,
    Error(String),
}

#[derive(Clone, Debug)]
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

pub struct EventStreamHandler<T, R, P> {
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

    pub async fn send(&mut self, msg: T) -> crate::Result<()> {
        send_msg(&mut self.send, &msg).await
    }

    pub async fn recv(&mut self) -> crate::Result<R> {
        recv_msg(&mut self.recv).await
    }

    pub async fn finish(&mut self) -> crate::Result<()> {
        self.send.finish().await
    }
}

impl<E> TrackedStream<iroh::endpoint::SendStream, E> {
    pub async fn finish(&mut self) -> crate::Result<()> {
        self.inner.finish()?;
        Ok(())
    }
}

async fn send_msg<T, R, P>(stream: &mut TrackedStream<R, P>, msg: &T) -> crate::Result<()>
where
    T: Serialize,
    R: tokio::io::AsyncWrite + Unpin,
    P: FromProgress + Clone,
{
    let payload = postcard::to_stdvec(msg)?;
    let len = payload.len() as u32;
    tracing::trace!(len = len, "Sending message over stream");
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

async fn recv_msg<T, R, P>(stream: &mut TrackedStream<R, P>) -> crate::Result<T>
where
    T: DeserializeOwned,
    R: tokio::io::AsyncRead + Unpin,
    P: FromProgress + Clone,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact_tracked(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    tracing::trace!(len = len, "Receiving message payload");
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
    pub async fn write_all(&mut self, data: &[u8]) -> crate::Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut sent = 0;
        while sent < data.len() {
            let n = self.inner.write(&data[sent..]).await?;

            if n == 0 {
                tracing::error!("Stream closed unexpectedly before all bytes were written");
                return Err(crate::Error::StreamClosedWrite);
            }
            sent += n;
            tracing::trace!(bytes = n, "Stream wrote bytes");

            if let Err(e) = self.tx.send_async(E::from_bytes(n)).await {
                tracing::warn!("Failed to send progress event to channel: {e}");
            }
        }

        Ok(())
    }
}

impl<S, E> TrackedStream<S, E>
where
    S: tokio::io::AsyncRead + Unpin,
    E: FromProgress,
{
    pub async fn read_exact_tracked(&mut self, buf: &mut [u8]) -> crate::Result<()> {
        use tokio::io::AsyncReadExt;

        let mut received = 0;
        while received < buf.len() {
            let n = self.inner.read(&mut buf[received..]).await?;
            if n == 0 {
                tracing::debug!("Stream closed during read (EOF or peer shutdown)");
                return Err(crate::Error::StreamClosedRead);
            }
            received += n;
            tracing::trace!(bytes = n, "Stream read bytes");

            if let Err(e) = self.tx.send_async(E::from_bytes(n)).await {
                tracing::warn!("Failed to send progress event to channel: {e}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn test_from_progress_events() {
        let se = SendEvent::from_bytes(100);
        match se {
            SendEvent::Progress { bytes } => assert_eq!(bytes, 100),
            _ => panic!("Expected SendEvent::Progress"),
        }

        let re = RecvEvent::from_bytes(200);
        match re {
            RecvEvent::Progress { bytes } => assert_eq!(bytes, 200),
            _ => panic!("Expected RecvEvent::Progress"),
        }
    }

    #[tokio::test]
    async fn test_tracked_stream_and_msg_roundtrip() -> crate::Result<()> {
        let (client_io, server_io) = duplex(1024);

        let (tx_send, rx_send) = flume::unbounded::<SendEvent>();
        let (tx_recv, rx_recv) = flume::unbounded::<RecvEvent>();

        let mut client_stream = TrackedStream::new(client_io, tx_send);
        let mut server_stream = TrackedStream::new(server_io, tx_recv);

        let sent_msg = SenderToReceiver::RequestSnapshot;

        // Send msg from client to server
        tokio::spawn(async move {
            send_msg(&mut client_stream, &sent_msg).await.unwrap();
        });

        let recv_msg: SenderToReceiver = recv_msg(&mut server_stream).await?;
        match recv_msg {
            SenderToReceiver::RequestSnapshot => {}
            _ => panic!("Expected RequestSnapshot"),
        }

        // Verify progress events emitted
        let send_progress: Vec<_> = rx_send.drain().collect();
        let recv_progress: Vec<_> = rx_recv.drain().collect();
        assert!(!send_progress.is_empty());
        assert!(!recv_progress.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_tracked_stream_read_write_closed() -> crate::Result<()> {
        let (client_io, server_io) = duplex(1024);
        let (tx1, _rx1) = flume::unbounded::<SendEvent>();
        let (tx2, _rx2) = flume::unbounded::<RecvEvent>();

        let mut client_stream = TrackedStream::new(client_io, tx1);
        let server_stream = TrackedStream::new(server_io, tx2);

        // Close server side
        drop(server_stream);

        // Writing or reading should fail
        let mut buf = [0u8; 10];
        assert!(client_stream.read_exact_tracked(&mut buf).await.is_err());

        Ok(())
    }
}
