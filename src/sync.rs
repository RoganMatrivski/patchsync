use iroh::endpoint::Connection;

use crate::snapshot::SnapshotEntry;

const ALPN: &[u8] = b"/id/my/rgmtrv/patchsync/0/";

enum SenderToReceiver {
    RequestSnapshot,
}

enum ReceiverToSender {
    Snapshot(Vec<SnapshotEntry>),
    TotalProgress(u64),
    Progress(u64),
    Ack,
    Error(String),
}

pub struct Handler {
    conn: Connection,
}

impl Handler {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    pub fn send(&self) -> eyre::Result<()> {
        todo!()
    }

    pub fn receive(&self) -> eyre::Result<()> {
        todo!()
    }

    pub fn run(&self) -> eyre::Result<()> {
        // Main loop event goes here?
        todo!()
    }

    // Event handler probs goes here?
}

