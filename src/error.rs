use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Path prefix error: {0}")]
    StripPrefix(#[from] std::path::StripPrefixError),

    #[error("Postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("Iroh write error: {0}")]
    IrohWrite(#[from] iroh::endpoint::WriteError),

    #[error("Iroh closed stream error: {0}")]
    IrohClosedStream(#[from] iroh::endpoint::ClosedStream),

    #[error("Iroh bind error: {0}")]
    IrohBind(#[from] iroh::endpoint::BindError),

    #[error("Iroh connect error: {0}")]
    IrohConnect(#[from] iroh::endpoint::ConnectError),

    #[error("Iroh connection error: {0}")]
    IrohConnection(#[from] iroh::endpoint::ConnectionError),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("Root directory does not exist: {0}")]
    RootDoesNotExist(PathBuf),

    #[error("Copy instruction bounds error: file len {file_len}, requested range {start}..{end}")]
    CopyInstructionOutOfBounds {
        file_len: usize,
        start: usize,
        end: usize,
    },

    #[error("Stream closed before all bytes were written")]
    StreamClosedWrite,

    #[error("Stream closed before all bytes were received")]
    StreamClosedRead,

    #[error("Receiver error: {0}")]
    Receiver(String),

    #[error("Path parent not found for {0}")]
    NoParentDir(PathBuf),

    #[error("Failed to persist temp file: {0}")]
    Persist(#[from] tempfile::PersistError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
