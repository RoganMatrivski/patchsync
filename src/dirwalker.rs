use std::{fs::File, io::BufReader, path::PathBuf, thread};

use ignore::{ParallelVisitor, ParallelVisitorBuilder};

use crate::chunker::FileChunk;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PathEntry {
    Dir {
        path: PathBuf,
    },
    File {
        path: PathBuf,
        chunks: Vec<FileChunk>,
    },
}

impl PathEntry {
    pub fn into_path(&self) -> PathBuf {
        match self {
            PathEntry::Dir { path } => path.to_path_buf(),
            PathEntry::File { path, .. } => path.to_path_buf(),
        }
    }

    pub fn into_path_root_trimmed(
        &self,
        root: impl AsRef<std::path::Path>,
    ) -> eyre::Result<PathBuf> {
        Ok(self.into_path().strip_prefix(root)?.to_owned())
    }
}

struct FileVisitor {
    tx: flume::Sender<PathEntry>,
}

impl ParallelVisitor for FileVisitor {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> ignore::WalkState {
        match entry {
            Ok(e) => {
                if e.file_type().expect("TODO: 'Path' is an stdin").is_dir() {
                    if let Err(e) = self.tx.send(PathEntry::Dir {
                        path: e.into_path(),
                    }) {
                        tracing::error!(
                            ?e,
                            "Failed to send DirEntry. This should not have happened"
                        );
                        ignore::WalkState::Quit
                    } else {
                        ignore::WalkState::Continue
                    }
                } else {
                    // TODO: Replace with memmap2 thingy
                    let filebin = std::fs::read(e.path()).expect("Failed to read file");

                    let fileentry = PathEntry::File {
                        path: e.into_path(),
                        chunks: crate::chunker::chunk(&filebin),
                    };

                    if let Err(e) = self.tx.send(fileentry) {
                        tracing::error!(
                            ?e,
                            "Failed to send DirEntry. This should not have happened"
                        );
                        ignore::WalkState::Quit
                    } else {
                        ignore::WalkState::Continue
                    }
                }
            }
            Err(err) => {
                tracing::warn!(?err, "Failed to access");
                ignore::WalkState::Continue
            }
        }
    }
}

struct FileVisitorBuilder {
    tx: flume::Sender<PathEntry>,
}

impl<'s> ParallelVisitorBuilder<'s> for FileVisitorBuilder {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(FileVisitor {
            tx: self.tx.clone(),
        })
    }
}

pub fn walkdir(dir: impl Into<PathBuf>) -> eyre::Result<Vec<PathEntry>> {
    let (tx, rx) = flume::unbounded();

    let dir = dir.into();
    thread::spawn(move || {
        let mut builder = FileVisitorBuilder { tx };
        ignore::WalkBuilder::new(dir)
            .git_ignore(false)
            .build_parallel()
            .visit(&mut builder);
    });

    Ok(rx.into_iter().collect::<Vec<_>>())
}
