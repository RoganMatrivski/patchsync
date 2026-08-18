use std::{path::PathBuf, thread};

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
    ) -> crate::Result<PathBuf> {
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
                    let file = std::fs::File::open(e.path()).expect("Failed to read file");

                    // TODO: Add feature flag to disable memmap2
                    // Why tho
                    // SAFETY: File opened for read-only. Shoulda been safe
                    let filebin = unsafe { memmap2::Mmap::map(&file) }.expect("Failed to map file");

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

pub fn walkdir(dir: impl Into<PathBuf>) -> crate::Result<Vec<PathEntry>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_path_entry_methods() -> crate::Result<()> {
        let root = PathBuf::from("/tmp/root");
        let dir_entry = PathEntry::Dir {
            path: root.join("sub/dir"),
        };
        assert_eq!(dir_entry.into_path(), root.join("sub/dir"));
        assert_eq!(
            dir_entry.into_path_root_trimmed(&root)?,
            PathBuf::from("sub/dir")
        );

        let file_entry = PathEntry::File {
            path: root.join("sub/file.txt"),
            chunks: vec![],
        };
        assert_eq!(file_entry.into_path(), root.join("sub/file.txt"));
        assert_eq!(
            file_entry.into_path_root_trimmed(&root)?,
            PathBuf::from("sub/file.txt")
        );

        let invalid_root = PathBuf::from("/other/root");
        assert!(dir_entry.into_path_root_trimmed(&invalid_root).is_err());

        Ok(())
    }

    #[test]
    fn test_walkdir_empty() -> crate::Result<()> {
        let dir = tempdir()?;
        let entries = walkdir(dir.path())?;
        // walkdir includes the root dir entry itself when ignore walks root
        assert!(!entries.is_empty());
        let root_dir_present = entries.iter().any(|e| match e {
            PathEntry::Dir { path } => path == dir.path(),
            _ => false,
        });
        assert!(root_dir_present);
        Ok(())
    }

    #[test]
    fn test_walkdir_nested_structure() -> crate::Result<()> {
        let dir = tempdir()?;
        let sub = dir.path().join("a/b");
        std::fs::create_dir_all(&sub)?;
        let file1 = sub.join("f1.txt");
        let file2 = dir.path().join("f2.txt");

        std::fs::write(&file1, "content1")?;
        std::fs::write(&file2, "content2")?;

        let entries = walkdir(dir.path())?;

        let mut found_f1 = false;
        let mut found_f2 = false;
        let mut found_sub = false;

        for entry in entries {
            match entry {
                PathEntry::File { path, chunks } => {
                    if path == file1 {
                        found_f1 = true;
                        assert_eq!(chunks.len(), 1);
                    } else if path == file2 {
                        found_f2 = true;
                        assert_eq!(chunks.len(), 1);
                    }
                }
                PathEntry::Dir { path } => {
                    if path == sub {
                        found_sub = true;
                    }
                }
            }
        }

        assert!(found_f1);
        assert!(found_f2);
        assert!(found_sub);

        Ok(())
    }
}

