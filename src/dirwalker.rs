use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use cap_std::fs::Dir;
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
    root: PathBuf,
    cap_root: Option<Arc<Dir>>,
}

impl FileVisitor {
    fn verify_dir(&self, path: &Path) -> std::io::Result<()> {
        if let Some(root) = &self.cap_root {
            let relative = path
                .strip_prefix(&self.root)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            if !relative.as_os_str().is_empty() {
                root.open_dir(relative)?;
            }
        }
        Ok(())
    }

    fn open_file(&self, path: &Path) -> std::io::Result<std::fs::File> {
        if let Some(root) = &self.cap_root {
            let relative = path
                .strip_prefix(&self.root)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
            root.open(relative).map(cap_std::fs::File::into_std)
        } else {
            std::fs::File::open(path)
        }
    }
}

impl ParallelVisitor for FileVisitor {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> ignore::WalkState {
        match entry {
            Ok(e) => {
                let file_type = match e.file_type() {
                    Some(ft) => ft,
                    None => {
                        tracing::warn!(path = %e.path().display(), "Cannot determine file type");
                        return ignore::WalkState::Continue;
                    }
                };

                if self.cap_root.is_some() && file_type.is_symlink() {
                    tracing::debug!(path = %e.path().display(), "Skipping symlink in receiver snapshot");
                    return ignore::WalkState::Continue;
                }

                if file_type.is_dir() {
                    if let Err(err) = self.verify_dir(e.path()) {
                        tracing::warn!(
                            path = %e.path().display(),
                            ?err,
                            "Failed to open directory within receiver root"
                        );
                        return ignore::WalkState::Continue;
                    }
                    if let Err(err) = self.tx.send(PathEntry::Dir {
                        path: e.into_path(),
                    }) {
                        tracing::error!(
                            ?err,
                            "Failed to send DirEntry through channel"
                        );
                        ignore::WalkState::Quit
                    } else {
                        ignore::WalkState::Continue
                    }
                } else {
                    if !file_type.is_file() {
                        return ignore::WalkState::Continue;
                    }

                    let file = match self.open_file(e.path()) {
                        Ok(f) => f,
                        Err(err) => {
                            tracing::error!(
                                path = %e.path().display(),
                                ?err,
                                "Failed to open file during directory walk"
                            );
                            return ignore::WalkState::Continue;
                        }
                    };

                    let is_empty = file.metadata().map(|m| m.len() == 0).unwrap_or(false);
                    let chunks = if is_empty {
                        match crate::chunker::chunk(&[]) {
                            Ok(c) => c,
                            Err(err) => {
                                tracing::error!(
                                    path = %e.path().display(),
                                    ?err,
                                    "Failed to chunk empty file during directory walk"
                                );
                                return ignore::WalkState::Continue;
                            }
                        }
                    } else {
                        match unsafe { memmap2::Mmap::map(&file) } {
                            Ok(filebin) => match crate::chunker::chunk(&filebin) {
                                Ok(c) => c,
                                Err(err) => {
                                    tracing::error!(
                                        path = %e.path().display(),
                                        ?err,
                                        "Failed to chunk file during directory walk"
                                    );
                                    return ignore::WalkState::Continue;
                                }
                            },
                            Err(err) => {
                                tracing::error!(
                                    path = %e.path().display(),
                                    ?err,
                                    "Failed to memory map file during directory walk"
                                );
                                return ignore::WalkState::Continue;
                            }
                        }
                    };

                    let fileentry = PathEntry::File {
                        path: e.into_path(),
                        chunks,
                    };

                    if let Err(err) = self.tx.send(fileentry) {
                        tracing::error!(
                            ?err,
                            "Failed to send FileEntry through channel"
                        );
                        ignore::WalkState::Quit
                    } else {
                        ignore::WalkState::Continue
                    }
                }
            }
            Err(err) => {
                tracing::warn!(?err, "Failed to access path during directory walk");
                ignore::WalkState::Continue
            }
        }
    }
}

struct FileVisitorBuilder {
    tx: flume::Sender<PathEntry>,
    root: PathBuf,
    cap_root: Option<Arc<Dir>>,
}

impl<'s> ParallelVisitorBuilder<'s> for FileVisitorBuilder {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(FileVisitor {
            tx: self.tx.clone(),
            root: self.root.clone(),
            cap_root: self.cap_root.clone(),
        })
    }
}

pub fn walkdir(dir: impl Into<PathBuf>) -> crate::Result<Vec<PathEntry>> {
    walkdir_inner(dir.into(), None)
}

pub(crate) fn walkdir_cap(
    dir: Arc<Dir>,
    display_root: impl Into<PathBuf>,
) -> crate::Result<Vec<PathEntry>> {
    walkdir_inner(display_root.into(), Some(dir))
}

fn walkdir_inner(dir: PathBuf, cap_root: Option<Arc<Dir>>) -> crate::Result<Vec<PathEntry>> {
    if !dir.exists() {
        tracing::error!(path = %dir.display(), "Directory does not exist for walkdir");
        return Err(crate::Error::RootDoesNotExist(dir));
    }

    let (tx, rx) = flume::unbounded();

    let walk_dir = dir.clone();
    thread::spawn(move || {
        let mut builder = FileVisitorBuilder {
            tx,
            root: walk_dir.clone(),
            cap_root,
        };
        ignore::WalkBuilder::new(walk_dir)
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

    #[cfg(unix)]
    #[test]
    fn test_walkdir_cap_skips_symlink_files() -> crate::Result<()> {
        use cap_std::ambient_authority;
        use std::os::unix::fs::symlink;

        let root = tempdir()?;
        let outside = tempdir()?;
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&outside_file, "must not be read")?;
        symlink(&outside_file, root.path().join("link.txt"))?;

        let cap_root = Dir::open_ambient_dir(root.path(), ambient_authority())?;
        let entries = walkdir_cap(&cap_root, root.path())?;

        assert!(
            entries
                .iter()
                .all(|entry| !entry.into_path().ends_with("link.txt"))
        );
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

    #[test]
    fn test_walkdir_non_existent() {
        let res = walkdir("/non/existent/path/for/sure/12345");
        assert!(res.is_err());
    }
}
