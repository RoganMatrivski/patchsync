use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    io::Write,
    mem,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::dirwalker::PathEntry;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathKey(String);

impl<'p> From<&'p Path> for PathKey {
    fn from(value: &'p Path) -> Self {
        Self(
            value
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
        )
    }
}

impl From<PathBuf> for PathKey {
    fn from(value: PathBuf) -> Self {
        Self(
            value
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"),
        )
    }
}

#[allow(dead_code)]
impl PathKey {
    fn from_paths(root: &Path, full: &Path) -> Self {
        let rel = full.strip_prefix(root).unwrap();
        rel.into()
    }

    pub fn from_pathentry(root: impl AsRef<Path>, entry: &PathEntry) -> crate::Result<Self> {
        Ok(entry.into_path_root_trimmed(root.as_ref())?.into())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum PatchInstructs {
    Copy { offset: u64, length: u64 },
    Literal { data: Vec<u8> },
}

impl PatchInstructs {
    pub fn get_length(&self) -> u64 {
        match self {
            PatchInstructs::Copy { .. } => {
                0 // What's even the length of this over the wire
            }
            PatchInstructs::Literal { data } => data.len() as u64,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum SnapshotEntry {
    Create {
        path: PathBuf,
        bytes: Vec<u8>,
    },
    Delete {
        path: PathBuf,
    },
    Update {
        path: PathBuf,
        instructs: Vec<PatchInstructs>,
    },
}

impl fmt::Debug for SnapshotEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create { path, bytes } => f
                .debug_struct("Create")
                .field("path", path)
                .field("b_len", &bytes.len())
                .finish(),

            Self::Delete { path } => f.debug_struct("Delete").field("path", path).finish(),

            Self::Update { path, instructs } => f
                .debug_struct("Update")
                .field("path", path)
                .field("instructs_len", &instructs.len())
                .finish(),
        }
    }
}

impl SnapshotEntry {
    pub fn path(&self) -> &Path {
        match self {
            Self::Create { path, .. } => path,
            Self::Delete { path } => path,
            Self::Update { path, .. } => path,
        }
    }

    pub fn apply(self, root: impl AsRef<Path>) -> crate::Result<()> {
        if !root.as_ref().exists() {
            return Err(crate::Error::RootDoesNotExist(
                root.as_ref().to_path_buf(),
            ));
        }

        match self {
            SnapshotEntry::Create { path, bytes } => {
                let p = root.as_ref().join(path);
                if let Some(pdir) = p.parent() {
                    std::fs::create_dir_all(&pdir)?;
                }
                std::fs::write(p, bytes)?;
            }
            SnapshotEntry::Update { path, instructs } => {
                let p = root.as_ref().join(path);
                let file = std::fs::File::open(&p)?;

                // TODO: TODO: Replace with seek read.
                // TODO: Add feature flag to disable memmap2
                // Why tho
                // SAFETY: File opened for read-only. Shoulda been safe
                let filebin = unsafe { memmap2::Mmap::map(&file) }?;

                let mut newfilebin = vec![];

                for entry in instructs {
                    match entry {
                        PatchInstructs::Copy { offset, length } => {
                            let uoffset = offset as usize;
                            let ulength = length as usize;

                            if uoffset + ulength > filebin.len() {
                                return Err(crate::Error::CopyInstructionOutOfBounds {
                                    file_len: filebin.len(),
                                    start: uoffset,
                                    end: uoffset + ulength,
                                });
                            }

                            newfilebin.extend_from_slice(&filebin[uoffset..uoffset + ulength]);
                        }
                        PatchInstructs::Literal { data } => {
                            newfilebin.extend(data);
                        }
                    }
                }

                drop(filebin);
                drop(file);

                let parent = p
                    .parent()
                    .ok_or_else(|| crate::Error::NoParentDir(p.clone()))?;
                let mut tempfile = tempfile::NamedTempFile::new_in(parent)?;

                tempfile.write_all(&newfilebin)?;
                tempfile.persist(p)?;
            }
            SnapshotEntry::Delete { path } => {
                if path.as_os_str().is_empty() {
                    return Ok(());
                }
                let p = root.as_ref().join(path);
                if p.is_dir() {
                    std::fs::remove_dir_all(&p)?;
                } else {
                    match std::fs::remove_file(&p) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }

        Ok(())
    }
}

pub fn diff<K1, K2>(
    old: HashMap<K1, PathEntry>,
    new: HashMap<K2, PathEntry>,
) -> crate::Result<Vec<SnapshotEntry>>
where
    K1: Into<PathKey> + Eq + Hash,
    K2: Into<PathKey> + Eq + Hash,
{
    let old: HashMap<PathKey, PathEntry> = old.into_iter().map(|(k, v)| (k.into(), v)).collect();
    let new: HashMap<PathKey, PathEntry> = new.into_iter().map(|(k, v)| (k.into(), v)).collect();

    let mut entries = vec![];

    for (key, new_entry) in &new {
        match (old.get(key), new_entry) {
            // New file
            (None, PathEntry::File { path, .. }) => entries.push(SnapshotEntry::Create {
                path: PathBuf::from(&key.0),
                bytes: std::fs::read(path)?,
            }),
            (None, PathEntry::Dir { path: _ }) => {
                // No-op, probably will add new dir if needed
            }

            (
                Some(PathEntry::File {
                    chunks: old_chunks, ..
                }),
                PathEntry::File {
                    path: new_path,
                    chunks: new_chunks,
                },
            ) => {
                let old_chunkmap = old_chunks
                    .into_iter()
                    .map(crate::chunker::FileChunk::to_hashmap_kv)
                    .collect::<HashMap<_, _>>();

                let new_filebin = std::fs::read(new_path)?;
                let mut instructs = vec![];

                for crate::chunker::FileChunk {
                    hash,
                    length,
                    offset,
                } in new_chunks
                {
                    match old_chunkmap.get(hash) {
                        Some(old) => {
                            tracing::trace!(offset, length, "Chunk matched with old");
                            instructs.push(PatchInstructs::Copy {
                                offset: old.offset,
                                length: old.length,
                            });
                        }
                        None => {
                            tracing::debug!("Chunk not found. Creating patches");
                            let uoffset = *offset as usize;
                            let ulength = *length as usize;
                            let start = uoffset.min(new_filebin.len());
                            let end = (uoffset + ulength).min(new_filebin.len());
                            instructs.push(PatchInstructs::Literal {
                                data: new_filebin[start..end].to_vec(),
                            })
                        }
                    }
                }

                // Claude says: Since FastCDC will deterministically spits out
                //              same chunk length and order, just zip and do
                //              equal compare

                let unchanged = old_chunks.len() == new_chunks.len()
                    && old_chunks
                        .iter()
                        .zip(new_chunks.iter())
                        .all(|(o, n)| o.hash == n.hash);

                if unchanged {
                    continue;
                }

                tracing::debug!(len = instructs.len(), "Instructs not empty!");

                entries.push(SnapshotEntry::Update {
                    path: PathBuf::from(&key.0),
                    instructs,
                });
            }

            // The type changed, dir -> file and vice versa
            // Treated as delete then create
            // ngl rarely touched mem::* fns. this one new to me
            // Future me:   mem::discriminant used for
            //              comparing enum type regardless of data within
            (Some(old_e), new_e) if mem::discriminant(old_e) != mem::discriminant(new_e) => {
                entries.push(SnapshotEntry::Delete {
                    path: PathBuf::from(&key.0),
                });

                match new_e {
                    PathEntry::File { path, .. } => entries.push(SnapshotEntry::Create {
                        path: PathBuf::from(&key.0),
                        bytes: std::fs::read(path)?,
                    }),
                    PathEntry::Dir { .. } => {
                        // Again, no-op. will consider using this?
                    }
                }
            }
            _ => {}
        }
    }

    // Reverse of before
    // Essentially DELETE entries
    for (key, _old_entry) in &old {
        if !new.contains_key(key) {
            entries.push(SnapshotEntry::Delete {
                path: PathBuf::from(&key.0),
            });
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_path_key_conversions() -> crate::Result<()> {
        let p = Path::new("foo/bar/baz.txt");
        let pk1 = PathKey::from(p);
        let pk2 = PathKey::from(p.to_path_buf());
        assert_eq!(pk1, pk2);

        let root = Path::new("/root");
        let full = Path::new("/root/sub/file.txt");
        let pk_from_paths = PathKey::from_paths(root, full);
        assert_eq!(pk_from_paths.0, "sub/file.txt");

        let entry = PathEntry::File {
            path: full.to_path_buf(),
            chunks: vec![],
        };
        let pk_from_entry = PathKey::from_pathentry(root, &entry)?;
        assert_eq!(pk_from_entry.0, "sub/file.txt");

        Ok(())
    }

    #[test]
    fn test_patch_instructs_get_length() {
        let copy_inst = PatchInstructs::Copy {
            offset: 0,
            length: 100,
        };
        assert_eq!(copy_inst.get_length(), 0);

        let lit_inst = PatchInstructs::Literal {
            data: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(lit_inst.get_length(), 5);
    }

    #[test]
    fn test_snapshot_entry_path_and_debug() {
        let create = SnapshotEntry::Create {
            path: PathBuf::from("a.txt"),
            bytes: vec![1, 2],
        };
        assert_eq!(create.path(), Path::new("a.txt"));
        assert!(format!("{:?}", create).contains("Create"));

        let update = SnapshotEntry::Update {
            path: PathBuf::from("b.txt"),
            instructs: vec![],
        };
        assert_eq!(update.path(), Path::new("b.txt"));
        assert!(format!("{:?}", update).contains("Update"));

        let delete = SnapshotEntry::Delete {
            path: PathBuf::from("c.txt"),
        };
        assert_eq!(delete.path(), Path::new("c.txt"));
        assert!(format!("{:?}", delete).contains("Delete"));
    }

    #[test]
    fn test_apply_non_existent_root_bails() {
        let entry = SnapshotEntry::Delete {
            path: PathBuf::from("foo"),
        };
        assert!(entry.apply("/non/existent/path/for/sure").is_err());
    }

    #[test]
    fn test_apply_create_nested_dir() -> crate::Result<()> {
        let dir = tempdir()?;
        let create = SnapshotEntry::Create {
            path: PathBuf::from("nested/deep/file.txt"),
            bytes: b"deep data".to_vec(),
        };
        create.apply(dir.path())?;
        let target = dir.path().join("nested/deep/file.txt");
        assert_eq!(std::fs::read_to_string(target)?, "deep data");
        Ok(())
    }

    #[test]
    fn test_apply_update_copy_and_literal() -> crate::Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("file.txt");
        std::fs::write(&file_path, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")?;

        // Construct update: Copy 0..5 ("ABCDE"), Literal "123", Copy 20..26 ("UVWXYZ")
        let update = SnapshotEntry::Update {
            path: PathBuf::from("file.txt"),
            instructs: vec![
                PatchInstructs::Copy {
                    offset: 0,
                    length: 5,
                },
                PatchInstructs::Literal {
                    data: b"123".to_vec(),
                },
                PatchInstructs::Copy {
                    offset: 20,
                    length: 6,
                },
            ],
        };
        update.apply(dir.path())?;
        assert_eq!(std::fs::read_to_string(file_path)?, "ABCDE123UVWXYZ");
        Ok(())
    }

    #[test]
    fn test_apply_update_out_of_bounds_error() -> crate::Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("file.txt");
        std::fs::write(&file_path, "short")?;

        let update = SnapshotEntry::Update {
            path: PathBuf::from("file.txt"),
            instructs: vec![PatchInstructs::Copy {
                offset: 0,
                length: 1000,
            }],
        };
        assert!(update.apply(dir.path()).is_err());
        Ok(())
    }

    #[test]
    fn test_apply_delete_edge_cases() -> crate::Result<()> {
        let dir = tempdir()?;

        // Empty path delete
        let empty_del = SnapshotEntry::Delete {
            path: PathBuf::new(),
        };
        empty_del.apply(dir.path())?;

        // Non-existent file delete should not error
        let non_exist_del = SnapshotEntry::Delete {
            path: PathBuf::from("ghost.txt"),
        };
        non_exist_del.apply(dir.path())?;

        Ok(())
    }

    #[test]
    fn test_delete_directory_entry() -> crate::Result<()> {
        let dir = tempdir()?;
        let sub_dir = dir.path().join("subdir");
        std::fs::create_dir_all(&sub_dir)?;
        let file_path = sub_dir.join("test.txt");
        std::fs::write(&file_path, "hello")?;

        let old_entries = crate::dirwalker::walkdir(dir.path())?;
        let old_map = old_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        let new_map: HashMap<PathKey, PathEntry> = HashMap::new();
        let diff_entries = diff(old_map, new_map)?;

        // Apply diff on original directory
        for entry in diff_entries {
            entry.apply(dir.path())?;
        }

        assert!(!sub_dir.exists());
        Ok(())
    }

    #[test]
    fn test_dir_changed_to_file() -> crate::Result<()> {
        let dir = tempdir()?;
        let sub_dir = dir.path().join("item");
        std::fs::create_dir_all(&sub_dir)?;

        let old_entries = crate::dirwalker::walkdir(dir.path())?;
        let old_map = old_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Replace item directory with a file named item
        std::fs::remove_dir_all(&sub_dir)?;
        std::fs::write(&sub_dir, "content")?;

        let new_entries = crate::dirwalker::walkdir(dir.path())?;
        let new_map = new_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        let diff_entries = diff(old_map, new_map)?;

        // Re-create the dir structure to test applying patch
        std::fs::remove_file(&sub_dir)?;
        std::fs::create_dir_all(&sub_dir)?;

        for entry in diff_entries {
            entry.apply(dir.path())?;
        }

        assert!(sub_dir.is_file());
        assert_eq!(std::fs::read_to_string(&sub_dir)?, "content");
        Ok(())
    }

    #[test]
    fn test_file_changed_to_dir() -> crate::Result<()> {
        let dir = tempdir()?;
        let item_path = dir.path().join("item");
        std::fs::write(&item_path, "file content")?;

        let old_entries = crate::dirwalker::walkdir(dir.path())?;
        let old_map = old_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        std::fs::remove_file(&item_path)?;
        std::fs::create_dir_all(&item_path)?;

        let new_entries = crate::dirwalker::walkdir(dir.path())?;
        let new_map = new_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        let diff_entries = diff(old_map, new_map)?;

        // Must contain a Delete for item
        let has_delete = diff_entries.iter().any(|e| match e {
            SnapshotEntry::Delete { path } => path == Path::new("item"),
            _ => false,
        });
        assert!(has_delete);

        Ok(())
    }

    #[test]
    fn test_diff_multiple_files_with_unchanged() -> crate::Result<()> {
        let dir = tempdir()?;
        let file_a = dir.path().join("a.txt");
        let file_b = dir.path().join("b.txt");
        std::fs::write(&file_a, "initial content a")?;
        std::fs::write(&file_b, "initial content b")?;

        let old_entries = crate::dirwalker::walkdir(dir.path())?;
        let old_map = old_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        // Modify file_b, keep file_a unchanged
        std::fs::write(&file_b, "modified content b")?;

        let new_entries = crate::dirwalker::walkdir(dir.path())?;
        let new_map = new_entries
            .into_iter()
            .map(|x| PathKey::from_pathentry(dir.path(), &x).map(|k| (k, x)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        let diff_entries = diff(old_map, new_map)?;

        // Must contain an Update for b.txt and no entries for a.txt
        assert_eq!(diff_entries.len(), 1);
        assert_eq!(diff_entries[0].path(), Path::new("b.txt"));

        Ok(())
    }
}
