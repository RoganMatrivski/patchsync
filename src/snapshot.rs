use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
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

    pub fn from_pathentry(root: impl AsRef<Path>, entry: &PathEntry) -> eyre::Result<Self> {
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

    pub fn apply(self, root: impl AsRef<Path>) -> eyre::Result<()> {
        if !root.as_ref().exists() {
            eyre::bail!(
                "{} not exist. Aborting patch",
                root.as_ref().to_string_lossy()
            )
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
                let filebin = std::fs::read(&p)?;
                let mut newfilebin = vec![];

                for entry in instructs {
                    match entry {
                        PatchInstructs::Copy { offset, length } => {
                            let uoffset = offset as usize;
                            let ulength = length as usize;

                            if uoffset + ulength > filebin.len() {
                                eyre::bail!(
                                    "Copy instruction bounds error: file len {}, requested range {}..{}",
                                    filebin.len(),
                                    uoffset,
                                    uoffset + ulength
                                );
                            }

                            newfilebin.extend_from_slice(&filebin[uoffset..uoffset + ulength]);
                        }
                        PatchInstructs::Literal { data } => {
                            newfilebin.extend(data);
                        }
                    }
                }

                std::fs::write(p, newfilebin)?;
            }
            SnapshotEntry::Delete { path } => {
                let p = root.as_ref().join(path);
                std::fs::remove_file(&p)?;
            }
        }

        Ok(())
    }
}

pub fn diff<K1, K2>(
    old: HashMap<K1, PathEntry>,
    new: HashMap<K2, PathEntry>,
) -> eyre::Result<Vec<SnapshotEntry>>
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
                    match old_chunkmap.get(&hash) {
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
