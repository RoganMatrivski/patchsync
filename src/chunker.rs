use fastcdc::v2020;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct FileChunk {
    pub hash: u64,
    pub offset: u64,
    pub length: u64,
}

impl From<v2020::Chunk> for FileChunk {
    fn from(value: v2020::Chunk) -> Self {
        // Downcasting like this is (probably) fine
        // If someone uses this crate in the near XX years i'll be impressed lmao
        Self {
            hash: value.hash,
            offset: value.offset as u64,
            length: value.length as u64,
        }
    }
}

impl FileChunk {
    pub fn to_hashmap_kv(&self) -> (u64, Self) {
        (self.hash, self.clone())
    }
}

pub fn chunk(data: &[u8]) -> Vec<FileChunk> {
    v2020::FastCDC::new(data, 2 << 13, 2 << 14, 2 << 16)
        .into_iter()
        .map(FileChunk::from)
        .collect::<Vec<_>>()
}
