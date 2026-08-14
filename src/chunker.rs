use fastcdc::v2020;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct FileChunk {
    pub hash: [u8; 32],
    pub offset: u64,
    pub length: u64,
}

impl FileChunk {
    pub fn to_hashmap_kv(&self) -> ([u8; 32], Self) {
        (self.hash, self.clone())
    }
}

pub fn chunk(data: &[u8]) -> Vec<FileChunk> {
    v2020::FastCDC::new(data, 2 << 13, 2 << 14, 2 << 16)
        .into_iter()
        .map(|x| {
            let (length, offset) = (x.length as u64, x.offset as u64);
            // TODO: Secure this
            let partial_data = &data[x.offset..(x.length + x.offset)];
            let hash = *blake3::hash(partial_data).as_bytes();

            FileChunk {
                hash,
                offset,
                length,
            }
        })
        .collect::<Vec<_>>()
}
