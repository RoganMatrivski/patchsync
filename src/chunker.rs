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

pub fn chunk(data: &[u8]) -> crate::Result<Vec<FileChunk>> {
    v2020::FastCDC::new(data, 2 << 13, 2 << 14, 2 << 16)
        .into_iter()
        .map(|x| {
            let offset = x.offset as usize;
            let length = x.length as usize;

            let end = offset
                .checked_add(length)
                .ok_or(crate::Error::CopyInstructionOverflow { offset, length })?;

            let partial_data =
                data.get(offset..end)
                    .ok_or(crate::Error::CopyInstructionOutOfBounds {
                        file_len: data.len(),
                        start: offset,
                        end,
                    })?;

            let hash = *blake3::hash(partial_data).as_bytes();

            let offset = x.offset as u64;
            let length = x.length as u64;

            Ok(FileChunk {
                hash,
                offset,
                length,
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_empty_data() -> crate::Result<()> {
        let chunks = chunk(&[])?;
        assert!(chunks.is_empty());
        Ok(())
    }

    #[test]
    fn test_chunk_small_data() -> crate::Result<()> {
        let data = b"hello patchsync world";
        let chunks = chunk(data)?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].length, data.len() as u64);
        assert_eq!(chunks[0].hash, *blake3::hash(data).as_bytes());
        Ok(())
    }

    #[test]
    fn test_chunk_large_data() -> crate::Result<()> {
        // Generate 256KB pattern
        let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        let chunks = chunk(&data)?;
        assert!(!chunks.is_empty());

        let mut covered_bytes = 0u64;
        for c in &chunks {
            assert_eq!(c.offset, covered_bytes);
            let slice = &data[c.offset as usize..(c.offset + c.length) as usize];
            assert_eq!(c.hash, *blake3::hash(slice).as_bytes());
            covered_bytes += c.length;
        }
        assert_eq!(covered_bytes, data.len() as u64);
        Ok(())
    }

    #[test]
    fn test_to_hashmap_kv() {
        let chunk_item = FileChunk {
            hash: [42u8; 32],
            offset: 10,
            length: 100,
        };
        let (hash, val) = chunk_item.to_hashmap_kv();
        assert_eq!(hash, [42u8; 32]);
        assert_eq!(val.offset, 10);
        assert_eq!(val.length, 100);
    }
}
