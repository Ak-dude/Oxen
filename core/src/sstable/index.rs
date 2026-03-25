use crate::error::{OxenError, Result};

/// One entry in the sparse index: the last (largest) key in a data block
/// plus the block's byte offset and size in the SSTable file.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Last key stored in the corresponding data block
    pub last_key: Vec<u8>,
    /// Byte offset of the data block in the SSTable file
    pub block_offset: u64,
    /// Byte length of the serialised data block (as written — may be compressed)
    pub block_size: u32,
    /// Whether the block was LZ4-compressed
    pub compressed: bool,
}

/// Sparse index block — one entry per data block.
///
/// Wire format:
/// ```text
/// [num_entries: u32 LE]
/// For each entry:
///   [key_len: u32 LE][key: bytes][block_offset: u64 LE][block_size: u32 LE][compressed: u8]
/// ```
pub struct IndexBlock {
    pub entries: Vec<IndexEntry>,
}

impl IndexBlock {
    pub fn new() -> Self {
        IndexBlock { entries: Vec::new() }
    }

    pub fn add_entry(&mut self, last_key: Vec<u8>, block_offset: u64, block_size: u32, compressed: bool) {
        self.entries.push(IndexEntry {
            last_key,
            block_offset,
            block_size,
            compressed,
        });
    }

    /// Serialise to bytes for writing into the SSTable file.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            buf.extend_from_slice(&(e.last_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&e.last_key);
            buf.extend_from_slice(&e.block_offset.to_le_bytes());
            buf.extend_from_slice(&e.block_size.to_le_bytes());
            buf.push(e.compressed as u8);
        }
        buf
    }

    /// Deserialise from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(OxenError::Corruption("IndexBlock too short".into()));
        }
        let num_entries = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(num_entries);
        let mut pos = 4usize;

        for _ in 0..num_entries {
            if pos + 4 > data.len() {
                return Err(OxenError::Corruption("IndexBlock entry truncated at key_len".into()));
            }
            let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + key_len + 13 > data.len() {
                return Err(OxenError::Corruption("IndexBlock entry truncated".into()));
            }
            let last_key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            let block_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let block_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let compressed = data[pos] != 0;
            pos += 1;

            entries.push(IndexEntry { last_key, block_offset, block_size, compressed });
        }

        Ok(IndexBlock { entries })
    }

    /// Binary search: return the index of the first entry whose `last_key >= key`.
    /// Returns `entries.len()` if all entries are smaller than `key`.
    pub fn find_block_for_key(&self, key: &[u8]) -> usize {
        let mut lo = 0;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.entries[mid].last_key.as_slice() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

impl Default for IndexBlock {
    fn default() -> Self {
        IndexBlock::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let mut idx = IndexBlock::new();
        idx.add_entry(b"apple".to_vec(), 0, 512, false);
        idx.add_entry(b"mango".to_vec(), 512, 768, true);
        idx.add_entry(b"zebra".to_vec(), 1280, 256, false);

        let encoded = idx.encode();
        let decoded = IndexBlock::decode(&encoded).unwrap();
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[1].last_key, b"mango".to_vec());
        assert_eq!(decoded.entries[1].block_offset, 512);
        assert!(decoded.entries[1].compressed);
    }

    #[test]
    fn find_block_for_key() {
        let mut idx = IndexBlock::new();
        idx.add_entry(b"f".to_vec(), 0, 100, false);
        idx.add_entry(b"m".to_vec(), 100, 100, false);
        idx.add_entry(b"z".to_vec(), 200, 100, false);

        assert_eq!(idx.find_block_for_key(b"a"), 0); // before first
        assert_eq!(idx.find_block_for_key(b"f"), 0); // exact first
        assert_eq!(idx.find_block_for_key(b"g"), 1); // between f and m
        assert_eq!(idx.find_block_for_key(b"z"), 2); // exact last
        assert_eq!(idx.find_block_for_key(b"zzz"), 3); // beyond last
    }
}
