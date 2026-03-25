use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::cache::BlockCache;
use crate::error::{OxenError, Result};
use crate::sstable::block::BlockReader;
use crate::sstable::bloom::BloomFilter;
use crate::sstable::builder::{FOOTER_SIZE, SSTABLE_MAGIC};
use crate::sstable::index::IndexBlock;

/// Reads key-value pairs from a single SSTable file.
pub struct SSTableReader {
    file_number: u64,
    file: File,
    index: IndexBlock,
    bloom: Option<BloomFilter>,
    cache: Option<Arc<BlockCache>>,
}

impl SSTableReader {
    /// Open an SSTable file.  Optionally attach a shared block cache.
    pub fn open(path: &Path, file_number: u64, cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let mut file = File::open(path).map_err(OxenError::Io)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(OxenError::Corruption(format!(
                "SSTable file too small: {} bytes",
                file_size
            )));
        }

        // Read and validate footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(OxenError::Io)?;
        let mut footer = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer).map_err(OxenError::Io)?;

        let magic = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(OxenError::Corruption(format!(
                "SSTable magic mismatch: {magic:#018x}"
            )));
        }

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_size = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(footer[12..20].try_into().unwrap());
        let bloom_size = u32::from_le_bytes(footer[20..24].try_into().unwrap()) as usize;

        // Read index block
        let index = {
            let mut buf = vec![0u8; index_size];
            file.seek(SeekFrom::Start(index_offset))
                .map_err(OxenError::Io)?;
            file.read_exact(&mut buf).map_err(OxenError::Io)?;
            IndexBlock::decode(&buf)?
        };

        // Read bloom filter (best-effort)
        let bloom = if bloom_size > 0 {
            let mut buf = vec![0u8; bloom_size];
            file.seek(SeekFrom::Start(bloom_offset))
                .map_err(OxenError::Io)?;
            file.read_exact(&mut buf).map_err(OxenError::Io)?;
            BloomFilter::decode(&buf)
        } else {
            None
        };

        Ok(SSTableReader { file_number, file, index, bloom, cache })
    }

    /// Point lookup.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Bloom filter gate
        if let Some(ref bloom) = self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        let block_idx = self.index.find_block_for_key(key);
        if block_idx >= self.index.entries.len() {
            return Ok(None);
        }

        // Copy entry data to avoid holding a reference into self while calling read_block
        let (block_offset, block_size, compressed) = {
            let e = &self.index.entries[block_idx];
            (e.block_offset, e.block_size, e.compressed)
        };

        let block = self.read_block(block_offset, block_size, compressed)?;
        Ok(block.get(key))
    }

    /// Range scan over `[start, end)`.
    pub fn scan(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Determine starting block index
        let start_block_idx = match start {
            Some(s) => {
                let idx = self.index.find_block_for_key(s);
                if idx > 0 { idx - 1 } else { 0 }
            }
            None => 0,
        };

        // Snapshot the index entries we care about (offset/size/compressed) to avoid
        // borrow conflicts when calling read_block below.
        let entries_snapshot: Vec<(u64, u32, bool, Vec<u8>)> = self.index.entries
            [start_block_idx..]
            .iter()
            .map(|e| (e.block_offset, e.block_size, e.compressed, e.last_key.clone()))
            .collect();

        let mut results = Vec::new();

        for (block_offset, block_size, compressed, last_key) in entries_snapshot {
            // Skip blocks entirely before our range
            if let Some(s) = start {
                if last_key.as_slice() < s {
                    continue;
                }
            }

            let block = self.read_block(block_offset, block_size, compressed)?;

            // Choose the right starting iterator position
            let iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>> = match start {
                Some(s) => Box::new(block.iter_from(s).collect::<Vec<_>>().into_iter()),
                None => Box::new(block.iter().collect::<Vec<_>>().into_iter()),
            };

            for (k, v) in iter {
                if let Some(e) = end {
                    if k.as_slice() >= e {
                        return Ok(results);
                    }
                }
                if let Some(s) = start {
                    if k.as_slice() < s {
                        continue;
                    }
                }
                results.push((k, v));
            }

            // Early exit if this block's last_key >= end
            if let Some(e) = end {
                if last_key.as_slice() >= e {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Iterate all entries in sorted key order.
    pub fn iter_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(None, None)
    }

    // ---- private ----

    fn read_block(&mut self, offset: u64, size: u32, compressed: bool) -> Result<BlockReader> {
        let cache_key = (self.file_number, offset);

        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(cache_key) {
                // Cached data is stored as-is (raw, possibly still compressed)
                return BlockReader::new(&cached, compressed);
            }
        }

        let mut buf = vec![0u8; size as usize];
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(OxenError::Io)?;
        self.file.read_exact(&mut buf).map_err(OxenError::Io)?;

        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, buf.clone());
        }

        BlockReader::new(&buf, compressed)
    }
}
