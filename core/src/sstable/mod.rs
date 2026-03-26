pub mod block;
pub mod bloom;
pub mod builder;
pub mod index;
pub mod reader;

pub use builder::SSTableBuilder;
pub use reader::SSTableReader;

use std::path::PathBuf;

/// Metadata describing one SSTable file on disk.
#[derive(Debug, Clone)]
pub struct SSTableMeta {
    /// Unique monotonic number assigned by the manifest
    pub file_number: u64,
    /// Compaction level (0 = freshly flushed from MemTable)
    pub level: usize,
    /// Absolute path to the `.sst` file
    pub file_path: PathBuf,
    /// Total file size in bytes
    pub file_size: u64,
    /// Smallest key in this file
    pub first_key: Vec<u8>,
    /// Largest key in this file
    pub last_key: Vec<u8>,
    /// Total number of entries (including tombstones)
    pub entry_count: u64,
}

impl SSTableMeta {
    /// Returns true if this SSTable's key range might overlap with [start, end).
    pub fn overlaps_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
        if let Some(e) = end {
            if self.first_key.as_slice() >= e {
                return false;
            }
        }
        if let Some(s) = start {
            if self.last_key.as_slice() < s {
                return false;
            }
        }
        true
    }

    /// Returns true if `key` falls within this file's [first_key, last_key] range.
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        key >= self.first_key.as_slice() && key <= self.last_key.as_slice()
    }
}
