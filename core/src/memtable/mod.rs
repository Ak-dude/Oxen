pub mod arena;
pub mod skiplist;

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::Result;
use skiplist::SkipList;

/// Approximate per-entry overhead for size accounting (key + value pointers, SkipMap node)
const ENTRY_OVERHEAD: usize = 64;

/// The in-memory write buffer for the storage engine.
///
/// `MemTable` wraps a `SkipList` with size tracking.  It is thread-safe via
/// internal locking; external code does not need to hold any lock.
pub struct MemTable {
    list: Arc<SkipList>,
    size_bytes: AtomicUsize,
    /// Sequence number of the most recently written entry
    last_seq: Arc<RwLock<u64>>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            list: Arc::new(SkipList::new()),
            size_bytes: AtomicUsize::new(0),
            last_seq: Arc::new(RwLock::new(0)),
        }
    }

    /// Insert a key-value pair (or overwrite existing).
    pub fn put(&self, key: &[u8], value: &[u8], seq: u64) -> Result<()> {
        let entry_size = key.len() + value.len() + ENTRY_OVERHEAD;
        self.list.put(key.to_vec(), value.to_vec());
        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.update_seq(seq);
        Ok(())
    }

    /// Write a tombstone for the given key.
    pub fn delete(&self, key: &[u8], seq: u64) -> Result<()> {
        let entry_size = key.len() + ENTRY_OVERHEAD;
        self.list.delete(key.to_vec());
        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
        self.update_seq(seq);
        Ok(())
    }

    /// Look up a key.
    ///
    /// Returns:
    /// - `Ok(Some(value))` — key present with a value
    /// - `Ok(None)`        — key is a tombstone or not found
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.list.get(key) {
            Some(Some(v)) => Ok(Some(v)),
            Some(None) => Ok(None),  // tombstone
            None => Ok(None),        // not found
        }
    }

    /// Returns `true` if the key exists in this MemTable (including as a tombstone).
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.list.get(key).is_some()
    }

    /// Returns `true` if the key has an explicit tombstone.
    pub fn is_deleted(&self, key: &[u8]) -> bool {
        matches!(self.list.get(key), Some(None))
    }

    /// Scan a half-open key range `[start, end)`.
    ///
    /// Returns live (non-tombstone) entries only.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.list
            .range(start, end)
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    /// Full sorted iterator including tombstones — used by SSTable flush.
    pub fn iter(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.list.iter_all()
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn last_seq(&self) -> u64 {
        *self.last_seq.read()
    }

    fn update_seq(&self, seq: u64) {
        let mut guard = self.last_seq.write();
        if seq > *guard {
            *guard = seq;
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        MemTable::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_operations() {
        let mt = MemTable::new();
        mt.put(b"foo", b"bar", 1).unwrap();
        mt.put(b"baz", b"qux", 2).unwrap();
        assert_eq!(mt.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        mt.delete(b"foo", 3).unwrap();
        assert_eq!(mt.get(b"foo").unwrap(), None);
        assert!(mt.size_bytes() > 0);
    }

    #[test]
    fn scan_live_entries() {
        let mt = MemTable::new();
        for i in 0u8..10 {
            mt.put(&[i], &[i * 2], i as u64).unwrap();
        }
        mt.delete(&[5u8], 100).unwrap();
        let results = mt.scan(None, None);
        assert_eq!(results.len(), 9); // 10 - 1 tombstone
    }
}
