use parking_lot::Mutex;
use std::collections::HashMap;

/// Cache key: (SSTable file_number, block_offset_in_file)
pub type BlockCacheKey = (u64, u64);

struct CacheEntry {
    data: Vec<u8>,
    /// Logical access timestamp used for LRU eviction
    last_used: u64,
}

/// A simple LRU block cache backed by a HashMap.
///
/// All operations are protected by a single `Mutex`.  For a production engine
/// you would shard this across multiple locks, but a single lock is correct and
/// sufficient for a first implementation.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    map: HashMap<BlockCacheKey, CacheEntry>,
    capacity_bytes: usize,
    used_bytes: usize,
    clock: u64,
}

impl BlockCache {
    /// Create a cache with the given capacity in bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        BlockCache {
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                capacity_bytes,
                used_bytes: 0,
                clock: 0,
            }),
        }
    }

    /// Create a cache from a capacity expressed in megabytes.
    pub fn with_mb(mb: usize) -> Self {
        Self::new(mb * 1024 * 1024)
    }

    /// Retrieve a cached block. Returns `None` on miss.
    pub fn get(&self, key: BlockCacheKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let clock = inner.clock;
        if let Some(entry) = inner.map.get_mut(&key) {
            entry.last_used = clock;
            return Some(entry.data.clone());
        }
        None
    }

    /// Insert a block. Evicts LRU entries if over capacity.
    pub fn insert(&self, key: BlockCacheKey, data: Vec<u8>) {
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let now = inner.clock;
        let entry_size = data.len();

        // Evict until there is room
        while inner.used_bytes + entry_size > inner.capacity_bytes && !inner.map.is_empty() {
            // Find LRU entry
            let lru_key = inner
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k);
            if let Some(k) = lru_key {
                if let Some(evicted) = inner.map.remove(&k) {
                    inner.used_bytes -= evicted.data.len();
                }
            } else {
                break;
            }
        }

        // If a single block is larger than the whole cache, skip caching it
        if entry_size > inner.capacity_bytes {
            return;
        }

        let old_size = inner
            .map
            .get(&key)
            .map(|e| e.data.len())
            .unwrap_or(0);
        inner.used_bytes = inner.used_bytes.saturating_sub(old_size);
        inner.map.insert(key, CacheEntry { data, last_used: now });
        inner.used_bytes += entry_size;
    }

    /// Remove a specific block from the cache (e.g., when an SSTable is deleted).
    pub fn remove(&self, key: BlockCacheKey) {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.map.remove(&key) {
            inner.used_bytes -= entry.data.len();
        }
    }

    /// Evict all cached blocks belonging to a specific file.
    pub fn evict_file(&self, file_number: u64) {
        let mut inner = self.inner.lock();
        let keys_to_remove: Vec<_> = inner
            .map
            .keys()
            .filter(|(fn_, _)| *fn_ == file_number)
            .cloned()
            .collect();
        for k in keys_to_remove {
            if let Some(entry) = inner.map.remove(&k) {
                inner.used_bytes -= entry.data.len();
            }
        }
    }

    pub fn used_bytes(&self) -> usize {
        self.inner.lock().used_bytes
    }

    pub fn capacity_bytes(&self) -> usize {
        self.inner.lock().capacity_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_get_insert() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert((1, 0), vec![1u8; 512]);
        assert!(cache.get((1, 0)).is_some());
        assert!(cache.get((1, 512)).is_none());
    }

    #[test]
    fn eviction_when_full() {
        // Cache that can hold exactly 2 × 512-byte blocks
        let cache = BlockCache::new(1024);
        cache.insert((1, 0), vec![0u8; 512]);
        cache.insert((1, 512), vec![1u8; 512]);
        // This should evict the LRU block
        cache.insert((1, 1024), vec![2u8; 512]);
        // Total entries should be ≤ 2
        assert!(cache.used_bytes() <= 1024);
    }

    #[test]
    fn evict_file() {
        let cache = BlockCache::new(10 * 1024 * 1024);
        for i in 0..5u64 {
            cache.insert((42, i * 100), vec![7u8; 256]);
        }
        cache.insert((99, 0), vec![8u8; 256]);
        cache.evict_file(42);
        assert!(cache.get((42, 0)).is_none());
        assert!(cache.get((99, 0)).is_some());
    }
}
