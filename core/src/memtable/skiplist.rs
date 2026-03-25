use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// A thin, ergonomic wrapper around `crossbeam_skiplist::SkipMap`.
///
/// Keys and values are owned `Vec<u8>` byte strings.  The underlying SkipMap
/// provides lock-free concurrent reads with fine-grained write locking —
/// exactly what an LSM MemTable needs.
///
/// Tombstones (deletes) are represented as entries with `value == None`.
#[derive(Clone)]
pub struct SkipList {
    inner: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl SkipList {
    pub fn new() -> Self {
        SkipList {
            inner: Arc::new(SkipMap::new()),
        }
    }

    /// Insert or overwrite a key-value pair.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, Some(value));
    }

    /// Insert a tombstone for the given key.
    pub fn delete(&self, key: Vec<u8>) {
        self.inner.insert(key, None);
    }

    /// Look up a key.  Returns:
    /// - `Some(Some(value))` if the key is present with a value
    /// - `Some(None)`        if the key has a tombstone
    /// - `None`              if the key has never been written to this SkipList
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.inner.get(key).map(|e| e.value().clone())
    }

    /// Return an iterator over all entries whose keys fall in `[start, end)`.
    /// Both bounds are optional; pass `None` for an open-ended scan.
    pub fn range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        use std::ops::Bound;

        let lo: Bound<Vec<u8>> = match start {
            Some(s) => Bound::Included(s.to_vec()),
            None => Bound::Unbounded,
        };
        let hi: Bound<Vec<u8>> = match end {
            Some(e) => Bound::Excluded(e.to_vec()),
            None => Bound::Unbounded,
        };

        self.inner
            .range((lo, hi))
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Full scan — returns all entries (including tombstones) in sorted key order.
    pub fn iter_all(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Number of logical entries (including tombstones).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for SkipList {
    fn default() -> Self {
        SkipList::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let sl = SkipList::new();
        sl.put(b"a".to_vec(), b"1".to_vec());
        sl.put(b"b".to_vec(), b"2".to_vec());
        assert_eq!(sl.get(b"a"), Some(Some(b"1".to_vec())));
        sl.delete(b"a".to_vec());
        assert_eq!(sl.get(b"a"), Some(None)); // tombstone
        assert_eq!(sl.get(b"c"), None);       // never written
    }

    #[test]
    fn range_scan() {
        let sl = SkipList::new();
        for c in b'a'..=b'e' {
            sl.put(vec![c], vec![c]);
        }
        let results = sl.range(Some(b"b"), Some(b"d"));
        assert_eq!(results.len(), 2); // b, c
        assert_eq!(results[0].0, b"b".to_vec());
        assert_eq!(results[1].0, b"c".to_vec());
    }
}
