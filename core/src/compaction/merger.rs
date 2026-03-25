use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// One entry produced by a sorted iterator that participates in the merge.
struct HeapEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    /// Which input iterator produced this entry (for stable ordering)
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we want min-key first, so reverse the key order.
        // For equal keys, lower source_idx wins (newer data overwrites older).
        other.key.cmp(&self.key)
            .then_with(|| self.source_idx.cmp(&other.source_idx))
    }
}

/// A k-way merge iterator over multiple pre-sorted key-value iterators.
///
/// Guarantees:
/// - Output is globally sorted by key.
/// - For duplicate keys, the entry from the lowest-indexed source (newest) wins.
/// - Tombstones (value = empty sentinel `\x00`) are propagated as-is; the
///   compaction logic decides whether to drop them.
pub struct MergingIterator {
    sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>>,
    heap: BinaryHeap<HeapEntry>,
    last_key: Option<Vec<u8>>,
}

impl MergingIterator {
    /// Build a MergingIterator from a list of sorted iterators.
    /// Source at index 0 is considered newest (highest priority).
    pub fn new(mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Prime the heap with the first entry from each source
        for (idx, src) in sources.iter_mut().enumerate() {
            if let Some((key, value)) = src.next() {
                heap.push(HeapEntry { key, value, source_idx: idx });
            }
        }

        MergingIterator { sources, heap, last_key: None }
    }
}

impl Iterator for MergingIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.heap.pop()?;

            // Advance the source that just produced this entry
            if let Some((next_key, next_val)) = self.sources[entry.source_idx].next() {
                self.heap.push(HeapEntry {
                    key: next_key,
                    value: next_val,
                    source_idx: entry.source_idx,
                });
            }

            // Skip duplicate keys — keep only the first occurrence (lowest source_idx = newest)
            if let Some(ref last) = self.last_key {
                if *last == entry.key {
                    // Drain all remaining heap entries with the same key
                    while self.heap.peek().map_or(false, |e| e.key == entry.key) {
                        let dup = self.heap.pop().unwrap();
                        if let Some((nk, nv)) = self.sources[dup.source_idx].next() {
                            self.heap.push(HeapEntry {
                                key: nk,
                                value: nv,
                                source_idx: dup.source_idx,
                            });
                        }
                    }
                    continue;
                }
            }

            self.last_key = Some(entry.key.clone());
            return Some((entry.key, entry.value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted_iter(pairs: Vec<(&str, &str)>) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        let owned: Vec<(Vec<u8>, Vec<u8>)> = pairs
            .into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect();
        Box::new(owned.into_iter())
    }

    #[test]
    fn basic_merge() {
        let a = sorted_iter(vec![("a", "1"), ("c", "3"), ("e", "5")]);
        let b = sorted_iter(vec![("b", "2"), ("d", "4"), ("f", "6")]);
        let merged: Vec<_> = MergingIterator::new(vec![a, b]).collect();
        let keys: Vec<_> = merged.iter().map(|(k, _)| String::from_utf8(k.clone()).unwrap()).collect();
        assert_eq!(keys, vec!["a", "b", "c", "d", "e", "f"]);
    }

    #[test]
    fn newer_source_wins_on_duplicate_key() {
        // Source 0 is "newer"
        let newer = sorted_iter(vec![("a", "new"), ("b", "new")]);
        let older = sorted_iter(vec![("a", "old"), ("b", "old"), ("c", "old")]);
        let merged: Vec<_> = MergingIterator::new(vec![newer, older]).collect();
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0], (b"a".to_vec(), b"new".to_vec()));
        assert_eq!(merged[1], (b"b".to_vec(), b"new".to_vec()));
        assert_eq!(merged[2], (b"c".to_vec(), b"old".to_vec()));
    }

    #[test]
    fn empty_sources() {
        let empty: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>> =
            Box::new(std::iter::empty());
        let merged: Vec<_> = MergingIterator::new(vec![empty]).collect();
        assert!(merged.is_empty());
    }
}
