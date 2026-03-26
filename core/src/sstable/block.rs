use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use crate::error::{OxenError, Result};

/// Every N-th entry stores a full key restart; others store shared prefix lengths.
const RESTART_INTERVAL: usize = 16;

/// Builds one data block.
///
/// Block wire format (after optional LZ4 decompression):
/// ```text
/// [entries...][restart_offsets: u32 LE * num_restarts][num_restarts: u32 LE]
/// ```
///
/// Each entry:
/// ```text
/// [shared_len: u32 LE][unshared_len: u32 LE][value_len: u32 LE][unshared_key: bytes][value: bytes]
/// ```
pub struct BlockBuilder {
    buf: Vec<u8>,
    restart_offsets: Vec<u32>,
    last_key: Vec<u8>,
    entry_count: usize,
    use_compression: bool,
}

impl BlockBuilder {
    pub fn new(use_compression: bool) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restart_offsets: Vec::new(),
            last_key: Vec::new(),
            entry_count: 0,
            use_compression,
        }
    }

    /// Add a key-value pair.  Keys must be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let is_restart = self.entry_count % RESTART_INTERVAL == 0;
        let shared_len: usize = if is_restart {
            self.restart_offsets.push(self.buf.len() as u32);
            0
        } else {
            shared_prefix_len(&self.last_key, key)
        };

        let unshared_len = key.len() - shared_len;
        let value_len = value.len();

        self.buf.extend_from_slice(&(shared_len as u32).to_le_bytes());
        self.buf.extend_from_slice(&(unshared_len as u32).to_le_bytes());
        self.buf.extend_from_slice(&(value_len as u32).to_le_bytes());
        self.buf.extend_from_slice(&key[shared_len..]);
        self.buf.extend_from_slice(value);

        self.last_key = key.to_vec();
        self.entry_count += 1;
    }

    /// Finalise and return the serialised block bytes (optionally LZ4-compressed).
    ///
    /// Returns (block_bytes, is_compressed).
    pub fn finish(mut self) -> (Vec<u8>, bool) {
        // Append restart array
        for &offset in &self.restart_offsets {
            self.buf.extend_from_slice(&offset.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restart_offsets.len() as u32).to_le_bytes());

        if self.use_compression && self.buf.len() > 128 {
            let compressed = compress_prepend_size(&self.buf);
            if compressed.len() < self.buf.len() {
                return (compressed, true);
            }
        }
        (self.buf, false)
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buf.len() + self.restart_offsets.len() * 4 + 4
    }
}

/// Reads entries from a deserialised block.
pub struct BlockReader {
    data: Vec<u8>,
    restart_offsets: Vec<usize>,
}

impl BlockReader {
    /// Construct from raw block bytes.  If `compressed == true`, LZ4-decompresses first.
    pub fn new(raw: &[u8], compressed: bool) -> Result<Self> {
        let data = if compressed {
            decompress_size_prepended(raw)
                .map_err(|e| OxenError::Corruption(format!("LZ4 decompression failed: {e}")))?
        } else {
            raw.to_vec()
        };

        if data.len() < 4 {
            return Err(OxenError::Corruption("Block too short to contain restart array size".into()));
        }

        let num_restarts =
            u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let restarts_start = data
            .len()
            .checked_sub(4 + num_restarts * 4)
            .ok_or_else(|| OxenError::Corruption("Block restart array overflows block".into()))?;

        let mut restart_offsets = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let off = restarts_start + i * 4;
            let v = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            restart_offsets.push(v);
        }

        Ok(BlockReader { data: data[..restarts_start].to_vec(), restart_offsets })
    }

    /// Seek to a specific key using binary search over restart points, then scan forward.
    pub fn get(&self, target: &[u8]) -> Option<Vec<u8>> {
        // Binary search: find largest restart whose key <= target
        let restart_idx = self.find_restart(target);
        let mut pos = self.restart_offsets.get(restart_idx).copied().unwrap_or(0);
        let mut current_key = Vec::new();

        while pos < self.data.len() {
            let (key, value, next_pos) = self.decode_entry(pos, &current_key).ok()?;
            if key == target {
                return Some(value);
            }
            if key.as_slice() > target {
                return None;
            }
            current_key = key;
            pos = next_pos;
        }
        None
    }

    /// Iterate all entries in the block.
    pub fn iter(&self) -> BlockIter {
        BlockIter {
            reader: self,
            pos: 0,
            current_key: Vec::new(),
        }
    }

    /// Iterate entries with key >= start_key.
    pub fn iter_from(&self, start_key: &[u8]) -> BlockIter {
        let restart_idx = self.find_restart(start_key);
        let pos = self.restart_offsets.get(restart_idx).copied().unwrap_or(0);
        let mut iter = BlockIter {
            reader: self,
            pos,
            current_key: Vec::new(),
        };
        // Advance past entries before start_key
        while let Some((ref k, _)) = iter.peek() {
            if k.as_slice() >= start_key {
                break;
            }
            iter.next();
        }
        iter
    }

    fn find_restart(&self, target: &[u8]) -> usize {
        // Binary search: largest restart index whose first key <= target
        if self.restart_offsets.is_empty() {
            return 0;
        }
        let mut lo = 0usize;
        let mut hi = self.restart_offsets.len();
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            match self.restart_key_at(mid) {
                Some(k) if k.as_slice() <= target => lo = mid,
                _ => hi = mid,
            }
        }
        lo
    }

    fn restart_key_at(&self, idx: usize) -> Option<Vec<u8>> {
        let pos = *self.restart_offsets.get(idx)?;
        let (key, _, _) = self.decode_entry(pos, &[]).ok()?;
        Some(key)
    }

    pub(crate) fn decode_entry(
        &self,
        pos: usize,
        prev_key: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>, usize)> {
        if pos + 12 > self.data.len() {
            return Err(OxenError::Corruption(format!(
                "Block entry header truncated at pos {pos}"
            )));
        }
        let shared_len = u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()) as usize;
        let unshared_len =
            u32::from_le_bytes(self.data[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let value_len =
            u32::from_le_bytes(self.data[pos + 8..pos + 12].try_into().unwrap()) as usize;

        let key_start = pos + 12;
        let val_start = key_start + unshared_len;
        let next_pos = val_start + value_len;

        if next_pos > self.data.len() {
            return Err(OxenError::Corruption(format!(
                "Block entry data truncated at pos {pos}"
            )));
        }

        let mut key = Vec::with_capacity(shared_len + unshared_len);
        key.extend_from_slice(&prev_key[..shared_len]);
        key.extend_from_slice(&self.data[key_start..val_start]);
        let value = self.data[val_start..next_pos].to_vec();

        Ok((key, value, next_pos))
    }
}

pub struct BlockIter<'a> {
    reader: &'a BlockReader,
    pos: usize,
    current_key: Vec<u8>,
}

impl<'a> BlockIter<'a> {
    fn peek(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.pos >= self.reader.data.len() {
            return None;
        }
        self.reader
            .decode_entry(self.pos, &self.current_key)
            .ok()
            .map(|(k, v, _)| (k, v))
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.reader.data.len() {
            return None;
        }
        match self.reader.decode_entry(self.pos, &self.current_key) {
            Ok((key, value, next_pos)) => {
                self.current_key = key.clone();
                self.pos = next_pos;
                Some((key, value))
            }
            Err(_) => None,
        }
    }
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_block(pairs: &[(&[u8], &[u8])], compress: bool) -> (Vec<u8>, bool) {
        let mut builder = BlockBuilder::new(compress);
        for (k, v) in pairs {
            builder.add(k, v);
        }
        builder.finish()
    }

    #[test]
    fn roundtrip_no_compression() {
        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"apple", b"1"),
            (b"banana", b"2"),
            (b"cherry", b"3"),
        ];
        let (data, compressed) = build_block(&pairs, false);
        let reader = BlockReader::new(&data, compressed).unwrap();
        assert_eq!(reader.get(b"banana"), Some(b"2".to_vec()));
        assert_eq!(reader.get(b"cherry"), Some(b"3".to_vec()));
        assert_eq!(reader.get(b"durian"), None);
    }

    #[test]
    fn iter_all_entries() {
        let pairs: Vec<(&[u8], &[u8])> = (0u8..50)
            .map(|i| (vec![i], vec![i * 2]))
            .collect::<Vec<_>>()
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        // Need to own the vecs — use a simpler approach
        let owned: Vec<(Vec<u8>, Vec<u8>)> =
            (0u8..50).map(|i| (vec![i], vec![i * 2])).collect();
        let refs: Vec<(&[u8], &[u8])> =
            owned.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();

        let mut builder = BlockBuilder::new(false);
        for (k, v) in &refs {
            builder.add(k, v);
        }
        let (data, compressed) = builder.finish();
        let reader = BlockReader::new(&data, compressed).unwrap();
        let entries: Vec<_> = reader.iter().collect();
        assert_eq!(entries.len(), 50);
    }

    #[test]
    fn roundtrip_with_compression() {
        // Use repetitive data so LZ4 can actually compress it
        let mut builder = BlockBuilder::new(true);
        for i in 0u32..200 {
            let k = format!("aaaaaaaaaa_{:05}", i);
            let v = format!("vvvvvvvvvv_{:05}", i);
            builder.add(k.as_bytes(), v.as_bytes());
        }
        let (data, compressed) = builder.finish();
        assert!(compressed, "Expected compression to kick in");
        let reader = BlockReader::new(&data, compressed).unwrap();
        let key = format!("aaaaaaaaaa_{:05}", 100);
        let val = format!("vvvvvvvvvv_{:05}", 100);
        assert_eq!(reader.get(key.as_bytes()), Some(val.into_bytes()));
    }
}
