use xxhash_rust::xxh3::xxh3_64_with_seed;

/// A space-efficient probabilistic filter using double-hashing with xxHash3.
///
/// False positive rate with `bits_per_key = 10` is approximately 1%.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hashes: usize,
    num_bits: usize,
}

impl BloomFilter {
    /// Build a filter for the given set of keys.
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Self {
        let bits_per_key = bits_per_key.clamp(1, 32);
        // Number of hash functions that minimises false positives: k = ln2 * bits_per_key
        let num_hashes = ((bits_per_key as f64 * std::f64::consts::LN_2) as usize).max(1).min(30);

        let num_bits = (keys.len() * bits_per_key).max(64);
        // Round up to next multiple of 8
        let num_bits = (num_bits + 7) & !7;
        let mut bits = vec![0u8; num_bits / 8];

        for key in keys {
            let (h1, h2) = double_hash(key);
            for i in 0..num_hashes {
                let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits as u64;
                let byte_idx = (bit_pos / 8) as usize;
                let bit_idx = (bit_pos % 8) as usize;
                bits[byte_idx] |= 1 << bit_idx;
            }
        }

        BloomFilter { bits, num_hashes, num_bits }
    }

    /// Returns `false` if the key is definitely NOT in the set.
    /// Returns `true` if the key is probably in the set.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return true;
        }
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hashes {
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.num_bits as u64;
            let byte_idx = (bit_pos / 8) as usize;
            let bit_idx = (bit_pos % 8) as usize;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the filter to bytes for embedding in an SSTable footer.
    /// Layout: [num_hashes: u8][num_bits: u32 LE][bits...]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(5 + self.bits.len());
        buf.push(self.num_hashes as u8);
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Deserialize from bytes produced by `encode()`.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 5 {
            return None;
        }
        let num_hashes = data[0] as usize;
        let num_bits = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        let expected_bytes = (num_bits + 7) / 8;
        if data.len() < 5 + expected_bytes {
            return None;
        }
        let bits = data[5..5 + expected_bytes].to_vec();
        Some(BloomFilter { bits, num_hashes, num_bits })
    }
}

/// Produce two independent 64-bit hashes from one key using xxHash3 with two seeds.
fn double_hash(key: &[u8]) -> (u64, u64) {
    let h1 = xxh3_64_with_seed(key, 0x_dead_beef_cafe_1234);
    let h2 = xxh3_64_with_seed(key, 0x_0123_4567_89ab_cdef);
    // Ensure h2 is odd so the step size is always coprime to the table size
    (h1, h2 | 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0u32..1000).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);
        for key in &key_refs {
            assert!(filter.may_contain(key), "False negative detected");
        }
    }

    #[test]
    fn reasonable_false_positive_rate() {
        let keys: Vec<Vec<u8>> = (0u32..1000).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        let mut fp_count = 0;
        for i in 1000u32..2000 {
            if filter.may_contain(&i.to_le_bytes()) {
                fp_count += 1;
            }
        }
        // With bits_per_key=10, FP rate should be < 2%
        assert!(fp_count < 20, "FP rate too high: {fp_count}/1000");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let keys: Vec<Vec<u8>> = (0u32..100).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);
        let encoded = filter.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        for key in &key_refs {
            assert!(decoded.may_contain(key));
        }
    }
}
