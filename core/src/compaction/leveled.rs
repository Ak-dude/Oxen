use std::path::Path;
use std::sync::Arc;

use crate::cache::BlockCache;
use crate::compaction::merger::MergingIterator;
use crate::error::{OxenError, Result};
use crate::manifest::version::{LEVEL_SIZE_BYTES, NUM_LEVELS};
use crate::manifest::VersionSet;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::SSTableReader;
use crate::sstable::SSTableMeta;

/// Sentinel value for tombstones inside SSTable data
pub const TOMBSTONE_VALUE: &[u8] = b"\x00TOMBSTONE\x00";

/// The leveled compaction strategy.
///
/// Invariants:
/// - L0 files may overlap (they are flushed directly from MemTable).
/// - L1+ files are non-overlapping within each level.
/// - Compaction picks L0→L1 when L0 reaches the trigger count.
/// - For L1+, compaction is triggered when a level exceeds its size budget.
pub struct LeveledCompaction<'a> {
    version: &'a mut VersionSet,
    data_dir: &'a Path,
    block_size_bytes: usize,
    bloom_bits_per_key: usize,
    cache: Arc<BlockCache>,
    l0_trigger: usize,
}

impl<'a> LeveledCompaction<'a> {
    pub fn new(
        version: &'a mut VersionSet,
        data_dir: &'a Path,
        block_size_bytes: usize,
        bloom_bits_per_key: usize,
        cache: Arc<BlockCache>,
        l0_trigger: usize,
    ) -> Self {
        LeveledCompaction {
            version,
            data_dir,
            block_size_bytes,
            bloom_bits_per_key,
            cache,
            l0_trigger,
        }
    }

    /// Run one round of compaction if any level needs it. Returns the number of
    /// compaction jobs that were executed.
    pub fn maybe_compact(&mut self) -> Result<usize> {
        let mut jobs = 0;

        // L0 → L1 compaction
        if self.version.l0_file_count() >= self.l0_trigger {
            self.compact_level(0)?;
            jobs += 1;
        }

        // L1..L5 size-based compaction
        for level in 1..NUM_LEVELS - 1 {
            let budget = LEVEL_SIZE_BYTES[level];
            if budget > 0 && self.version.level_size_bytes(level) > budget {
                self.compact_level(level)?;
                jobs += 1;
            }
        }

        Ok(jobs)
    }

    /// Compact `level` into `level + 1`.
    fn compact_level(&mut self, level: usize) -> Result<()> {
        let target_level = level + 1;
        let inputs = self.version.files_at_level(level);
        if inputs.is_empty() {
            return Ok(());
        }

        // For L1+ pick only the file with the smallest first_key to limit write-amp
        let selected: Vec<SSTableMeta> = if level == 0 {
            // Compact ALL L0 files together (they may overlap)
            inputs
        } else {
            // Pick the one file with the smallest first_key
            let mut v = inputs;
            v.sort_by(|a, b| a.first_key.cmp(&b.first_key));
            vec![v.into_iter().next().unwrap()]
        };

        // Find overlapping files in the target level
        let target_files = self.version.files_at_level(target_level);
        let min_key = selected.iter().map(|m| &m.first_key).min().cloned().unwrap_or_default();
        let max_key = selected.iter().map(|m| &m.last_key).max().cloned().unwrap_or_default();

        let overlapping: Vec<SSTableMeta> = target_files
            .into_iter()
            .filter(|m| {
                m.last_key >= min_key && m.first_key <= max_key
            })
            .collect();

        let all_inputs: Vec<SSTableMeta> = selected.iter().cloned()
            .chain(overlapping.iter().cloned())
            .collect();

        if all_inputs.is_empty() {
            return Ok(());
        }

        log::info!(
            "Compacting {} files from L{} + {} files from L{} -> L{}",
            selected.len(), level, overlapping.len(), target_level, target_level
        );

        // Build merged iterator from all input files
        let mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>> = Vec::new();
        for meta in &all_inputs {
            let mut reader = SSTableReader::open(
                &meta.file_path,
                meta.file_number,
                Some(self.cache.clone()),
            )?;
            let entries = reader.iter_all()?;
            sources.push(Box::new(entries.into_iter()));
        }

        let merged = MergingIterator::new(sources);

        // Write output SSTable(s) — split at target_level size budget / 10
        let target_size = if target_level == 0 || LEVEL_SIZE_BYTES[target_level] == 0 {
            64 * 1024 * 1024u64 // 64 MiB default for L0/L6
        } else {
            LEVEL_SIZE_BYTES[target_level] / 10
        };

        let mut output_metas: Vec<SSTableMeta> = Vec::new();
        let mut current_builder: Option<SSTableBuilder> = None;
        let mut current_size_estimate: u64 = 0;

        for (key, value) in merged {
            // Drop pure tombstones at the bottom level
            let is_tombstone = value == TOMBSTONE_VALUE;
            if is_tombstone && target_level == NUM_LEVELS - 1 {
                continue;
            }

            if current_builder.is_none() {
                let fn_ = self.version.next_file_number();
                let path = self.data_dir.join(format!("{:020}.sst", fn_));
                current_builder = Some(SSTableBuilder::create(
                    &path,
                    fn_,
                    target_level,
                    self.block_size_bytes,
                    self.bloom_bits_per_key,
                )?);
                current_size_estimate = 0;
            }

            let b = current_builder.as_mut().unwrap();
            b.add(&key, &value)?;
            current_size_estimate += (key.len() + value.len()) as u64;

            if current_size_estimate >= target_size {
                let builder = current_builder.take().unwrap();
                let meta = builder.finish()?;
                output_metas.push(meta);
            }
        }

        if let Some(builder) = current_builder.take() {
            let meta = builder.finish()?;
            output_metas.push(meta);
        }

        // Update manifest: add new, remove old
        for meta in output_metas {
            self.version.add_file(meta)?;
        }
        for meta in &all_inputs {
            self.version.remove_file(meta.level, meta.file_number)?;
            self.cache.evict_file(meta.file_number);
            // Best-effort delete the old SSTable file
            let _ = std::fs::remove_file(&meta.file_path);
        }

        Ok(())
    }
}
