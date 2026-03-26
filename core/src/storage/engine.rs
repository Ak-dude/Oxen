use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::cache::BlockCache;
use crate::compaction::leveled::TOMBSTONE_VALUE;
use crate::compaction::scheduler::{CompactionScheduler, SharedCompactionState};
use crate::compaction::LeveledCompaction;
use crate::config::EngineConfig;
use crate::error::{OxenError, Result};
use crate::manifest::VersionSet;
use crate::memtable::MemTable;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::SSTableReader;
use crate::wal::{OpType, WALReader, WALWriter};

/// The main storage engine.
///
/// Thread safety: `StorageEngine` is `Send + Sync`.  Internal state is protected
/// by a `Mutex<EngineState>` so multiple threads can share a single `Arc<StorageEngine>`.
pub struct StorageEngine {
    inner: Arc<Mutex<EngineState>>,
}

struct EngineState {
    config: EngineConfig,
    data_dir: PathBuf,
    memtable: MemTable,
    wal: WALWriter,
    version: Arc<Mutex<VersionSet>>,
    cache: Arc<BlockCache>,
    seq_counter: u64,
    compaction_scheduler: CompactionScheduler,
    closed: bool,
}

impl StorageEngine {
    /// Open (or create) a database at the directory specified in `config`.
    pub fn open(config: EngineConfig) -> Result<Self> {
        config.validate()?;

        let data_dir = config.data_dir.clone();
        std::fs::create_dir_all(&data_dir)?;

        let wal_dir = data_dir.join("wal");
        let wal = WALWriter::open(&wal_dir, config.sync_mode.clone())?;

        let cache = Arc::new(BlockCache::with_mb(config.block_cache_mb));
        let version = Arc::new(Mutex::new(VersionSet::open(&data_dir)?));

        let memtable = MemTable::new();

        // Replay WAL to reconstruct any unflushed MemTable state
        let max_seq = Self::replay_wal(&wal_dir, &memtable)?;

        let compaction_state = Arc::new(SharedCompactionState {
            version: version.clone(),
            cache: cache.clone(),
            data_dir: data_dir.clone(),
            block_size_bytes: config.block_size_bytes,
            bloom_bits_per_key: config.bloom_bits_per_key,
            l0_trigger: config.l0_compaction_trigger,
        });

        let scheduler = CompactionScheduler::start(compaction_state, Duration::from_secs(5));

        let state = EngineState {
            config,
            data_dir,
            memtable,
            wal,
            version,
            cache,
            seq_counter: max_seq + 1,
            compaction_scheduler: scheduler,
            closed: false,
        };

        Ok(StorageEngine {
            inner: Arc::new(Mutex::new(state)),
        })
    }

    /// Store a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }

        let seq = state.next_seq();
        let record = crate::wal::WALRecord::new_put(seq, key.to_vec(), value.to_vec());
        state.wal.append(&record)?;
        state.memtable.put(key, value, seq)?;

        if state.memtable.size_bytes() >= state.config.memtable_size_bytes {
            Self::flush_memtable_locked(&mut state)?;
        }
        Ok(())
    }

    /// Delete a key (writes a tombstone).
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }

        let seq = state.next_seq();
        let record = crate::wal::WALRecord::new_delete(seq, key.to_vec());
        state.wal.append(&record)?;
        state.memtable.delete(key, seq)?;

        if state.memtable.size_bytes() >= state.config.memtable_size_bytes {
            Self::flush_memtable_locked(&mut state)?;
        }
        Ok(())
    }

    /// Look up a key.  Returns `None` if the key does not exist or was deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }

        // 1. Check MemTable first (most recent writes)
        if state.memtable.contains_key(key) {
            return state.memtable.get(key);
        }

        // 2. Search SSTables from newest to oldest (L0 → L6)
        // Snapshot the file metadata under the version lock, then release it
        // before doing any I/O so we don't hold the lock across disk reads.
        let files_per_level: Vec<Vec<crate::sstable::SSTableMeta>> = {
            let version = state.version.lock();
            (0..crate::manifest::NUM_LEVELS)
                .map(|level| {
                    let mut files = version.files_at_level(level);
                    files.retain(|m| m.key_in_range(key));
                    // Newest (highest file_number) first
                    files.sort_by(|a, b| b.file_number.cmp(&a.file_number));
                    files
                })
                .collect()
        };

        let cache = state.cache.clone();
        for files in files_per_level {
            for meta in files {
                let mut reader =
                    SSTableReader::open(&meta.file_path, meta.file_number, Some(cache.clone()))?;
                match reader.get(key)? {
                    Some(v) if v == TOMBSTONE_VALUE => return Ok(None),
                    Some(v) => return Ok(Some(v)),
                    None => continue,
                }
            }
        }

        Ok(None)
    }

    /// Scan a half-open key range `[start, end)`.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }

        // Collect from MemTable
        let mut results: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            state.memtable.scan(start, end)
                .into_iter()
                .collect();

        // Snapshot SSTable file metadata under the version lock, then release
        // it before doing I/O.
        let files_per_level: Vec<Vec<crate::sstable::SSTableMeta>> = {
            let version = state.version.lock();
            (0..crate::manifest::NUM_LEVELS)
                .map(|level| {
                    let mut files = version.files_at_level(level);
                    files.retain(|m| m.overlaps_range(start, end));
                    // Process oldest first so newer files can overwrite
                    files.sort_by(|a, b| a.file_number.cmp(&b.file_number));
                    files
                })
                .collect()
        };

        let cache = state.cache.clone();
        // Overlay SSTable data from oldest to newest (lower levels = older)
        for files in files_per_level.into_iter().rev() {
            for meta in &files {
                let mut reader =
                    SSTableReader::open(&meta.file_path, meta.file_number, Some(cache.clone()))?;
                let entries = reader.scan(start, end)?;
                for (k, v) in entries {
                    // Only insert if a newer source doesn't already have this key
                    results.entry(k).or_insert(v);
                }
            }
        }

        // Remove tombstones and sort
        let mut sorted: Vec<(Vec<u8>, Vec<u8>)> = results
            .into_iter()
            .filter(|(_, v)| v != TOMBSTONE_VALUE)
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(sorted)
    }

    /// Force a MemTable flush to L0.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }
        Self::flush_memtable_locked(&mut state)
    }

    /// Trigger an immediate compaction round.
    pub fn compact(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Err(OxenError::EngineClosed);
        }
        let mut version = state.version.lock();
        let mut compaction = LeveledCompaction::new(
            &mut version,
            &state.data_dir,
            state.config.block_size_bytes,
            state.config.bloom_bits_per_key,
            state.cache.clone(),
            state.config.l0_compaction_trigger,
        );
        compaction.maybe_compact()?;
        Ok(())
    }

    /// Gracefully close the engine: flush, sync WAL, stop background threads.
    pub fn close(&self) -> Result<()> {
        let mut state = self.inner.lock();
        if state.closed {
            return Ok(());
        }
        // Flush in-memory data
        if !state.memtable.is_empty() {
            Self::flush_memtable_locked(&mut state)?;
        }
        state.wal.write_checkpoint()?;
        state.wal.sync()?;
        state.compaction_scheduler.shutdown();
        state.closed = true;
        Ok(())
    }

    // ---- private ----

    fn flush_memtable_locked(state: &mut EngineState) -> Result<()> {
        let entries = state.memtable.iter();
        if entries.is_empty() {
            return Ok(());
        }

        let fn_ = state.version.lock().next_file_number();
        let sst_path = state.data_dir.join(format!("{:020}.sst", fn_));

        let mut builder = SSTableBuilder::create(
            &sst_path,
            fn_,
            0, // L0
            state.config.block_size_bytes,
            state.config.bloom_bits_per_key,
        )?;

        for (key, value_opt) in entries {
            let value = value_opt.unwrap_or_else(|| TOMBSTONE_VALUE.to_vec());
            builder.add(&key, &value)?;
        }

        let meta = builder.finish()?;
        state.version.lock().add_file(meta)?;

        // Replace the flushed MemTable with a fresh one
        state.memtable = MemTable::new();

        // Rotate WAL so old entries can eventually be deleted
        let _ = state.wal.sync();

        log::info!("Flushed MemTable to L0 SSTable #{fn_}");
        Ok(())
    }

    fn replay_wal(wal_dir: &Path, memtable: &MemTable) -> Result<u64> {
        if !wal_dir.exists() {
            return Ok(0);
        }

        let reader = match WALReader::open(wal_dir) {
            Ok(r) => r,
            Err(_) => return Ok(0),
        };

        let mut max_seq = 0u64;
        for result in reader {
            match result {
                Ok(record) => {
                    if record.seq_no > max_seq {
                        max_seq = record.seq_no;
                    }
                    match record.op {
                        OpType::Put => {
                            memtable.put(&record.key, &record.value, record.seq_no)?;
                        }
                        OpType::Delete => {
                            memtable.delete(&record.key, record.seq_no)?;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("WAL replay error (treating as end-of-log): {e}");
                    break;
                }
            }
        }

        if max_seq > 0 {
            log::info!("WAL replay complete: recovered up to seq {max_seq}");
        }
        Ok(max_seq)
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Best-effort close on drop
        let _ = self.close();
    }
}

impl EngineState {
    fn next_seq(&mut self) -> u64 {
        let seq = self.seq_counter;
        self.seq_counter += 1;
        seq
    }
}
