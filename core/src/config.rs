use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::error::{OxenError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SyncMode {
    /// fsync on every WAL write — safest, slowest
    Full,
    /// OS handles flushing — fastest, risk of data loss on crash
    None,
    /// fsync periodically in background
    Periodic,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Full
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Directory where all data files are stored
    pub data_dir: PathBuf,

    /// Maximum size of the in-memory MemTable before flushing to L0 SSTable
    pub memtable_size_bytes: usize,

    /// Target block size for SSTable data blocks
    pub block_size_bytes: usize,

    /// Bits per key for Bloom filters (higher = fewer false positives, more memory)
    pub bloom_bits_per_key: usize,

    /// WAL sync strategy
    pub sync_mode: SyncMode,

    /// Maximum number of simultaneously open SSTable file handles
    pub max_open_files: usize,

    /// Block cache capacity in megabytes
    pub block_cache_mb: usize,

    /// Number of L0 SSTables that triggers a minor compaction
    pub l0_compaction_trigger: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./oxendb_data"),
            memtable_size_bytes: 64 * 1024 * 1024,  // 64 MiB
            block_size_bytes: 4 * 1024,              // 4 KiB
            bloom_bits_per_key: 10,
            sync_mode: SyncMode::Full,
            max_open_files: 1000,
            block_cache_mb: 128,
            l0_compaction_trigger: 4,
        }
    }
}

impl EngineConfig {
    /// Load config from a TOML file, falling back to defaults for missing fields
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(OxenError::Io)?;
        let config: EngineConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Persist config to a TOML file
    pub fn save_to_file(&self, path: &std::path::Path) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content).map_err(OxenError::Io)?;
        Ok(())
    }

    /// Validate the config values are sensible
    pub fn validate(&self) -> Result<()> {
        if self.memtable_size_bytes < 1024 {
            return Err(OxenError::InvalidArgument(
                "memtable_size_bytes must be at least 1024".into(),
            ));
        }
        if self.block_size_bytes < 512 {
            return Err(OxenError::InvalidArgument(
                "block_size_bytes must be at least 512".into(),
            ));
        }
        if self.bloom_bits_per_key == 0 || self.bloom_bits_per_key > 32 {
            return Err(OxenError::InvalidArgument(
                "bloom_bits_per_key must be between 1 and 32".into(),
            ));
        }
        if self.l0_compaction_trigger == 0 {
            return Err(OxenError::InvalidArgument(
                "l0_compaction_trigger must be at least 1".into(),
            ));
        }
        Ok(())
    }
}
