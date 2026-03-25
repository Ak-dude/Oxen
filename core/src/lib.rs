//! # OxenDB Core
//!
//! A high-performance LSM-tree storage engine implemented in Rust.
//!
//! ## Architecture
//!
//! - **WAL** — Write-Ahead Log for crash recovery (CRC32C framing)
//! - **MemTable** — In-memory sorted write buffer backed by a skip-list
//! - **SSTable** — Immutable sorted string tables with LZ4 compression and Bloom filters
//! - **Compaction** — Leveled compaction (L0–L6) with background scheduling
//! - **BlockCache** — LRU block cache for hot data
//! - **C FFI** — `extern "C"` API for embedding via cgo or other FFI layers

pub mod cache;
pub mod compaction;
pub mod config;
pub mod error;
pub mod ffi;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod storage;
pub mod wal;

// Public re-exports for convenience
pub use config::EngineConfig;
pub use error::{OxenError, Result};
pub use storage::StorageEngine;
