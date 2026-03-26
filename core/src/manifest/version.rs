use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{OxenError, Result};
use crate::sstable::SSTableMeta;

/// Number of compaction levels
pub const NUM_LEVELS: usize = 7;

/// Maximum bytes allowed in each level (exponential growth factor 10x)
pub const LEVEL_SIZE_BYTES: [u64; NUM_LEVELS] = [
    0,                          // L0: governed by file count
    10 * 1024 * 1024,           // L1: 10 MiB
    100 * 1024 * 1024,          // L2: 100 MiB
    1_000 * 1024 * 1024,        // L3: 1 GiB
    10_000 * 1024 * 1024,       // L4: 10 GiB
    100_000 * 1024 * 1024,      // L5: 100 GiB
    1_000_000 * 1024 * 1024,    // L6: 1 TiB
];

/// A MANIFEST edit — one line in the append-only log.
#[derive(Debug, Clone)]
pub enum ManifestEdit {
    /// A new SSTable was flushed or produced by compaction
    AddFile {
        level: usize,
        file_number: u64,
        file_path: String,
        file_size: u64,
        first_key: Vec<u8>,
        last_key: Vec<u8>,
        entry_count: u64,
    },
    /// An SSTable was superseded by compaction
    RemoveFile {
        level: usize,
        file_number: u64,
    },
    /// Next file number to use
    NextFileNumber(u64),
}

/// Tracks which SSTable files are live at each compaction level.
///
/// Persists edits to an append-only MANIFEST text file.
pub struct VersionSet {
    /// levels[i] is the set of live SSTables at level i, keyed by file_number
    levels: Arc<RwLock<Vec<HashMap<u64, SSTableMeta>>>>,
    manifest_file: Mutex<File>,
    next_file_number: AtomicU64,
}

impl VersionSet {
    /// Open or create the MANIFEST in `dir`.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let manifest_path = dir.join("MANIFEST");

        let mut levels: Vec<HashMap<u64, SSTableMeta>> =
            (0..NUM_LEVELS).map(|_| HashMap::new()).collect();
        let mut next_file_number: u64 = 1;

        // Replay existing MANIFEST
        if manifest_path.exists() {
            let f = File::open(&manifest_path).map_err(OxenError::Io)?;
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let line = line.map_err(OxenError::Io)?;
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                match Self::parse_edit(&line) {
                    Ok(edit) => {
                        Self::apply_edit_to_levels(&mut levels, &edit, &mut next_file_number)
                    }
                    Err(e) => {
                        log::warn!("Skipping malformed MANIFEST line: {e}");
                    }
                }
            }
        }

        let manifest_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)
            .map_err(OxenError::Io)?;

        Ok(VersionSet {
            levels: Arc::new(RwLock::new(levels)),
            manifest_file: Mutex::new(manifest_file),
            next_file_number: AtomicU64::new(next_file_number),
        })
    }

    /// Allocate the next unique file number.
    pub fn next_file_number(&self) -> u64 {
        let n = self.next_file_number.fetch_add(1, Ordering::SeqCst);
        // Persist the new watermark
        let _ = self.log_edit(&ManifestEdit::NextFileNumber(n + 1));
        n
    }

    /// Record that a new SSTable was created.
    pub fn add_file(&mut self, meta: SSTableMeta) -> Result<()> {
        let edit = ManifestEdit::AddFile {
            level: meta.level,
            file_number: meta.file_number,
            file_path: meta.file_path.to_string_lossy().into_owned(),
            file_size: meta.file_size,
            first_key: meta.first_key.clone(),
            last_key: meta.last_key.clone(),
            entry_count: meta.entry_count,
        };
        self.log_edit(&edit)?;
        let level = meta.level;
        let fn_ = meta.file_number;
        self.levels.write()[level].insert(fn_, meta);
        Ok(())
    }

    /// Record that an SSTable was removed (superseded by compaction).
    pub fn remove_file(&mut self, level: usize, file_number: u64) -> Result<()> {
        let edit = ManifestEdit::RemoveFile { level, file_number };
        self.log_edit(&edit)?;
        self.levels.write()[level].remove(&file_number);
        Ok(())
    }

    /// Return a snapshot of all live SSTables at `level`.
    pub fn files_at_level(&self, level: usize) -> Vec<SSTableMeta> {
        self.levels.read()[level].values().cloned().collect()
    }

    /// Return the total bytes stored at a level.
    pub fn level_size_bytes(&self, level: usize) -> u64 {
        self.levels.read()[level].values().map(|m| m.file_size).sum()
    }

    /// Number of files at L0 (used for compaction trigger).
    pub fn l0_file_count(&self) -> usize {
        self.levels.read()[0].len()
    }

    // ---- serialization ----

    fn log_edit(&self, edit: &ManifestEdit) -> Result<()> {
        let line = Self::format_edit(edit);
        let mut file = self.manifest_file.lock();
        writeln!(file, "{}", line).map_err(OxenError::Io)?;
        file.sync_all().map_err(OxenError::Io)?;
        Ok(())
    }

    fn format_edit(edit: &ManifestEdit) -> String {
        match edit {
            ManifestEdit::AddFile {
                level,
                file_number,
                file_path,
                file_size,
                first_key,
                last_key,
                entry_count,
            } => {
                format!(
                    "ADD {} {} {} {} {} {} {}",
                    level,
                    file_number,
                    file_size,
                    entry_count,
                    hex::encode_bytes(first_key),
                    hex::encode_bytes(last_key),
                    file_path,
                )
            }
            ManifestEdit::RemoveFile { level, file_number } => {
                format!("REM {} {}", level, file_number)
            }
            ManifestEdit::NextFileNumber(n) => {
                format!("NEXT {}", n)
            }
        }
    }

    fn parse_edit(line: &str) -> Result<ManifestEdit> {
        let parts: Vec<&str> = line.splitn(10, ' ').collect();
        match parts.as_slice() {
            ["ADD", level, file_number, file_size, entry_count, first_key_hex, last_key_hex, file_path] => {
                Ok(ManifestEdit::AddFile {
                    level: level
                        .parse()
                        .map_err(|_| OxenError::Manifest("bad level".into()))?,
                    file_number: file_number
                        .parse()
                        .map_err(|_| OxenError::Manifest("bad file_number".into()))?,
                    file_path: file_path.to_string(),
                    file_size: file_size
                        .parse()
                        .map_err(|_| OxenError::Manifest("bad file_size".into()))?,
                    first_key: hex::decode_bytes(first_key_hex)?,
                    last_key: hex::decode_bytes(last_key_hex)?,
                    entry_count: entry_count
                        .parse()
                        .map_err(|_| OxenError::Manifest("bad entry_count".into()))?,
                })
            }
            ["REM", level, file_number] => Ok(ManifestEdit::RemoveFile {
                level: level
                    .parse()
                    .map_err(|_| OxenError::Manifest("bad level".into()))?,
                file_number: file_number
                    .parse()
                    .map_err(|_| OxenError::Manifest("bad file_number".into()))?,
            }),
            ["NEXT", n] => Ok(ManifestEdit::NextFileNumber(
                n.parse()
                    .map_err(|_| OxenError::Manifest("bad next_file_number".into()))?,
            )),
            _ => Err(OxenError::Manifest(format!(
                "Unknown MANIFEST line: {line}"
            ))),
        }
    }

    fn apply_edit_to_levels(
        levels: &mut Vec<HashMap<u64, SSTableMeta>>,
        edit: &ManifestEdit,
        next_file_number: &mut u64,
    ) {
        match edit {
            ManifestEdit::AddFile {
                level,
                file_number,
                file_path,
                file_size,
                first_key,
                last_key,
                entry_count,
            } => {
                if *level < levels.len() {
                    levels[*level].insert(
                        *file_number,
                        SSTableMeta {
                            file_number: *file_number,
                            level: *level,
                            file_path: PathBuf::from(file_path),
                            file_size: *file_size,
                            first_key: first_key.clone(),
                            last_key: last_key.clone(),
                            entry_count: *entry_count,
                        },
                    );
                    if *file_number >= *next_file_number {
                        *next_file_number = *file_number + 1;
                    }
                }
            }
            ManifestEdit::RemoveFile { level, file_number } => {
                if *level < levels.len() {
                    levels[*level].remove(file_number);
                }
            }
            ManifestEdit::NextFileNumber(n) => {
                if *n > *next_file_number {
                    *next_file_number = *n;
                }
            }
        }
    }
}

/// Minimal hex utilities (avoids pulling in another crate)
mod hex {
    use crate::error::{OxenError, Result};

    pub fn encode_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode_bytes(s: &str) -> Result<Vec<u8>> {
        if s.len() % 2 != 0 {
            return Err(OxenError::Manifest(format!("Odd hex length: {s}")));
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|_| OxenError::Manifest(format!("Bad hex byte at {i}: {s}")))
            })
            .collect()
    }
}
