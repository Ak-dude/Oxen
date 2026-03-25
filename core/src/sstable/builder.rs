use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::{OxenError, Result};
use crate::sstable::block::BlockBuilder;
use crate::sstable::bloom::BloomFilter;
use crate::sstable::index::IndexBlock;
use crate::sstable::SSTableMeta;

/// SSTable file layout:
/// ```text
/// [data block 0]
/// [data block 1]
/// ...
/// [data block N]
/// [index block]
/// [bloom filter]
/// [footer: index_offset(8) + index_size(4) + bloom_offset(8) + bloom_size(4) + magic(8)]
/// ```
///
/// Footer is always exactly 32 bytes at the end of the file.
pub const SSTABLE_MAGIC: u64 = 0x4F78656E44425F31; // "OxenDB_1"
pub const FOOTER_SIZE: usize = 32;

pub struct SSTableBuilder {
    writer: BufWriter<File>,
    file_path: PathBuf,
    file_number: u64,
    level: usize,
    block_builder: BlockBuilder,
    index: IndexBlock,
    all_keys: Vec<Vec<u8>>,
    current_block_offset: u64,
    last_key_in_block: Vec<u8>,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    entry_count: u64,
    block_size_bytes: usize,
    bits_per_key: usize,
    bytes_written: u64,
}

impl SSTableBuilder {
    /// Create a new SSTable file at `path`.
    pub fn create(
        path: &Path,
        file_number: u64,
        level: usize,
        block_size_bytes: usize,
        bits_per_key: usize,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(OxenError::Io)?;

        Ok(SSTableBuilder {
            writer: BufWriter::with_capacity(1024 * 1024, file),
            file_path: path.to_path_buf(),
            file_number,
            level,
            block_builder: BlockBuilder::new(true),
            index: IndexBlock::new(),
            all_keys: Vec::new(),
            current_block_offset: 0,
            last_key_in_block: Vec::new(),
            first_key: None,
            last_key: None,
            entry_count: 0,
            block_size_bytes,
            bits_per_key,
            bytes_written: 0,
        })
    }

    /// Add a sorted key-value pair to the SSTable.
    /// Caller must ensure keys arrive in ascending order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());

        self.block_builder.add(key, value);
        self.last_key_in_block = key.to_vec();
        self.all_keys.push(key.to_vec());
        self.entry_count += 1;

        if self.block_builder.current_size_estimate() >= self.block_size_bytes {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Finalise the file: flush remaining block, write index + bloom + footer.
    pub fn finish(mut self) -> Result<SSTableMeta> {
        // Flush any remaining entries
        if !self.block_builder.is_empty() {
            self.flush_block()?;
        }

        // Write index block
        let index_offset = self.bytes_written;
        let index_bytes = self.index.encode();
        let index_size = index_bytes.len() as u32;
        self.write_bytes(&index_bytes)?;

        // Write bloom filter
        let bloom_offset = self.bytes_written;
        let key_refs: Vec<&[u8]> = self.all_keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&key_refs, self.bits_per_key);
        let bloom_bytes = bloom.encode();
        let bloom_size = bloom_bytes.len() as u32;
        self.write_bytes(&bloom_bytes)?;

        // Write fixed-size footer (32 bytes)
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&index_offset.to_le_bytes());
        footer[8..12].copy_from_slice(&index_size.to_le_bytes());
        footer[12..20].copy_from_slice(&bloom_offset.to_le_bytes());
        footer[20..24].copy_from_slice(&bloom_size.to_le_bytes());
        footer[24..32].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        self.write_bytes(&footer)?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all().map_err(OxenError::Io)?;

        let file_size = self.bytes_written;

        Ok(SSTableMeta {
            file_number: self.file_number,
            level: self.level,
            file_path: self.file_path,
            file_size,
            first_key: self.first_key.unwrap_or_default(),
            last_key: self.last_key.unwrap_or_default(),
            entry_count: self.entry_count,
        })
    }

    fn flush_block(&mut self) -> Result<()> {
        let builder = std::mem::replace(
            &mut self.block_builder,
            BlockBuilder::new(true),
        );
        if builder.is_empty() {
            return Ok(());
        }
        let block_offset = self.current_block_offset;
        let (block_bytes, compressed) = builder.finish();
        let block_size = block_bytes.len() as u32;

        self.write_bytes(&block_bytes)?;
        self.index.add_entry(
            self.last_key_in_block.clone(),
            block_offset,
            block_size,
            compressed,
        );
        self.current_block_offset = self.bytes_written;
        self.last_key_in_block.clear();
        Ok(())
    }

    fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data).map_err(OxenError::Io)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }
}
