use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use crate::error::{OxenError, Result};
use crate::wal::record::{decode_frame, RecordType, WALRecord};

/// Iterates over WAL segment files in order and yields deserialized WALRecords.
///
/// Used during engine recovery to replay all operations since the last flush.
pub struct WALReader {
    segments: Vec<PathBuf>,
    current_index: usize,
    current_reader: Option<BufReader<File>>,
    /// Reassembly buffer for fragmented (First/Middle/Last) records
    frag_buf: Vec<u8>,
}

impl WALReader {
    /// Open all `.wal` files in `dir`, sorted numerically, for sequential replay.
    pub fn open(dir: &Path) -> Result<Self> {
        let mut segments: Vec<PathBuf> = std::fs::read_dir(dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |e| e == "wal"))
            .collect();

        segments.sort_by(|a, b| {
            let num_a = Self::segment_number(a).unwrap_or(0);
            let num_b = Self::segment_number(b).unwrap_or(0);
            num_a.cmp(&num_b)
        });

        let mut reader = WALReader {
            segments,
            current_index: 0,
            current_reader: None,
            frag_buf: Vec::new(),
        };
        reader.open_next_segment()?;
        Ok(reader)
    }

    fn segment_number(path: &Path) -> Option<u64> {
        path.file_stem()?.to_str()?.parse().ok()
    }

    fn open_next_segment(&mut self) -> Result<bool> {
        if self.current_index >= self.segments.len() {
            self.current_reader = None;
            return Ok(false);
        }
        let path = self.segments[self.current_index].clone();
        let file = File::open(&path).map_err(OxenError::Io)?;
        self.current_reader = Some(BufReader::with_capacity(256 * 1024, file));
        self.frag_buf.clear();
        self.current_index += 1;
        Ok(true)
    }

    /// Read one complete logical WALRecord, advancing through segments as needed.
    fn read_next(&mut self) -> Result<Option<WALRecord>> {
        loop {
            if self.current_reader.is_none() {
                return Ok(None);
            }

            match self.read_one_frame()? {
                FrameResult::Record(r) => return Ok(Some(r)),
                FrameResult::Skip => continue,
                FrameResult::Eof => {
                    if !self.open_next_segment()? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Attempt to read one physical frame from `current_reader`.
    fn read_one_frame(&mut self) -> Result<FrameResult> {
        // Read 7-byte frame header
        let mut header = [0u8; 7];
        {
            let reader = self.current_reader.as_mut().unwrap();
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(FrameResult::Eof);
                }
                Err(e) => return Err(OxenError::Io(e)),
            }
        }

        let payload_len = u16::from_le_bytes([header[4], header[5]]) as usize;

        // Read payload
        let mut frame_buf = vec![0u8; 7 + payload_len];
        frame_buf[..7].copy_from_slice(&header);
        {
            let reader = self.current_reader.as_mut().unwrap();
            if payload_len > 0 {
                reader.read_exact(&mut frame_buf[7..]).map_err(OxenError::Io)?;
            }
        }

        let (record_type, payload, _) = decode_frame(&frame_buf)?;

        match record_type {
            RecordType::Checkpoint => Ok(FrameResult::Skip),
            RecordType::Full => {
                let record = WALRecord::decode(&payload)?;
                Ok(FrameResult::Record(record))
            }
            RecordType::First => {
                self.frag_buf = payload;
                Ok(FrameResult::Skip)
            }
            RecordType::Middle => {
                self.frag_buf.extend_from_slice(&payload);
                Ok(FrameResult::Skip)
            }
            RecordType::Last => {
                self.frag_buf.extend_from_slice(&payload);
                let assembled = std::mem::take(&mut self.frag_buf);
                if assembled.is_empty() {
                    return Err(OxenError::Corruption(
                        "WAL Last frame without preceding First frame".into(),
                    ));
                }
                let record = WALRecord::decode(&assembled)?;
                Ok(FrameResult::Record(record))
            }
        }
    }
}

enum FrameResult {
    Record(WALRecord),
    Skip,
    Eof,
}

impl Iterator for WALReader {
    type Item = Result<WALRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(Some(r)) => Some(Ok(r)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
