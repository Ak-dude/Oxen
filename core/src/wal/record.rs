use crc32fast::Hasher as CrcHasher;
use crate::error::{OxenError, Result};

/// WAL block framing type — mirrors LevelDB's log format
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Record fits entirely in one block
    Full = 1,
    /// First fragment of a multi-block record
    First = 2,
    /// Middle fragment
    Middle = 3,
    /// Last fragment
    Last = 4,
    /// Checkpoint marker: all data up to this point is durable
    Checkpoint = 5,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(RecordType::Full),
            2 => Ok(RecordType::First),
            3 => Ok(RecordType::Middle),
            4 => Ok(RecordType::Last),
            5 => Ok(RecordType::Checkpoint),
            _ => Err(OxenError::Corruption(format!("Unknown WAL record type: {v}"))),
        }
    }
}

/// Operation type embedded in the WAL record payload
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Put = 1,
    Delete = 2,
}

impl OpType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(OpType::Put),
            2 => Ok(OpType::Delete),
            _ => Err(OxenError::Corruption(format!("Unknown WAL op type: {v}"))),
        }
    }
}

/// A single logical WAL record (after reassembly of fragments)
///
/// Wire layout of the payload:
/// ```text
/// [op_type: u8][seq_no: u64 LE][key_len: u32 LE][key: bytes][value_len: u32 LE][value: bytes]
/// ```
/// For Delete ops, value_len == 0 and value is empty.
///
/// Each physical frame written to disk:
/// ```text
/// [crc32c: u32 LE][length: u16 LE][type: u8][payload: bytes]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WALRecord {
    pub op: OpType,
    pub seq_no: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Header size: crc32c(4) + length(2) + type(1) = 7 bytes
pub const FRAME_HEADER_SIZE: usize = 7;

impl WALRecord {
    pub fn new_put(seq_no: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        WALRecord { op: OpType::Put, seq_no, key, value }
    }

    pub fn new_delete(seq_no: u64, key: Vec<u8>) -> Self {
        WALRecord { op: OpType::Delete, seq_no, key, value: Vec::new() }
    }

    /// Encode the logical record into raw bytes (the payload carried inside frames)
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u32;
        let val_len = self.value.len() as u32;
        let total = 1 + 8 + 4 + self.key.len() + 4 + self.value.len();
        let mut buf = Vec::with_capacity(total);

        buf.push(self.op as u8);
        buf.extend_from_slice(&self.seq_no.to_le_bytes());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(&self.value);
        buf
    }

    /// Decode a logical record from a payload byte slice
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < 13 {
            return Err(OxenError::Corruption(format!(
                "WAL record payload too short: {} bytes",
                payload.len()
            )));
        }
        let op = OpType::from_u8(payload[0])?;
        let seq_no = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        let key_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;

        if payload.len() < 13 + key_len + 4 {
            return Err(OxenError::Corruption("WAL record truncated at key".into()));
        }
        let key = payload[13..13 + key_len].to_vec();
        let val_len_offset = 13 + key_len;
        let val_len = u32::from_le_bytes(
            payload[val_len_offset..val_len_offset + 4].try_into().unwrap(),
        ) as usize;
        let val_offset = val_len_offset + 4;

        if payload.len() < val_offset + val_len {
            return Err(OxenError::Corruption("WAL record truncated at value".into()));
        }
        let value = payload[val_offset..val_offset + val_len].to_vec();

        Ok(WALRecord { op, seq_no, key, value })
    }
}

/// Encode a single physical frame: [crc32c(4)][len(2)][type(1)][payload]
pub fn encode_frame(record_type: RecordType, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u16;
    // CRC covers: type byte + payload
    let mut hasher = CrcHasher::new();
    hasher.update(&[record_type as u8]);
    hasher.update(payload);
    let crc = hasher.finalize();

    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.push(record_type as u8);
    frame.extend_from_slice(payload);
    frame
}

/// Decode one physical frame from a byte slice. Returns (record_type, payload, consumed_bytes).
pub fn decode_frame(buf: &[u8]) -> Result<(RecordType, Vec<u8>, usize)> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Err(OxenError::Corruption(format!(
            "Frame header too short: {} bytes",
            buf.len()
        )));
    }
    let expected_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let payload_len = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
    let record_type = RecordType::from_u8(buf[6])?;

    if buf.len() < FRAME_HEADER_SIZE + payload_len {
        return Err(OxenError::Corruption(format!(
            "Frame payload truncated: need {}, have {}",
            payload_len,
            buf.len() - FRAME_HEADER_SIZE
        )));
    }

    let payload = &buf[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + payload_len];

    // Verify CRC
    let mut hasher = CrcHasher::new();
    hasher.update(&[record_type as u8]);
    hasher.update(payload);
    let actual_crc = hasher.finalize();

    if actual_crc != expected_crc {
        return Err(OxenError::CrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    Ok((record_type, payload.to_vec(), FRAME_HEADER_SIZE + payload_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_put_record() {
        let rec = WALRecord::new_put(42, b"hello".to_vec(), b"world".to_vec());
        let encoded = rec.encode();
        let decoded = WALRecord::decode(&encoded).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn roundtrip_delete_record() {
        let rec = WALRecord::new_delete(99, b"byekey".to_vec());
        let encoded = rec.encode();
        let decoded = WALRecord::decode(&encoded).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn frame_crc_mismatch_detected() {
        let payload = b"test payload";
        let mut frame = encode_frame(RecordType::Full, payload);
        // Corrupt byte 10 (inside payload)
        let last = frame.len() - 1;
        frame[last] ^= 0xFF;
        assert!(decode_frame(&frame).is_err());
    }
}
