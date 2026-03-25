use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum OxenError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Corruption detected: {0}")]
    Corruption(String),

    #[error("CRC mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Key not found")]
    KeyNotFound,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Compaction error: {0}")]
    Compaction(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("SSTable error: {0}")]
    SSTable(String),

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Engine closed")]
    EngineClosed,

    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("Database already open at {0}")]
    AlreadyOpen(String),

    #[error("Null pointer error: {0}")]
    NullPointer(String),
}

pub type Result<T> = std::result::Result<T, OxenError>;
