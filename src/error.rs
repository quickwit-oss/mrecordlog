use std::io;

use thiserror::Error;

#[derive(Debug, Copy, Clone)]
pub struct AlreadyExists;

#[derive(Error, Debug)]
pub enum CreateQueueError {
    #[error("Already exists")]
    AlreadyExists,
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
}

impl From<AlreadyExists> for CreateQueueError {
    fn from(_: AlreadyExists) -> Self {
        CreateQueueError::AlreadyExists
    }
}

#[derive(Error, Debug)]
pub enum DeleteQueueError {
    #[error("Missing queue")]
    MissingQueue(String),
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
}

impl From<MissingQueue> for DeleteQueueError {
    fn from(missing_queue: MissingQueue) -> Self {
        DeleteQueueError::MissingQueue(missing_queue.0)
    }
}

#[derive(Debug, Error)]
#[error("MultiRecordCorruption")]
pub struct MultiRecordCorruption;

impl From<MultiRecordCorruption> for ReadRecordError {
    fn from(_: MultiRecordCorruption) -> ReadRecordError {
        ReadRecordError::Corruption
    }
}

#[derive(Error, Debug)]
pub enum TruncateError {
    #[error("Missing queue: {0}")]
    MissingQueue(String),
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
}

impl From<MissingQueue> for TruncateError {
    fn from(missing_queue: MissingQueue) -> Self {
        TruncateError::MissingQueue(missing_queue.0)
    }
}

#[derive(Error, Debug)]
pub enum AppendError {
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
    #[error("Missing queue: {0}")]
    MissingQueue(String),
    #[error("Past")]
    Past,
}

impl From<MissingQueue> for AppendError {
    fn from(missing_queue: MissingQueue) -> Self {
        AppendError::MissingQueue(missing_queue.0)
    }
}

#[derive(Debug)]
pub struct MissingQueue(pub String);

impl std::fmt::Display for MissingQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Missing queue: {}", self.0)
    }
}

impl std::error::Error for MissingQueue {}

#[derive(Error, Debug)]
pub enum ReadRecordError {
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
    #[error("Corruption")]
    Corruption,
}

#[derive(Error, Debug)]
pub enum HeaderError {
    #[error("invalid magic number: found {magic_number}")]
    InvalidMagicNumber { magic_number: u32 },
    #[error("invalid checksum")]
    InvalidChecksum,
    #[error("unsupported version: {version}")]
    UnsupportedVersion { version: u32 },
}
