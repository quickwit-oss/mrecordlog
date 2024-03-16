use std::sync::Arc;

mod block_read_write;

pub use block_read_write::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};
pub mod error;
mod frame;
mod mem;
mod multi_record_log;
mod record;
mod recordlog;
mod rolling;

pub use mem::PagesBuf;
pub use multi_record_log::{MultiRecordLog, SyncPolicy};

#[derive(Debug)]
pub struct Record<'a> {
    pub position: u64,
    pub payload: PagesBuf<'a>,
}

impl<'a> Record<'a> {
    #[cfg(test)]
    pub fn payload_equal(&self, payload: &[u8]) -> bool {
        self.payload.to_cow() == payload
    }
}

#[derive(Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct FileNumber {
    file_number: Arc<u64>,
}

impl FileNumber {
    fn new(file_number: u64) -> Self {
        FileNumber {
            file_number: Arc::new(file_number),
        }
    }

    /// Returns whether there is no clone of this FileNumber in existance.
    ///
    /// /!\ care should be taken to not have some other code store a &FileNumber which could alias
    /// with self as it might then be sementically incorrect to delete content based only on this
    /// returning `true`.
    pub fn can_be_deleted(&self) -> bool {
        Arc::strong_count(&self.file_number) == 1
    }

    #[cfg(test)]
    pub fn unroll(&self, tracker: &crate::rolling::FileTracker) -> Vec<u64> {
        let mut file = self.clone();
        let mut file_numbers = Vec::new();
        loop {
            file_numbers.push(file.file_number());
            if let Some(next_file) = tracker.next(&file) {
                file = next_file;
            } else {
                return file_numbers;
            }
        }
    }

    pub fn filename(&self) -> String {
        format!("wal-{:020}", self.file_number)
    }

    #[cfg(test)]
    pub fn file_number(&self) -> u64 {
        *self.file_number
    }

    #[cfg(test)]
    pub fn for_test(file_number: u64) -> Self {
        FileNumber::new(file_number)
    }
}

/// Resources used by mrecordlog
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceUsage {
    /// Actual size of the memory used
    pub memory_used_bytes: usize,
    /// Capacity allocated, a part of which may be unused right now
    pub memory_allocated_bytes: usize,
    /// Disk size used
    pub disk_used_bytes: usize,
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod proptests;

pub trait Serializable<'a>: Sized {
    /// Clears the buffer first.
    fn serialize(&self, buffer: &mut Vec<u8>);
    fn deserialize(buffer: &'a [u8]) -> Option<Self>;
}

impl<'a> Serializable<'a> for &'a str {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        buffer.extend_from_slice(self.as_bytes())
    }

    fn deserialize(buffer: &'a [u8]) -> Option<Self> {
        std::str::from_utf8(buffer).ok()
    }
}
