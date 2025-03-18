use std::borrow::Cow;

mod block_read_write;

pub use block_read_write::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};
pub mod error;
mod frame;
mod mem;

// mod multi_record_log;
mod persist_policy;
// mod record;
// mod recordlog;
mod page_directory;
pub mod rolling;

pub use mem::{QueueSummary, QueuesSummary};
// pub use multi_record_log::MultiRecordLog;
pub(crate) use persist_policy::PersistState;
pub use persist_policy::{PersistAction, PersistPolicy};

#[derive(Debug, PartialEq, Eq)]
pub struct Record<'a> {
    pub position: u64,
    pub payload: Cow<'a, [u8]>,
}

impl<'a> Record<'a> {
    pub fn new(position: u64, payload: &'a [u8]) -> Self {
        Record {
            position,
            payload: Cow::Borrowed(payload),
        }
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

// #[cfg(test)]
// mod tests;

#[cfg(test)]
mod mockfile;

// #[cfg(test)]
// mod proptests;

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
