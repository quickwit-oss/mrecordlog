use std::borrow::Cow;
use std::fs::File;
use std::io::{self, BufWriter, Cursor};

mod block_read_write;

pub use block_read_write::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};
pub mod error;
mod frame;
mod mem;

mod multi_record_log;
mod page_directory;
mod persist_policy;
mod record;
mod recordlog;

pub use mem::{QueueSummary, QueuesSummary};
pub use multi_record_log::MultiRecordLog;
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
    pub num_pages: u32,
    pub num_used_pages: u32,
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod mockfile;

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

pub trait FileLikeWrite: io::Write + io::Seek {
    fn fsyncdata(&mut self) -> io::Result<()>;
    fn set_len(&mut self, num_bytes: u64) -> io::Result<()>;
}

pub trait FileLike: io::Read + FileLikeWrite {}

impl<F: FileLikeWrite> FileLikeWrite for BufWriter<F> {
    fn fsyncdata(&mut self) -> io::Result<()> {
        self.get_mut().fsyncdata()
    }

    fn set_len(&mut self, num_bytes: u64) -> io::Result<()> {
        self.get_mut().set_len(num_bytes)
    }
}

impl FileLikeWrite for File {
    fn fsyncdata(&mut self) -> io::Result<()> {
        self.sync_data()
    }

    fn set_len(&mut self, num_bytes: u64) -> io::Result<()> {
        File::set_len(self, num_bytes)
    }
}

impl FileLike for File {}

impl FileLikeWrite for Cursor<Vec<u8>> {
    fn fsyncdata(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&mut self, num_bytes: u64) -> io::Result<()> {
        self.get_mut().resize(num_bytes as usize, 0u8);
        Ok(())
    }
}

impl FileLike for Cursor<Vec<u8>> {}
