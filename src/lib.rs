mod block_read_write;
pub use self::block_read_write::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};

pub mod error;
mod frame;
mod mem;
mod multi_record_log;
mod record;
mod recordlog;
mod rolling;

pub use self::multi_record_log::{FlushPolicy, MultiRecordLog};

#[cfg(test)]
mod tests;

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
