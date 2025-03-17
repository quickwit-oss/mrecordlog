use std::io;

use crate::PersistAction;

pub const BLOCK_NUM_BYTES: usize = 32_768;

/// A block read is supposed to be positioned on a block at its initialization.
///
/// In other words, it is not necessary to call `next_block` a first time
/// before calling `block()`.
pub trait BlockRead {
    type Session;

    fn start_session(&self) -> Self::Session;

    /// Loads the next block.
    /// If `Ok(true)` is returned, the new block is available through
    /// `.block()`.
    ///
    /// If `Ok(false)` is returned, the end of the `BlockReader`
    /// has been reached and the content of `block()` could be anything.
    fn next_block(&mut self, read_session: &mut Self::Session) -> io::Result<bool>;

    /// A `BlockReader` is always position on a specific block.
    ///
    /// There are no "unitialized state".
    ///
    /// # Panics
    ///
    /// May panic if the last call to next_block returned `false`
    /// or returned an io::Error.
    fn block(&self) -> &[u8; BLOCK_NUM_BYTES];
}

pub trait BlockWrite {
    type Session;

    fn start_write_session(&mut self) -> io::Result<Self::Session>;

    fn make_room(&mut self, num_bytes: u64) -> io::Result<()>;

    /// Must panic if buf is larger than `num_bytes_remaining_in_block`.
    /// Not that this trait does not have next_block() method.
    ///
    /// We automatically go to the next block after the current block has been entirely written.
    fn write(&mut self, buf: &[u8], write_session: &mut Self::Session) -> io::Result<()>;
    /// Persist the data following the `persist_action`.
    fn persist(&mut self, persist_action: PersistAction) -> io::Result<()>;
    /// Number of bytes that can be added in the block.
    fn num_bytes_remaining_in_block(&self) -> usize;
}

pub struct ArrayReader<'a> {
    block: [u8; BLOCK_NUM_BYTES],
    data: &'a [u8],
}

impl<'a> From<&'a [u8]> for ArrayReader<'a> {
    fn from(data: &'a [u8]) -> Self {
        assert!(data.len() >= BLOCK_NUM_BYTES);
        let mut block = [0u8; BLOCK_NUM_BYTES];
        let (head, tail) = data.split_at(BLOCK_NUM_BYTES);
        block.copy_from_slice(head);
        Self { block, data: tail }
    }
}

impl BlockRead for ArrayReader<'_> {
    type Session = ();

    fn start_session(&self) -> Self::Session {}

    fn next_block(&mut self, _session: &mut Self::Session) -> io::Result<bool> {
        if self.data.len() < BLOCK_NUM_BYTES {
            return Ok(false);
        }
        let (first_block, remaining) = self.data.split_at(BLOCK_NUM_BYTES);
        self.block.copy_from_slice(first_block);
        self.data = remaining;
        Ok(true)
    }

    fn block(&self) -> &[u8; BLOCK_NUM_BYTES] {
        &self.block
    }
}

#[derive(Default)]
pub struct VecBlockWriter {
    cursor: usize,
    buffer: Vec<u8>,
}

fn ceil_to_block(len: usize) -> usize {
    BLOCK_NUM_BYTES * ((len + BLOCK_NUM_BYTES - 1) / BLOCK_NUM_BYTES)
}

impl From<VecBlockWriter> for Vec<u8> {
    fn from(vec_block_writer: VecBlockWriter) -> Vec<u8> {
        vec_block_writer.buffer
    }
}

impl BlockWrite for VecBlockWriter {
    type Session = ();
    fn make_room(&mut self, num_bytes: u64) -> io::Result<()> {
        // TODO consider just doubling for performance.
        let new_len = ceil_to_block(self.cursor + num_bytes as usize);
        self.buffer.resize(new_len, 0u8);
        Ok(())
    }
    fn write(&mut self, buf: &[u8], _session: &mut Self::Session) -> io::Result<()> {
        assert!(buf.len() <= self.num_bytes_remaining_in_block());
        if self.cursor + buf.len() > self.buffer.len() {
            let new_len = ceil_to_block((self.cursor + buf.len()) * 2 + 1);
            self.buffer.resize(new_len, 0u8);
        }
        self.buffer[self.cursor..][..buf.len()].copy_from_slice(buf);
        self.cursor += buf.len();
        Ok(())
    }

    fn persist(&mut self, _persist_action: PersistAction) -> io::Result<()> {
        Ok(())
    }

    fn num_bytes_remaining_in_block(&self) -> usize {
        BLOCK_NUM_BYTES - (self.cursor % BLOCK_NUM_BYTES)
    }

    fn start_write_session(&mut self) -> io::Result<Self::Session> {
        Ok(())
    }
}
