use std::io;

use crate::PersistAction;

pub const BLOCK_NUM_BYTES: usize = 32_768;

pub trait BlockRead {
    /// Loads the next block.
    /// If `Ok(true)` is returned, the new block is available through
    /// `.block()`.
    ///
    /// If `Ok(false)` is returned, the end of the `BlockReader`
    /// has been reached and the content of `block()` could be anything.
    fn next_block(&mut self) -> io::Result<bool>;

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
    /// Must panic if buf is larger than `num_bytes_remaining_in_block`.
    fn write(&mut self, buf: &[u8]) -> io::Result<()>;
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

impl<'a> BlockRead for ArrayReader<'a> {
    fn next_block(&mut self) -> io::Result<bool> {
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
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
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
}
