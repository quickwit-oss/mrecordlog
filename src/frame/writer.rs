use std::io;

use crate::frame::{FrameType, Header, HEADER_LEN};
use crate::{BlockWrite, PersistAction, BLOCK_NUM_BYTES};

pub struct FrameWriter<W> {
    pub(crate) wrt: W,
    // temporary buffer, not storing anything in particular after any function returns
    buffer: Box<[u8; BLOCK_NUM_BYTES]>,
}

impl<W: BlockWrite + Unpin> FrameWriter<W> {
    pub fn create(wrt: W) -> Self {
        FrameWriter {
            wrt,
            buffer: Box::new([0u8; BLOCK_NUM_BYTES]),
        }
    }

    pub fn start_session(&mut self) -> io::Result<W::Session> {
        self.wrt.start_write_session()
    }

    pub fn make_room(&mut self, num_bytes: u64) -> io::Result<()> {
        // Framing adds some overhead. We can however compute an upperbound of the amount of room
        // that will be needed. The worst case scenario is if we start at the very end of a
        // block and the first frame is empty and we end up just writing a header.
        const MAX_EFFECTIVE_BLOCK_BYTES: u64 = (BLOCK_NUM_BYTES - HEADER_LEN) as u64;
        let num_blocks_upperbound = 1 + num_bytes.div_ceil(MAX_EFFECTIVE_BLOCK_BYTES);
        let room_needed_upperbound: u64 = HEADER_LEN as u64 * num_blocks_upperbound + num_bytes;
        self.wrt.make_room(room_needed_upperbound)
    }

    /// Writes a frame. The payload has to be lower than the
    /// remaining space in the frame as defined
    /// by `max_writable_frame_length`.
    pub fn write_frame(
        &mut self,
        frame_type: FrameType,
        payload: &[u8],
        session: &mut W::Session,
    ) -> io::Result<()> {
        let num_bytes_remaining_in_block = self.wrt.num_bytes_remaining_in_block();
        if num_bytes_remaining_in_block < HEADER_LEN {
            let zero_bytes = [0u8; HEADER_LEN];
            self.wrt
                .write(&zero_bytes[..num_bytes_remaining_in_block], session)?;
        }
        let record_len = HEADER_LEN + payload.len();
        let (buffer_header, buffer_record) = self.buffer[..record_len].split_at_mut(HEADER_LEN);
        buffer_record.copy_from_slice(payload);
        Header::for_payload(frame_type, payload).serialize(buffer_header);
        self.wrt.write(&self.buffer[..record_len], session)?;
        Ok(())
    }

    /// Flush the buffered writer used in the FrameWriter.
    ///
    /// When writing to a file, this performs a syscall and
    /// the OS will be in charge of eventually writing the data
    /// to disk, but this is not sufficient to ensure durability.
    pub fn persist(&mut self, persist_action: PersistAction) -> io::Result<()> {
        self.wrt.persist(persist_action)
    }

    /// Returns the maximum amount of bytes that can be written.
    pub fn max_writable_frame_length(&self) -> usize {
        let available_num_bytes_in_block = self.wrt.num_bytes_remaining_in_block();
        if available_num_bytes_in_block >= HEADER_LEN {
            available_num_bytes_in_block - HEADER_LEN
        } else {
            // That block is finished. We will have to pad it.
            BLOCK_NUM_BYTES - HEADER_LEN
        }
    }

    pub fn get_underlying_wrt(&self) -> &W {
        &self.wrt
    }

    #[cfg(test)]
    pub fn into_writer(self) -> W {
        self.wrt
    }
}
