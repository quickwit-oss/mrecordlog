use std::io;

use crate::frame::{FrameType, Header, HEADER_LEN};
use crate::{BlockWrite, BLOCK_NUM_BYTES};

pub struct FrameWriter {
    // temporary buffer, not storing anything in particular after any function returns
    buffer: Box<[u8; BLOCK_NUM_BYTES]>,
}

impl Default for FrameWriter {
    fn default() -> FrameWriter {
        FrameWriter {
            buffer: Box::new([0u8; BLOCK_NUM_BYTES]),
        }
    }
}

impl FrameWriter {
    /// Writes a frame. The payload has to be lower than the
    /// remaining space in the frame as defined
    /// by `max_writable_frame_length`.
    pub fn write_frame(
        &mut self,
        frame_type: FrameType,
        payload: &[u8],
        wrt: &mut impl BlockWrite,
    ) -> io::Result<()> {
        let num_bytes_remaining_in_block = wrt.num_bytes_remaining_in_block();
        if num_bytes_remaining_in_block < HEADER_LEN {
            let zero_bytes = [0u8; HEADER_LEN];
            wrt.write(&zero_bytes[..num_bytes_remaining_in_block])?;
        }
        let record_len = HEADER_LEN + payload.len();
        let (buffer_header, buffer_record) = self.buffer[..record_len].split_at_mut(HEADER_LEN);
        buffer_record.copy_from_slice(payload);
        Header::for_payload(frame_type, payload).serialize(buffer_header);
        wrt.write(&self.buffer[..record_len])?;
        Ok(())
    }

    /// Returns the maximum amount of bytes that can be written.
    pub fn max_writable_frame_length(&self, wrt: &mut impl BlockWrite) -> usize {
        let available_num_bytes_in_block = wrt.num_bytes_remaining_in_block();
        if available_num_bytes_in_block >= HEADER_LEN {
            available_num_bytes_in_block - HEADER_LEN
        } else {
            // That block is finished. We will have to pad it.
            BLOCK_NUM_BYTES - HEADER_LEN
        }
    }
}
