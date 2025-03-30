use std::io;

use crate::frame::{FrameType, FrameWriter};
use crate::{BlockWrite, Serializable};

#[derive(Default)]
pub struct RecordWriter {
    frame_writer: FrameWriter,
    buffer: Vec<u8>,
}

fn frame_type(is_first_frame: bool, is_last_frame: bool) -> FrameType {
    match (is_first_frame, is_last_frame) {
        (true, true) => FrameType::Full,
        (true, false) => FrameType::First,
        (false, true) => FrameType::Last,
        (false, false) => FrameType::Middle,
    }
}

impl RecordWriter {
    /// Writes a record.
    ///
    /// Even if this call returns `Ok(())`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub fn write_record<'a>(
        &mut self,
        record: impl Serializable<'a>,
        wrt: &mut impl BlockWrite,
    ) -> io::Result<()> {
        let mut is_first_frame = true;
        self.buffer.clear();
        record.serialize(&mut self.buffer);
        let mut payload = &self.buffer[..];
        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length(wrt)
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame);
            self.frame_writer
                .write_frame(frame_type, frame_payload, wrt)?;
            is_first_frame = false;
            if is_last_frame {
                break;
            }
        }
        Ok(())
    }
}
