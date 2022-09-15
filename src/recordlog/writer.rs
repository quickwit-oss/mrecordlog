use tokio::io;

use crate::block_read_write::VecBlockWriter;
use crate::frame::{FrameType, FrameWriter};
use crate::rolling::{FileNumber, RollingWriter};
use crate::{BlockWrite, Serializable};

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
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

impl<W: BlockWrite + Unpin> From<FrameWriter<W>> for RecordWriter<W> {
    fn from(frame_writer: FrameWriter<W>) -> Self {
        RecordWriter {
            frame_writer,
            buffer: Vec::with_capacity(10_000),
        }
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    #[cfg(test)]
    pub fn into_writer(self) -> W {
        self.frame_writer.into_writer()
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    /// Writes a record.
    ///
    /// Even if this call returns `Ok(())`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub async fn write_record(&mut self, record: impl Serializable<'_>) -> io::Result<()> {
        let mut is_first_frame = true;
        self.buffer.clear();
        record.serialize(&mut self.buffer);
        let mut payload = &self.buffer[..];
        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length()
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame);
            self.frame_writer
                .write_frame(frame_type, frame_payload)
                .await?;
            is_first_frame = false;
            if is_last_frame {
                break;
            }
        }
        Ok(())
    }

    /// Flushes and sync the data to disk.
    pub async fn flush(&mut self) -> io::Result<()> {
        // Empty the application buffer.
        self.frame_writer.flush().await?;
        Ok(())
    }

    pub fn get_underlying_wrt(&self) -> &W {
        self.frame_writer.get_underlying_wrt()
    }
}

impl RecordWriter<RollingWriter> {
    pub async fn gc(&mut self) -> io::Result<()> {
        self.frame_writer.gc().await
    }

    pub fn current_file(&mut self) -> &FileNumber {
        self.get_underlying_wrt().current_file()
    }
}

impl RecordWriter<VecBlockWriter> {
    #[cfg(test)]
    pub fn in_memory() -> Self {
        FrameWriter::create(VecBlockWriter::default()).into()
    }
}
