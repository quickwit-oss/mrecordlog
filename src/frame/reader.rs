use std::io;

use thiserror::Error;

use crate::frame::{FrameType, FrameWriter, Header, HEADER_LEN};
use crate::rolling::{RollingReader, RollingWriter};
use crate::{BlockRead, BLOCK_NUM_BYTES};

pub struct FrameReader<R> {
    reader: R,

    /// In block cursor
    cursor: usize,

    // The current block is corrupted.
    block_corrupted: bool,
}

#[derive(Error, Debug)]
pub enum ReadFrameError {
    #[error("Io error: {0}")]
    IoError(#[from] io::Error),
    #[error("Corruption in frame")]
    Corruption,
    #[error("Next frame not available")]
    NotAvailable,
}

impl<R: BlockRead + Unpin> FrameReader<R> {
    pub fn open(reader: R) -> Self {
        FrameReader {
            reader,
            cursor: 0,
            block_corrupted: false,
        }
    }

    pub fn read(&self) -> &R {
        &self.reader
    }

    // Returns the number of bytes remaining into
    // the current block.
    //
    // These bytes may or may not be available.
    fn num_bytes_to_end_of_block(&self) -> usize {
        crate::BLOCK_NUM_BYTES - self.cursor
    }

    async fn go_to_next_block_if_necessary(&mut self) -> Result<(), ReadFrameError> {
        let num_bytes_to_end_of_block = self.num_bytes_to_end_of_block();
        let need_to_skip_block = self.block_corrupted || num_bytes_to_end_of_block < HEADER_LEN;
        if !need_to_skip_block {
            return Ok(());
        }
        self.reader.next_block().await?;
        self.cursor = 0;
        self.block_corrupted = false;
        Ok(())
    }

    // Attempt to read the header of the next frame
    // This method does not consume any bytes (which is why it is called get and not read).
    async fn get_frame_header(&mut self) -> Result<Header, ReadFrameError> {
        let header_bytes: &[u8] = &self.reader.block()[self.cursor..][..HEADER_LEN];
        if header_bytes == [0u8; HEADER_LEN] {
            return Err(ReadFrameError::NotAvailable);
        }
        self.cursor += HEADER_LEN;
        match Header::deserialize(header_bytes) {
            Some(header) => Ok(header),
            None => {
                self.block_corrupted = true;
                Err(ReadFrameError::Corruption)
            }
        }
    }

    // Reads the next frame.
    pub async fn read_frame(&mut self) -> Result<(FrameType, &[u8]), ReadFrameError> {
        self.go_to_next_block_if_necessary().await?;
        let header = self.get_frame_header().await?;
        if self.cursor + header.len() > BLOCK_NUM_BYTES {
            // The number of bytes for this frame would span over
            // the next block.
            // This is a corruption for which we need to drop the entire block.
            self.block_corrupted = true;
            return Err(ReadFrameError::Corruption);
        }
        let frame_payload = &self.reader.block()[self.cursor..][..header.len()];
        self.cursor += header.len();
        if !header.check(frame_payload) {
            // The CRC check is wrong.
            // We do not necessarily need to corrupt the block.
            //
            // With a little luck, a single frame payload byte was corrupted
            // but the frame length was correct.
            return Err(ReadFrameError::Corruption);
        }
        Ok((header.frame_type(), frame_payload))
    }
}

impl FrameReader<RollingReader> {
    pub async fn into_writer(self) -> io::Result<FrameWriter<RollingWriter>> {
        let mut rolling_writer: RollingWriter = self.reader.into_writer().await?;
        rolling_writer.forward(self.cursor).await?;
        Ok(FrameWriter::create(rolling_writer))
    }
}
