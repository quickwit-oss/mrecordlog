mod directory;
mod file_number;

use std::io;

pub use self::directory::{Directory, RollingReader, RollingWriter};
pub use self::file_number::{FileNumber, FileTracker};
use crate::frame::{FrameReader, FrameWriter};
use crate::recordlog::{RecordReader, RecordWriter};

const FRAME_NUM_BYTES: usize = 1 << 15;

#[cfg(not(test))]
const NUM_BLOCKS_PER_FILE: usize = 1 << 12;

#[cfg(test)]
const NUM_BLOCKS_PER_FILE: usize = 4;

const FILE_NUM_BYTES: usize = FRAME_NUM_BYTES * NUM_BLOCKS_PER_FILE;
#[cfg(test)]
mod tests;

impl FrameReader<RollingReader> {
    pub fn into_writer(self) -> io::Result<FrameWriter<RollingWriter>> {
        let mut rolling_writer: RollingWriter = self.reader.into_writer()?;
        rolling_writer.forward(self.cursor)?;
        Ok(FrameWriter::create(rolling_writer))
    }
}

impl FrameWriter<RollingWriter> {
    pub fn directory(&mut self) -> &mut Directory {
        &mut self.wrt.directory
    }
}

impl RecordReader<RollingReader> {
    pub fn into_writer(self) -> io::Result<RecordWriter<RollingWriter>> {
        let frame_writer: FrameWriter<RollingWriter> = self.frame_reader.into_writer()?;
        Ok(RecordWriter::from(frame_writer))
    }
}

// TODO remove me
impl RecordWriter<RollingWriter> {
    pub fn directory(&mut self) -> &mut Directory {
        self.frame_writer.directory()
    }

    pub fn current_file(&mut self) -> &FileNumber {
        self.get_underlying_wrt().current_file()
    }

    pub fn size(&self) -> usize {
        self.get_underlying_wrt().size()
    }
}
