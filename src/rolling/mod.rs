mod directory;
mod file_number;

pub use self::directory::{Directory, RollingReader, RollingWriter};
pub use self::file_number::{FileNumber, FileTracker};

const FRAME_NUM_BYTES: usize = 1 << 15;

#[cfg(not(test))]
const NUM_BLOCKS_PER_FILE: usize = 1 << 12;

#[cfg(test)]
const NUM_BLOCKS_PER_FILE: usize = 4;

const FILE_NUM_BYTES: usize = FRAME_NUM_BYTES * NUM_BLOCKS_PER_FILE;
#[cfg(test)]
mod tests;
