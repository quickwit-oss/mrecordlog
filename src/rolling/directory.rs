use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tracing::debug;

use super::{FileNumber, FileTracker};
use crate::rolling::{FILE_NUM_BYTES, FRAME_NUM_BYTES};
use crate::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};

pub struct Directory {
    dir: PathBuf,
    pub(crate) files: FileTracker,
}

fn filename_to_position(file_name: &str) -> Option<u64> {
    if file_name.len() != 24 {
        return None;
    }
    if !file_name.starts_with("wal-") {
        return None;
    }
    let seq_number_str = &file_name[4..];
    if !seq_number_str.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    file_name[4..].parse::<u64>().ok()
}

pub(crate) fn filepath(dir: &Path, file_number: &FileNumber) -> PathBuf {
    dir.join(file_number.filename())
}

fn filepath_for_number(dir: &Path, file_number: u64) -> PathBuf {
    dir.join(super::file_number::filename_from_number(file_number))
}

async fn create_file(dir_path: &Path, file_number: &FileNumber) -> io::Result<File> {
    let new_filepath = filepath(dir_path, file_number);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&new_filepath)
        .await?;
    file.set_len(FILE_NUM_BYTES as u64).await?;
    file.seek(SeekFrom::Start(0)).await?;
    Ok(file)
}

impl Directory {
    /// Open a `Directory`, or create a new, empty, one. `dir_path` must exist and be a directory.
    pub async fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut file_numbers: Vec<u64> = Default::default();
        let mut read_dir = tokio::fs::read_dir(dir_path).await?;
        while let Some(dir_entry) = read_dir.next_entry().await? {
            if !dir_entry.file_type().await?.is_file() {
                continue;
            }
            let file_name = if let Some(file_name) = dir_entry.file_name().to_str() {
                file_name.to_string()
            } else {
                continue;
            };
            if let Some(seq_number) = filename_to_position(&file_name) {
                file_numbers.push(seq_number);
            }
        }
        let files = if let Some(files) = FileTracker::from_file_numbers(file_numbers) {
            files
        } else {
            let files = FileTracker::new();
            let file_number = files.first();
            create_file(dir_path, file_number).await?;
            files
        };
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            files,
        })
    }

    /// Get the first still used FileNumber.
    pub fn first_file_number(&self) -> &FileNumber {
        self.files.first()
    }

    /// Returns true if some file could be GCed.
    pub fn has_files_that_can_be_deleted(&self) -> bool {
        self.files.count() >= 2 && self.files.first().can_be_deleted()
    }

    /// Delete FileNumbers and the associated wal files no longer used.
    ///
    /// We never delete the last file.
    pub(crate) async fn gc(&mut self) -> io::Result<()> {
        while let Some(file) = self.files.take_first_unused() {
            let filepath = filepath(&self.dir, &file);
            debug!(file=%filepath.display(), "gc remove file");
            tokio::fs::remove_file(&filepath).await?;
        }
        Ok(())
    }

    /// Open the wal file with the provided FileNumber.
    pub async fn open_file(&self, file_number: &FileNumber) -> io::Result<File> {
        let filepath = filepath(&self.dir, file_number);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&filepath)
            .await?;
        file.seek(SeekFrom::Start(0u64)).await?;
        Ok(file)
    }

    async fn sync_directory(&self) -> io::Result<()> {
        let path = self.dir.clone();
        tokio::task::spawn_blocking(move || fsync_data_directory(&path))
            .await
            .map_err(|_| io::Error::other("fsync panicked".to_string()))?
    }

    pub async fn sync_data(&self, files: std::ops::Range<u64>) -> io::Result<()> {
        let directory_sync_future = self.sync_directory();
        let file_sync_future = async {
            for file_number in files.into_iter() {
                let filepath = filepath_for_number(&self.dir, file_number);
                match OpenOptions::new()
                    .read(true)
                    .create(false)
                    .open(&filepath)
                    .await
                {
                    Ok(file) => {
                        file.sync_data().await?;
                    }
                    Err(err) if err.kind() == io::ErrorKind::NotFound => (),
                    Err(err) => return Err(err),
                }
            }
            Ok(())
        };

        tokio::try_join!(directory_sync_future, file_sync_future)?;
        Ok(())
    }
}

/// fsyncdata a directory. This is a blocking operation, you should run it in a blocking task.
fn fsync_data_directory(path: &Path) -> io::Result<()> {
    // sadly, neither tokio nor the std comes with anything to sync a directory (see also
    // https://github.com/tokio-rs/tokio/issues/1922 ).
    // So we do things manually.
    use std::os::unix::ffi::OsStrExt;

    let target = path.as_os_str().as_bytes().as_ptr().cast::<libc::c_char>();

    unsafe {
        // safety: target is a valid path, and provided flags are valid for open()
        let fd = libc::open(target, libc::O_RDONLY | libc::O_DIRECTORY);
        if fd < 0 {
            let errno = *libc::__errno_location();
            return Err(io::Error::from_raw_os_error(errno));
        }
        // safety: fd is a valid file descriptor
        let res = libc::fsync(fd);
        if res == 0 {
            // safety: fd is a valid file descriptor
            let res = libc::close(fd);
            if res == 0 {
                Ok(())
            } else {
                let errno = *libc::__errno_location();
                Err(io::Error::from_raw_os_error(errno))
            }
        } else {
            let errno = *libc::__errno_location();

            // safety: fd is a valid file descriptor
            // we already got an error, don't handle one on close (if any)
            let _res = libc::close(fd);
            Err(io::Error::from_raw_os_error(errno))
        }
    }
}

pub struct RollingReader {
    file: File,
    directory: Directory,
    file_number: FileNumber,
    block_id: usize,
    block: Box<[u8; BLOCK_NUM_BYTES]>,
}

impl RollingReader {
    /// Open a directory for reading.
    pub async fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path).await?;
        let first_file = directory.first_file_number().clone();
        let mut file = directory.open_file(&first_file).await?;
        let mut block = Box::new([0u8; BLOCK_NUM_BYTES]);
        file.read_exact(&mut *block).await?;
        Ok(RollingReader {
            file,
            directory,
            file_number: first_file.clone(),
            block_id: 0,
            block,
        })
    }

    pub fn current_file(&self) -> &FileNumber {
        &self.file_number
    }

    /// Creates a write positioned at the beginning of the last read block.
    ///
    /// If no block was read, positions itself at the beginning.
    pub async fn into_writer(mut self) -> io::Result<RollingWriter> {
        let offset = self.block_id * crate::BLOCK_NUM_BYTES;
        self.file.seek(SeekFrom::Start(offset as u64)).await?;
        Ok(RollingWriter {
            file: BufWriter::with_capacity(FRAME_NUM_BYTES, self.file),
            offset,
            file_number: self.file_number.clone(),
            directory: self.directory,
            last_fsync_file_number: self.file_number.file_number(),
        })
    }
}

async fn read_block(file: &mut File, block: &mut [u8; BLOCK_NUM_BYTES]) -> io::Result<bool> {
    match file.read_exact(block).await {
        Ok(len) => {
            assert_eq!(len, BLOCK_NUM_BYTES);
            Ok(true)
        }
        Err(io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(io_err) => Err(io_err),
    }
}

#[async_trait]
impl BlockRead for RollingReader {
    async fn next_block(&mut self) -> io::Result<bool> {
        let success = read_block(&mut self.file, &mut self.block).await?;
        if success {
            self.block_id += 1;
            return Ok(true);
        }

        let mut next_file_number =
            if let Some(next_file_number) = self.directory.files.next(&self.file_number) {
                next_file_number
            } else {
                return Ok(false);
            };

        loop {
            let mut next_file: File = self.directory.open_file(&next_file_number).await?;
            let success = read_block(&mut next_file, &mut self.block).await?;
            if success {
                self.block_id = 0;
                self.file = next_file;
                self.file_number = next_file_number;
                return Ok(true);
            }

            next_file_number =
                if let Some(next_file_number) = self.directory.files.next(&next_file_number) {
                    next_file_number
                } else {
                    return Ok(false);
                };
        }
    }

    fn block(&self) -> &[u8; BLOCK_NUM_BYTES] {
        &self.block
    }
}

pub struct RollingWriter {
    file: BufWriter<File>,
    offset: usize,
    file_number: FileNumber,
    pub(crate) directory: Directory,
    // we don't want this to be a FileNumber because it would keep that file alive
    last_fsync_file_number: u64,
}

impl RollingWriter {
    /// Move forward of `num_bytes` without actually writing anything.
    pub async fn forward(&mut self, num_bytes: usize) -> io::Result<()> {
        self.file.seek(SeekFrom::Current(num_bytes as i64)).await?;
        self.offset += num_bytes;
        Ok(())
    }

    pub fn current_file(&self) -> &FileNumber {
        &self.file_number
    }

    pub fn size(&self) -> usize {
        self.directory.files.count() * FILE_NUM_BYTES
    }

    #[cfg(test)]
    pub fn list_file_numbers(&self) -> Vec<u64> {
        self.directory
            .first_file_number()
            .unroll(&self.directory.files)
    }
}

#[async_trait]
impl BlockWrite for RollingWriter {
    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        assert!(buf.len() <= self.num_bytes_remaining_in_block());
        if self.offset + buf.len() > FILE_NUM_BYTES {
            self.file.flush().await?;

            let (file_number, file) =
                if let Some(next_file_number) = self.directory.files.next(&self.file_number) {
                    let file = self.directory.open_file(&next_file_number).await?;
                    (next_file_number, file)
                } else {
                    let next_file_number = self.directory.files.inc(&self.file_number);
                    let file = create_file(&self.directory.dir, &next_file_number).await?;
                    (next_file_number, file)
                };

            self.file = BufWriter::with_capacity(FRAME_NUM_BYTES, file);
            self.file_number = file_number;
            self.offset = 0;
        }
        self.offset += buf.len();
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn flush(&mut self, fsync: bool) -> io::Result<()> {
        if fsync {
            let current_file_number = self.file_number.file_number();
            tokio::try_join!(
                async {
                    self.file.flush().await?;
                    self.file.get_ref().sync_data().await
                },
                self.directory
                    .sync_data(self.last_fsync_file_number..current_file_number)
            )?;
            self.last_fsync_file_number = current_file_number;
        } else {
            self.file.flush().await?;
        }
        Ok(())
    }

    fn num_bytes_remaining_in_block(&self) -> usize {
        BLOCK_NUM_BYTES - (self.offset % BLOCK_NUM_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::filename_to_position;

    #[test]
    fn test_filename_to_seq_number_invalid_prefix_rejected() {
        assert_eq!(filename_to_position("fil-00000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_padding_rejected() {
        assert_eq!(filename_to_position("wal-0000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_len_rejected() {
        assert_eq!(filename_to_position("wal-000000000000000000011"), None);
    }

    #[test]
    fn test_filename_to_seq_number_simple() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[test]
    fn test_filename_to_seq_number_33b() {
        // 2**32, overflow a u32
        assert_eq!(
            filename_to_position("wal-00000000004294967296"),
            Some(4294967296)
        );
    }

    #[test]
    fn test_filename_to_seq_number_64b() {
        // 2**64-1, max supported value
        assert_eq!(
            filename_to_position(&format!("wal-{}", u64::MAX)),
            Some(u64::MAX)
        );
    }
}
