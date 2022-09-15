// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

use super::FileNumber;
use crate::rolling::FILE_NUM_BYTES;
use crate::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};

#[derive(Clone)]
pub struct Directory {
    dir: PathBuf,
    first_file: FileNumber,
}

fn filename_to_position(file_name: &str) -> Option<u32> {
    if file_name.len() != 24 {
        return None;
    }
    if !file_name.starts_with("wal-") {
        return None;
    }
    let seq_number_str = &file_name[4..];
    if !seq_number_str
        .as_bytes()
        .iter()
        .all(|b| (b'0'..=b'9').contains(b))
    {
        return None;
    }
    file_name[4..].parse::<u32>().ok()
}

fn filepath(dir: &Path, file_number: &FileNumber) -> PathBuf {
    dir.join(&file_number.filename())
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
    pub async fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut file_numbers: Vec<u32> = Default::default();
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
        let first_file = if let Some(first_file) = FileNumber::from_file_numbers(file_numbers) {
            first_file
        } else {
            let file_number = FileNumber::default();
            create_file(dir_path, &file_number).await?;
            file_number
        };
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            first_file,
        })
    }

    pub fn first_file_number(&self) -> &FileNumber {
        &self.first_file
    }

    async fn gc(&mut self) -> io::Result<()> {
        let mut file_cursor = &self.first_file;
        while file_cursor.can_be_deleted() {
            let filepath = filepath(&self.dir, file_cursor);
            tokio::fs::remove_file(&filepath).await?;
            if let Some(next_file) = file_cursor.next() {
                self.first_file = next_file.clone();
                file_cursor = &self.first_file;
            }
        }
        Ok(())
    }

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
}

pub struct RollingReader {
    file: File,
    directory: Directory,
    file_number: FileNumber,
    block_id: usize, //< number of the next block to read.
    block: Box<[u8; BLOCK_NUM_BYTES]>,
}

impl RollingReader {
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
            file: BufWriter::with_capacity(1 << 15, self.file),
            offset,
            file_number: self.file_number.clone(),
            directory: self.directory,
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
        loop {
            let next_file_number = if let Some(next_file_number) = self.file_number.next() {
                next_file_number
            } else {
                return Ok(false);
            };
            let mut next_file: File = self.directory.open_file(&next_file_number).await?;
            let success = read_block(&mut next_file, &mut self.block).await?;
            if success {
                self.block_id += 1;
                self.file = next_file;
                self.file_number = next_file_number;
                return Ok(true);
            }
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
    directory: Directory,
}

impl RollingWriter {
    pub async fn gc(&mut self) -> io::Result<()> {
        self.directory.gc().await
    }

    pub async fn forward(&mut self, num_bytes: usize) -> io::Result<()> {
        self.file.seek(SeekFrom::Current(num_bytes as i64)).await?;
        self.offset += num_bytes;
        Ok(())
    }
    pub fn current_file(&self) -> &FileNumber {
        &self.file_number
    }

    #[cfg(test)]
    pub fn list_file_numbers(&self) -> Vec<u32> {
        self.directory.first_file_number().unroll()
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
            if let Some(next_file_number) = self.file_number.next() {
                self.file = BufWriter::with_capacity(
                    1 << 15,
                    self.directory.open_file(&next_file_number).await?,
                );
                self.file_number = next_file_number;
                self.offset = 0;
            } else {
                let next_file_number = self.file_number.inc();
                self.file = BufWriter::with_capacity(
                    1 << 15,
                    create_file(&self.directory.dir, &next_file_number).await?,
                );
                self.file_number = next_file_number;
                self.offset = 0;
            }
        }
        self.offset += buf.len();
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
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
}
