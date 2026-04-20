use std::fs::File;
use std::io;
use std::path::Path;

mod header;
mod page_list;
mod page_refcounts;
mod reader;
mod writer;

use page_list::PageList;
pub(crate) use page_refcounts::PageRangeRef;
pub use reader::PageListReader;
pub use writer::PageListWriter;

use crate::frame::{FrameReader, FrameWriter};
use crate::recordlog::{RecordReader, RecordWriter};
use crate::{FileLike, BLOCK_NUM_BYTES};

pub const PAGE_SIZE: usize = 1 << 16; // 65,536 bytes
pub const MIN_NUM_PAGES: usize = 2;

pub type PageId = u32;

pub struct Directory<F: FileLike = File> {
    file: F,
    page_list: PageList,
}

enum CreateOrOpen {
    Created,
    Opened,
}

pub(crate) fn compute_slot_len(num_pages: u32) -> usize {
    8 + // epoch
    3 * num_pages as usize + // page ids encoded over 3 bytes each
    4 // checksum
}

fn create_or_open_page_file(path: &Path, num_pages: usize) -> io::Result<(CreateOrOpen, File)> {
    let len = num_pages * PAGE_SIZE;
    match std::fs::File::create_new(path) {
        Ok(file) => {
            file.set_len(len as u64)?;
            Ok((CreateOrOpen::Created, file))
        }
        Err(io_err) if io_err.kind() == io::ErrorKind::AlreadyExists => {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            Ok((CreateOrOpen::Opened, file))
        }
        Err(err) => Err(err),
    }
}

impl Directory {
    pub fn create_or_open(path: &Path, len: u64) -> io::Result<Directory> {
        let num_pages = len as usize / PAGE_SIZE;
        if num_pages < MIN_NUM_PAGES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file length is too small. requested {}, should be at least {}",
                    len,
                    PAGE_SIZE * MIN_NUM_PAGES
                ),
            ));
        }
        let (create_or_open, file) = create_or_open_page_file(path, num_pages)?;
        match create_or_open {
            CreateOrOpen::Created => Directory::create(num_pages, file),
            CreateOrOpen::Opened => Directory::open(num_pages, file),
        }
    }
}

impl<F: FileLike> Directory<F> {
    fn create(num_pages: usize, mut file: F) -> io::Result<Directory<F>> {
        let page_list = PageList::initialize_page_file(num_pages, &mut file)?;
        let directory = Directory { file, page_list };
        Ok(directory)
    }

    fn open(num_pages: usize, mut file: F) -> io::Result<Directory<F>> {
        file.seek(io::SeekFrom::Start(0u64))?;
        let header = header::deserialize_header(&mut file)?;
        if header.num_pages as usize != num_pages {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "number of pages does not match existing file",
            ));
        }
        let page_list = PageList::load(header, &mut file)?;
        Ok(Directory { file, page_list })
    }
}

impl RecordReader<PageListReader> {
    pub fn into_writer(self) -> io::Result<RecordWriter<PageListWriter>> {
        let frame_writer: FrameWriter<PageListWriter> = self.frame_reader.into_writer()?;
        Ok(RecordWriter::from(frame_writer))
    }
}

impl FrameReader<PageListReader> {
    pub fn into_writer(self) -> io::Result<FrameWriter<PageListWriter>> {
        let offset: u64 = self.reader.block_id as u64 * BLOCK_NUM_BYTES as u64 + self.cursor as u64;
        let page_list_writer: PageListWriter = self.reader.into_writer(offset as u64)?;
        Ok(FrameWriter::create(page_list_writer))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::reader::PageListReader;
    use super::*;
    use crate::{BlockRead as _, BlockWrite as _, BLOCK_NUM_BYTES};

    #[test]
    fn test_simple_no_write() {
        let fake_file = {
            let directory = Directory::create(100, Cursor::new(Vec::new())).unwrap();
            let mut page_list_reader = PageListReader::new(directory).unwrap();
            let mut session = page_list_reader.start_session();
            assert_eq!(session.num_pages, 1);
            loop {
                if !page_list_reader.next_block(&mut session).unwrap() {
                    break;
                }
            }
            assert_eq!(session.start_page_id, 0u32);
            assert_eq!(session.num_pages, 100u16);
            page_list_reader.into_writer(0u64).unwrap().into_file()
        };

        let directory = Directory::open(100, fake_file).unwrap();
        let page_list_reader = PageListReader::new(directory).unwrap();
        let _writer = page_list_reader.into_writer(0);
    }

    #[test]
    fn test_simple_write() {
        let fake_file0 = Cursor::new(Vec::new());
        let directory = Directory::create(100, fake_file0).unwrap();
        let mut page_list_reader = PageListReader::new(directory).unwrap();
        let mut session = page_list_reader.start_session();
        loop {
            if !page_list_reader.next_block(&mut session).unwrap() {
                break;
            }
        }
        let fake_file1 = page_list_reader.into_writer(0u64).unwrap().into_file();

        let directory = Directory::open(100, fake_file1).unwrap();
        let page_list_reader = PageListReader::new(directory).unwrap();
        let mut writer = page_list_reader.into_writer(0).unwrap();
        let mut session = writer.start_write_session().unwrap();
        assert_eq!(writer.num_bytes_remaining_in_block(), BLOCK_NUM_BYTES);
        writer.write(b"hello", &mut session).unwrap();
        assert_eq!(session.num_pages, 1u16);
        assert_eq!(
            writer.num_bytes_remaining_in_block(),
            BLOCK_NUM_BYTES - b"hello".len()
        );
        let fake_file2 = writer.into_file();

        let directory = Directory::open(100, fake_file2).unwrap();
        let page_list_reader = PageListReader::new(directory).unwrap();
        assert_eq!(&page_list_reader.block()[..5], b"hello");
    }
}
