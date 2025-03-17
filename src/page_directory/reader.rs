use std::fs::File;
use std::io::{self, BufWriter, SeekFrom};

use super::page_list::PageList;
use super::writer::PageListWriter;
use super::{Directory, PAGE_SIZE};
use crate::page_directory::PageRangeRef;
use crate::{BlockRead, FileLike, BLOCK_NUM_BYTES};

const NUM_BLOCK_PER_PAGE: usize = PAGE_SIZE / BLOCK_NUM_BYTES;

pub struct PageListReader<F: FileLike = File> {
    file: F,
    page_list: PageList,
    page_buffer: Box<[u8]>,
    pub(crate) block_id: usize,
    page_id: u32,
}

impl<F: FileLike> PageListReader<F> {
    pub fn new(page_directory: Directory<F>) -> io::Result<Self> {
        let Directory { file, page_list } = page_directory;
        let mut page_list_reader = PageListReader {
            file,
            page_list,
            page_buffer: vec![0u8; PAGE_SIZE].into_boxed_slice(),
            page_id: 0u32,
            block_id: 0,
        };
        page_list_reader.load_page()?;
        Ok(page_list_reader)
    }

    fn block_id_within_page(&self) -> usize {
        self.block_id % NUM_BLOCK_PER_PAGE
    }

    // Loads the current page.
    //
    // Returns Ok(false) if we have reached the end of the file.
    fn load_page(&mut self) -> std::io::Result<bool> {
        let page_ord = self.block_id / NUM_BLOCK_PER_PAGE;
        let Some(page_id) = self.page_list.page_id(page_ord) else {
            return Ok(false);
        };
        self.page_id = page_id;
        let Some(page_start_offset) = self.page_list.page_start_offset(page_ord) else {
            return Ok(false);
        };
        self.file.seek(SeekFrom::Start(page_start_offset as u64))?;
        self.file.read_exact(&mut self.page_buffer[..])?;
        Ok(true)
    }

    pub fn into_writer(self, num_bytes: u64) -> io::Result<PageListWriter<F>> {
        let mut page_list_writer = PageListWriter {
            page_list: self.page_list,
            wrt: BufWriter::with_capacity(BLOCK_NUM_BYTES, self.file),
            cursor: num_bytes,
        };
        page_list_writer.reposition_write_head()?;
        Ok(page_list_writer)
    }
}

impl<F: FileLike> BlockRead for PageListReader<F> {
    type Session = PageRangeRef;

    fn start_session(&self) -> Self::Session {
        self.page_list.new_page_range_ref(self.page_id)
    }

    fn next_block(&mut self, session: &mut Self::Session) -> std::io::Result<bool> {
        self.block_id += 1;
        let block_id_within_page = self.block_id_within_page();
        if block_id_within_page == 0 {
            if self.load_page()? {
                session.num_pages += 1u16;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(true)
        }
    }

    fn block(&self) -> &[u8; crate::BLOCK_NUM_BYTES] {
        let start_offset = self.block_id_within_page() * crate::BLOCK_NUM_BYTES;
        let block_slice = &self.page_buffer[start_offset..start_offset + crate::BLOCK_NUM_BYTES];
        block_slice.try_into().unwrap()
    }
}
