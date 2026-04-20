use std::fs::File;
use std::io::{self, BufWriter, Seek as _, SeekFrom, Write};

use super::page_list::PageList;
use super::PageId;
use crate::page_directory::{PageRangeRef, PAGE_SIZE};
use crate::{BlockWrite, FileLike, PersistAction, BLOCK_NUM_BYTES};

pub struct PageListWriter<F: FileLike = File> {
    pub(crate) page_list: PageList,
    pub(crate) wrt: BufWriter<F>,
    // cursor is the offset at which we are trying to write.
    // This is not a physical offset (an offset on disk), but a logical one.
    // The page list is here to convert this into an actual physical offset.
    pub(crate) cursor: u64,
}

impl<F: FileLike> BlockWrite for PageListWriter<F> {
    type Session = PageRangeRef;

    #[inline(always)]
    fn start_write_session(&mut self) -> io::Result<Self::Session> {
        let Some(page_id) = self.get_write_page() else {
            return Err(std::io::Error::new(
                io::ErrorKind::StorageFull,
                "All pages are used",
            ));
        };
        let initial_num_pages = if self.cursor % PAGE_SIZE as u64 == 0 {
            // We haven't loaded the page yet.
            0
        } else {
            1
        };
        let page_ref = self
            .page_list
            .new_page_range_ref_with_num_pages(page_id, initial_num_pages);
        Ok(page_ref)
    }

    fn write(&mut self, buf: &[u8], page_range_ref: &mut PageRangeRef) -> std::io::Result<()> {
        if buf.len() == 0 {
            return Ok(());
        }
        assert!(buf.len() <= self.num_bytes_remaining_in_block());
        if self.cursor % PAGE_SIZE as u64 == 0 {
            // If we are about to run the first write on a page.
            // We need to seek into its physical address.
            page_range_ref.add_page();
            self.reposition_write_head()?;
        }
        self.wrt.write_all(buf)?;
        self.cursor += buf.len() as u64;
        Ok(())
    }

    fn persist(&mut self, persist_action: crate::PersistAction) -> std::io::Result<()> {
        match persist_action {
            PersistAction::FlushAndFsync => {
                self.wrt.flush()?;
                self.wrt.get_mut().fsyncdata()?;
                Ok(())
            }
            PersistAction::Flush => {
                // This will flush the buffer of the BufWriter to the underlying OS.
                self.wrt.flush()
            }
        }
    }

    #[inline]
    fn num_bytes_remaining_in_block(&self) -> usize {
        BLOCK_NUM_BYTES - (self.cursor as usize % BLOCK_NUM_BYTES)
    }

    fn make_room(&mut self, num_bytes: u64) -> io::Result<()> {
        if self.remaining_capacity() >= num_bytes {
            return Ok(());
        }
        self.gc()?;
        if self.remaining_capacity() >= num_bytes {
            return Ok(());
        }
        let error_msg = format!(
            "mrecordlog capacity reached. cursor={}, num_pages={}, requested={num_bytes}",
            self.cursor,
            self.page_list.num_pages()
        );
        Err(io::Error::new(io::ErrorKind::OutOfMemory, error_msg))
    }
}

impl<F: FileLike> PageListWriter<F> {
    fn gc(&mut self) -> io::Result<()> {
        let num_pages_delete = self.page_list.gc(self.cursor, &mut self.wrt)?;
        // We need to update the cursor.
        self.cursor -= num_pages_delete as u64 * PAGE_SIZE as u64;
        self.reposition_write_head()?;
        Ok(())
    }

    #[inline(always)]
    pub fn num_pages(&self) -> u32 {
        self.page_list.num_pages()
    }

    pub fn num_used_pages(&self) -> u32 {
        self.cursor.div_ceil(PAGE_SIZE as u64) as u32
    }

    // Returns the physical file offset range that corresponds to the current page.
    #[inline(always)]
    fn get_write_page(&mut self) -> Option<PageId> {
        // if self.cursor == self.page_list.num_pages() as u64 * PAGE_SIZE as u64 {
        //     self.gc()?;
        // }
        let page_ord = (self.cursor / PAGE_SIZE as u64) as usize;
        self.page_list.page_id(page_ord)
    }

    #[inline(always)]
    fn remaining_capacity(&self) -> u64 {
        let total_capacity = self.page_list.num_pages() as u64 * PAGE_SIZE as u64;
        total_capacity - self.cursor
    }

    #[cfg(test)]
    pub fn into_file(self) -> F {
        self.wrt.into_inner().map_err(|_| ()).unwrap()
    }

    // Seek into the file to our current write position.
    pub fn reposition_write_head(&mut self) -> io::Result<()> {
        let page_ord = (self.cursor / PAGE_SIZE as u64) as usize;
        let Some(page_start_offset) = self.page_list.page_start_offset(page_ord) else {
            return Err(std::io::Error::new(
                io::ErrorKind::StorageFull,
                "All pages are used",
            ));
        };
        let offset_within_page = self.cursor - page_ord as u64 * PAGE_SIZE as u64;
        self.wrt
            .seek(SeekFrom::Start(page_start_offset + offset_within_page))?;
        Ok(())
    }
}
