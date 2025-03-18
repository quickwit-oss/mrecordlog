use std::fs::File;
use std::io;


enum PageListSlot {
    First,
    Second,
}

impl PageListSlot {
    fn flip(&self) -> Self {
        match self {
            PageListSlot::First => PageListSlot::Second,
            PageListSlot::Second => PageListSlot::First,
        }
    }
}

use super::{PageHandle, PageId};

pub struct PageList {
    num_pages: usize,
    // This is the order in which the pages should be read,
    // were we to read the log entirely again.
    //
    // It includes both the written page and the page that are
    // still free.
    written_page_ids: Vec<PageId>,
    available_page_ids: Vec<PageId>,
    page_handles: Vec<PageHandle>,

    // Stores the slot where the current page list is stored.
    page_list_selector: PageListSlot,
}

impl PageList {
    pub fn new(num_pages: usize) -> Self {
        PageList {
            num_pages,
            written_page_ids: Vec::new(),
            available_page_ids: (0..num_pages as PageId).collect(),
            page_handles: vec![PageHandle::default(); num_pages],
            page_list_selector: PageListSlot::First,
        }
    }

    pub fn read_page_list(num_pages: usize, read: &mut dyn io::Read) -> io::Result<()> {
        todo!();
    }

    pub fn write_slot(&self, wrt: &mut W)

    fn write_page_list<W: io::Write + io::Seek>(&mut self, wrt: &mut W) -> io::Result<()> {
        wrt.seek(io::SeekFrom::Start(i))?;
        Ok(())
    }
}
