use std::fs::File;
use std::io;


pub enum PageListSector {
    First,
    Second,
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
}

impl PageList {
    pub fn read_page_list(num_pages: usize, read: &mut dyn io::Read) -> io::Result<()> {
        todo!();
    }

    fn write_page_list(&mut self, file: &mut File) -> io::Result<()> {
        todo!();
    }
}
