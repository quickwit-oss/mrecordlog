use std::{fs::File, io, path::Path, sync::Arc};

mod page_list;
mod directory;

use page_list::PageList;

pub const MAGIC_NUMBER: u32 = 1_778_463_742_u32;
pub const PAGE_SIZE: usize = 1 << 18; // 262,144 bytes
pub const MIN_NUM_PAGES: usize = 20;
pub const MAX_PAGE_SIZE: usize = 262_144;

pub type PageId = u32;
pub type PageHandle = Arc<PageId>;

pub struct Directory {

    file: File,
    page_list: PageList,
}


enum CreateOrOpen {
    Created,
    Opened,
}


fn create_or_open_page_file(path: &Path, len: usize) -> io::Result<(CreateOrOpen, File)> {
    match std::fs::File::create_new(path) {
        Ok(file) => {
            file.set_len(len as u64)?;
            Ok((true, file))
        },
        Err(io_err) if io_err.kind() == io::ErrorKind::AlreadyExists => {
                    let file = std::fs::File::open(path)?;
                    let metadata = file.metadata()?;
                    if len != metadata.len() {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "file length does not match expected length"));
                    }
                    Ok((false, file))
                },
                err @ io::Error(_) => {
                    return Err(err)
                }
        }
}


impl Directory {
    pub fn create_or_open(path: &Path, len: usize) -> io::Result<()> {
        let num_pages = len / PAGE_SIZE;
        if num_pages < MIN_NUM_PAGES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file length is too small"));
        }
        let (create_or_open, file) = create_or_open_page_file(path, len)?;
        match create_or_open {
            CreateOrOpen::Created => Directory::create(file, len),
            CreateOrOpen::Opened => Directory::open(file, len),
        }
    }

    fn create(file: File, num_pages: usize) -> io::Result<()> {
        todo!();
    }

    fn open(file: File, len: usize) -> io::Result<()> {
        todo!()
    }

    pub fn write_page(&mut self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
        todo!();
    }

    pub fn gc(&mut self) -> io::Result<()> {
        let mut freed_pages: Vec<PageId> = Vec::new();
        for &page_id in &self.written_page_ids {
            let page_handle = &self.page_handles[page_id as usize];
            if Arc::strong_count(&page_handle) == 1 {
                freed_pages.push(page_id);
            }
        }
        freed_pages.sort_unstable();
        self.available_page_ids.extend_from_slice(&freed_pages);
        self.write_new_page_order()?;
        Ok(())
    }

    fn write_new_page_order(&mut self,) -> io::Result<()> {
        todo!()

    }
}
