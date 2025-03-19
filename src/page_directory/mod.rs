use std::fs::File;
use std::io;
use std::path::Path;

mod header;
mod page_list;
mod writer;

use page_list::PageList;
pub use writer::PageListWriter;

pub const PAGE_SIZE: usize = 1 << 16; // 65,536 bytes
pub const MIN_NUM_PAGES: usize = 20;
pub const MAX_PAGE_SIZE: usize = 262_144;

pub type PageId = u32;
pub type RefCount = u32;

#[derive(Copy, Clone, Debug)]
pub struct PageHandle {
    pub start_page_id: u32,
    pub num_pages: u32,
}

pub struct Directory {
    file: File,
    page_list: PageList,
}

enum CreateOrOpen {
    Created,
    Opened,
}

fn create_or_open_page_file(path: &Path, len: u64) -> io::Result<(CreateOrOpen, File)> {
    match std::fs::File::create_new(path) {
        Ok(file) => {
            file.set_len(len as u64)?;
            Ok((CreateOrOpen::Created, file))
        }
        Err(io_err) if io_err.kind() == io::ErrorKind::AlreadyExists => {
            let file = std::fs::File::open(path)?;
            let metadata = file.metadata()?;
            if len != metadata.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file length does not match expected length",
                ));
            }
            Ok((CreateOrOpen::Opened, file))
        }
        Err(err) => Err(err),
    }
}

pub trait FileLike: io::Read + io::Write + io::Seek {
    fn fsyncdata(&mut self) -> io::Result<()>;
    fn set_len(&mut self, num_bytes: u64) -> io::Result<()>;
}

impl FileLike for File {
    fn fsyncdata(&mut self) -> io::Result<()> {
        self.sync_data()
    }

    fn set_len(&mut self, num_bytes: u64) -> io::Result<()> {
        File::set_len(self, num_bytes)
    }
}

fn initialize_page_file<W: FileLike>(num_pages: usize, file: &mut W) -> io::Result<PageList> {
    file.seek(io::SeekFrom::Start(0))?;
    let header_info = header::serialize_header(num_pages as u32, file)?;
    let mut page_list = PageList::new(header_info);
    let body_start = page_list.first_page_offset();
    let file_num_bytes = body_start + (num_pages as u64 * PAGE_SIZE as u64);
    file.set_len(file_num_bytes)?;
    page_list.gc(file)?;
    Ok(page_list)
}

impl Directory {
    pub fn create_or_open(path: &Path, len: u64) -> io::Result<Directory> {
        let num_pages = len as usize / PAGE_SIZE;
        if num_pages < MIN_NUM_PAGES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file length is too small",
            ));
        }
        let (create_or_open, file) = create_or_open_page_file(path, len)?;
        match create_or_open {
            CreateOrOpen::Created => Directory::create(num_pages, file),
            CreateOrOpen::Opened => Directory::open(num_pages, file),
        }
    }

    fn create(num_pages: usize, mut file: File) -> io::Result<Directory> {
        let page_list = initialize_page_file(num_pages, &mut file)?;
        let directory = Directory { file, page_list };
        Ok(directory)
    }

    fn open(num_pages: usize, mut file: File) -> io::Result<Directory> {
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
