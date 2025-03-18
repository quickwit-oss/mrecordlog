use std::io::{self, Read, Seek, Write};
use std::ops::Range;

use super::header::HeaderInfo;
use super::{FileLike, PageHandle, PageId, PageListWriter, RefCount, PAGE_SIZE};

pub struct PageList {
    num_pages: u32,
    header_len: usize,

    // This is the order in which the pages should be read,
    // were we to read the log entirely again.
    //
    // It includes both the written page and the page that are
    // still free.
    page_ids: Vec<PageId>,
    page_ids_idx: Vec<u32>,

    cursor: u64,

    page_refcounts: Vec<RefCount>,

    epoch: u64,
}

fn compute_slot_len(num_pages: u32) -> usize {
    8 + // epoch
    3 * num_pages as usize + // page ids encoded over 3 bytes each
    4 // checksum
}

const CHUNK_NUM_PAGES: usize = 4_096;
const CHUNK_BYTES: usize = CHUNK_NUM_PAGES * 3;

fn write_slot(
    epoch: u64,
    page_ids: &[PageId],
    wrt: &mut impl Write,
) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK_BYTES);
    let mut hasher = crc32fast::Hasher::new();

    let epoch_bytes = epoch.to_le_bytes();
    hasher.update(&epoch_bytes);
    wrt.write_all(&epoch_bytes)?;
    for page_chunk in page_ids.chunks(CHUNK_NUM_PAGES) {
        buf.clear();
        for &page_id in page_chunk {
            let page_id_bytes = page_id.to_le_bytes();
            buf.extend_from_slice(&page_id_bytes[..3]);
        }
        if buf.is_empty() {
            break;
        }
        hasher.update(&buf);
        wrt.write_all(&buf)?;
        if buf.len() < CHUNK_BYTES {
            break;
        }
    }
    let digest = hasher.finalize();
    wrt.write_all(&digest.to_le_bytes())?;
    Ok(())
}

struct PageListSerialized {
    epoch: u64,
    page_ids: Vec<PageId>,
}

fn read_slot(num_pages: u32, read: &mut dyn Read) -> io::Result<Option<PageListSerialized>> {
    let mut page_ids = Vec::with_capacity(num_pages as usize);
    let mut hasher = crc32fast::Hasher::new();

    let mut epoch_bytes = [0u8; 8];
    read.read_exact(&mut epoch_bytes)?;
    hasher.update(&epoch_bytes);

    let epoch = u64::from_le_bytes(epoch_bytes.try_into().unwrap());

    let mut buff = vec![0u8; CHUNK_BYTES];
    while page_ids.len() < num_pages as usize {
        let num_pages_to_read = (num_pages as usize - page_ids.len()).min(CHUNK_NUM_PAGES);
        let num_bytes_to_read = num_pages_to_read * 3;
        let chunk_buf = &mut buff[..num_bytes_to_read];
        read.read_exact(chunk_buf)?;
        hasher.update(chunk_buf);
        page_ids.extend(chunk_buf.chunks_exact(3).map(|page_id| {
            let mut page_id_bytes = [0u8; 4];
            page_id_bytes[0..3].copy_from_slice(page_id);
            u32::from_le_bytes(page_id_bytes)
        }));
    }
    assert_eq!(page_ids.len(), num_pages as usize);
    let expected_digest = hasher.finalize();

    let mut digest_bytes = [0u8; 4];
    read.read_exact(&mut digest_bytes[..])?;
    let digest = u32::from_le_bytes(digest_bytes);

    if expected_digest != digest {
        return Ok(None);
    }

    Ok(Some(PageListSerialized { epoch, page_ids }))
}

fn next_multiple_of(offset: u64, mult: u64) -> u64 {
    let k = offset.div_ceil(mult);
    k * mult
}

impl PageList {
    pub fn new(header_info: HeaderInfo) -> Self {
        PageList {
            num_pages: header_info.num_pages,
            header_len: header_info.header_len,
            page_ids: (0..header_info.num_pages as PageId).collect(),
            page_ids_idx: (0..header_info.num_pages as PageId).collect(),
            cursor: 0u64,

            page_refcounts: vec![0u32; header_info.num_pages as usize],
            epoch: 0u64,
        }
    }

    pub fn load(header_info: HeaderInfo, file: &mut impl FileLike) -> io::Result<PageList> {
        let mut page_list = PageList::new(header_info);

        let first_slot_range = page_list.compute_slot_range(false);
        file.seek(io::SeekFrom::Start(first_slot_range.start as u64))?;
        let first_page_list = read_slot(page_list.num_pages, file)?;

        let second_slot_range = page_list.compute_slot_range(true);
        file.seek(io::SeekFrom::Start(second_slot_range.start as u64))?;
        let second_page_list = read_slot(page_list.num_pages, file)?;

        let PageListSerialized { epoch, page_ids } = match (first_page_list, second_page_list) {
            (None, None) => {
                let error_msg = "page list is corrupted";
                return Err(io::Error::new(io::ErrorKind::InvalidData, error_msg));
            }
            (None, Some(page_list)) | (Some(page_list), None) => page_list,
            (Some(first_page_list), Some(second_page_list)) => {
                if first_page_list.epoch > second_page_list.epoch {
                    first_page_list
                } else {
                    second_page_list
                }
            }
        };

        page_list.page_ids = page_ids;
        page_list.rebuild_page_index();

        page_list.cursor = 0u64;

        page_list.epoch = epoch;


        Ok(page_list)
    }

    pub(crate) fn initialize_file(
        header_info: HeaderInfo,
        file: &mut impl FileLike,
    ) -> io::Result<PageList> {
        let num_pages = header_info.num_pages;
        let mut page_list = PageList::new(header_info);
        let body_start = page_list.first_page_offset();
        let file_num_bytes = body_start + num_pages as u64 * PAGE_SIZE as u64;
        file.set_len(file_num_bytes)?;
        page_list.gc(file)?;
        Ok(page_list)
    }

    fn rebuild_page_index(&mut self) {
        self.page_ids_idx.resize(self.page_ids.len(), 0u32);
        for (idx, &page_id) in self.page_ids.iter().enumerate() {
            self.page_ids_idx[page_id as usize] = idx as u32;
        }
    }

    fn inc_ref_count(&mut self, page_handle: PageHandle, count: u32) {
        assert_ne!(page_handle.num_pages, 0);
        let selected_page_ids = if page_handle.num_pages > 1 {
            &[page_handle.start_page_id]
        } else {
            let page_ord = self.page_ids_idx[page_handle.start_page_id as usize] as usize;
            let page_ids = &self.page_ids[page_ord..][..page_handle.num_pages as usize];
            assert_eq!(page_ids[0], page_handle.start_page_id);
            page_ids
        };
        for &page_id in selected_page_ids {
            self.page_refcounts[page_id as usize] += count;
        }
    }

    fn dec_ref_count(&mut self, page_handle: PageHandle) {
        assert_ne!(page_handle.num_pages, 0);
        let selected_page_ids = if page_handle.num_pages > 1 {
            &[page_handle.start_page_id]
        } else {
            let page_ord = self.page_ids_idx[page_handle.start_page_id as usize] as usize;
            let page_ids = &self.page_ids[page_ord..][..page_handle.num_pages as usize];
            assert_eq!(page_ids[0], page_handle.start_page_id);
            page_ids
        };
        for &page_id in selected_page_ids {
            self.page_refcounts[page_id as usize] -= 1;
        }
    }

    pub fn writer(&mut self) -> PageListWriter {
        todo!()
    }

    fn compute_slot_range(&self, epoch_parity: bool) -> Range<usize> {
        let slot_len = compute_slot_len(self.num_pages);
        let start_offset = if epoch_parity {
            self.header_len + slot_len
        } else {
            self.header_len
        };
        start_offset..start_offset + slot_len
    }

    pub fn first_page_offset(&self) -> u64 {
        let end_of_page_list = self.compute_slot_range(true).end as u64;
        next_multiple_of(end_of_page_list, PAGE_SIZE as u64)
    }

    pub fn gc(&mut self, file: &mut impl FileLike) -> io::Result<()> {
        self.epoch += 1;
        let written_page_id = self.cursor.div_ceil(PAGE_SIZE as u64) as usize;
        let mut freed_pages = self.page_ids.drain(written_page_id..).collect();
        drain_filter(&mut self.page_ids, |page_id| {
            self.page_refcounts[page_id as usize] == 0u32
        }, &mut freed_pages);
        freed_pages.sort_unstable();
        self.page_ids.extend_from_slice(&freed_pages);
        let slot_range = self.compute_slot_range(self.epoch % 2 != 0);
        file.seek(io::SeekFrom::Start(slot_range.start as u64))?;
        write_slot(self.epoch, &self.page_ids, file)?;
        file.flush()?;
        file.fsyncdata()?;
        self.rebuild_page_index();
        Ok(())
    }
}

fn drain_filter(els: &mut Vec<PageId>, filter: impl Fn(PageId)->bool, output: &mut Vec<PageId>) {
    let mut wrt_cursor = 0;
    for read_cursor in 0..els.len() {
        let page = els[read_cursor];
        if filter(page) {
            output.push(page);
        } else {
            els[wrt_cursor] = page;
            wrt_cursor += 1;
        }
    }
    els.truncate(wrt_cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mockfile::MockFile;

    #[test]
    fn test_drain_filter() {
        let mut output = vec![6u32, 4];
        let mut els: Vec<PageId> = vec![1u32, 3, 2, 8, 5, 7];
        drain_filter(&mut els, |page_id| page_id % 2 == 0, &mut output);
        assert_eq!(&els[..], &[1, 3, 5, 7]);
        assert_eq!(&output[..], &[6, 4, 2, 8]);
    }

    #[track_caller]
    fn test_serialize_page_list_slot_aux(epoch: u64, page_ids: &[u32]) {
        let mut buf = Vec::new();
        write_slot(epoch, page_ids, &mut buf).unwrap();
        assert_eq!(
            compute_slot_len(page_ids.len() as u32),
            buf.len(),
            "serialized len does not match expectation"
        );
        let mut arr = &buf[..];
        let page_list_serialized_opt = read_slot(page_ids.len() as u32, &mut arr).unwrap();
        assert!(arr.is_empty(), "data was not entirely read");
        let page_list_serialized = page_list_serialized_opt.unwrap();
        assert_eq!(page_list_serialized.epoch, epoch, "epoch does not match");
        assert_eq!(
            &page_list_serialized.page_ids, page_ids,
            "page ids do not match"
        );
    }

    #[test]
    fn test_serialize_page_list_slot() {
        test_serialize_page_list_slot_aux(3u64, &[]);
        test_serialize_page_list_slot_aux(3u64, &[3u32, 9u32]);
    }

    proptest::proptest! {
        #[test]
        fn test_proptest_serialize_page_list_slot(
            epoch in proptest::num::u64::ANY,
            page_ids in proptest::collection::vec(0..1u32 << 24, 0..CHUNK_NUM_PAGES * 3)
        ) {
            test_serialize_page_list_slot_aux(epoch, &page_ids);
        }
    }

    #[test]
    fn test_page_list_gc_simple() {
        let mut file = MockFile::new();
        let header_info = HeaderInfo {
            header_len: 3,
            num_pages: 100u32,
        };
        {
            let page_list = PageList::initialize_file(header_info, &mut file).unwrap();
            assert_eq!(page_list.epoch, 1);
            assert_eq!(file.len() % PAGE_SIZE, 0);
            assert_eq!(page_list.first_page_offset() % PAGE_SIZE as u64, 0u64);
        }
        {
            let mut page_list = PageList::load(header_info, &mut file).unwrap();
            assert_eq!(page_list.epoch, 1);
            assert_eq!(page_list.num_pages, header_info.num_pages);
            for i in 0..header_info.num_pages as usize {
                assert_eq!(page_list.page_ids[i], i as PageId);
            }
            page_list.gc(&mut file).unwrap();
        }
        {
            let page_list = PageList::load(header_info, &mut file).unwrap();
            assert_eq!(page_list.epoch, 2);
            assert_eq!(page_list.num_pages, header_info.num_pages);
            for i in 0..header_info.num_pages as usize {
                assert_eq!(page_list.page_ids[i], i as PageId);
            }
        }
    }
}
