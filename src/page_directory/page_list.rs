use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::atomic::Ordering;

use super::header::{self, HeaderInfo};
use super::{PageId, PAGE_SIZE};
use crate::page_directory::page_refcounts::PageRegistry;
use crate::page_directory::PageRangeRef;
use crate::{FileLike, FileLikeWrite};

pub struct PageList {
    header_info: HeaderInfo,
    page_registry: rclite::Arc<PageRegistry>,
    epoch: u64,
    first_page_offset: u64,
}

const CHUNK_NUM_PAGES: usize = 4_096;
const CHUNK_BYTES: usize = CHUNK_NUM_PAGES * 3;

fn write_slot(epoch: u64, page_ids: &[PageId], wrt: &mut impl Write) -> io::Result<()> {
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

#[derive(Debug)]
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
    fn new(header_info: HeaderInfo) -> Self {
        let page_registry = PageRegistry::new(header_info.num_pages as usize);
        let end_of_page_list = header_info.compute_slot_range(true).end as u64;
        let first_page_offset = next_multiple_of(end_of_page_list, PAGE_SIZE as u64);
        PageList {
            header_info,
            page_registry: rclite::Arc::new(page_registry),
            epoch: 0u64,
            first_page_offset,
        }
    }

    pub(crate) fn initialize_page_file<W: FileLike>(
        num_pages: usize,
        file: &mut W,
    ) -> io::Result<PageList> {
        file.seek(io::SeekFrom::Start(0))?;
        let header_info = header::serialize_header(num_pages as u32, file)?;
        let mut page_list = PageList::new(header_info);
        let file_num_bytes = page_list.first_page_offset + (num_pages as u64 * PAGE_SIZE as u64);
        file.set_len(file_num_bytes)?;
        // We call gc once in order to initialize the page_list.
        page_list.gc(0u64, file)?;
        Ok(page_list)
    }

    fn compute_slot_range(&self, epoch_parity: bool) -> Range<usize> {
        self.header_info.compute_slot_range(epoch_parity)
    }

    pub fn load(header_info: HeaderInfo, file: &mut impl FileLike) -> io::Result<PageList> {
        let mut page_list = PageList::new(header_info);

        let first_slot_range = page_list.compute_slot_range(false);
        file.seek(io::SeekFrom::Start(first_slot_range.start as u64))?;
        let first_page_list = read_slot(page_list.num_pages(), file)?;

        let second_slot_range = page_list.compute_slot_range(true);
        file.seek(io::SeekFrom::Start(second_slot_range.start as u64))?;
        let second_page_list = read_slot(page_list.num_pages(), file)?;

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

        page_list.page_registry = rclite::Arc::new(PageRegistry::with_page_ids(page_ids));

        page_list.epoch = epoch;

        Ok(page_list)
    }

    pub fn new_page_range_ref(&self, start_page_id: u32) -> PageRangeRef {
        self.new_page_range_ref_with_num_pages(start_page_id, 1u32)
    }

    #[inline(always)]
    pub fn new_page_range_ref_with_num_pages(
        &self,
        start_page_id: u32,
        num_pages: u32,
    ) -> PageRangeRef {
        let mut page_range_ref = PageRangeRef {
            start_page_id: start_page_id,
            num_pages: 0,
            page_registry: self.page_registry.clone(),
        };
        for _ in 0..num_pages {
            page_range_ref.add_page();
        }
        page_range_ref
    }

    #[inline(always)]
    pub fn num_pages(&self) -> u32 {
        self.header_info.num_pages
    }

    #[inline(always)]
    pub fn page_id(&self, page_ord: usize) -> Option<PageId> {
        self.page_registry
            .page_index
            .load()
            .ord_to_id
            .get(page_ord)
            .copied()
    }

    #[inline(always)]
    pub fn page_start_offset(&self, page_ord: usize) -> Option<u64> {
        let page_id = self.page_id(page_ord)?;
        Some(self.first_page_offset + page_id as u64 * PAGE_SIZE as u64)
    }

    // Runs a gc operation.
    // `cursor` is the next offset of the byte to be written.
    //
    // This method returns the number that have been freed.
    pub fn gc(&mut self, cursor: u64, file: &mut impl FileLikeWrite) -> io::Result<usize> {
        self.epoch += 1;
        let written_page_id = cursor.div_ceil(PAGE_SIZE as u64) as usize;
        let mut page_ids = self.page_registry.page_index.load().ord_to_id.clone();
        let mut free_pages: Vec<PageId> = page_ids.drain(written_page_id..).collect();
        let num_free_pages_when_gc_started = free_pages.len();
        drain_filter(
            &mut page_ids,
            |page_id| {
                self.page_registry.ref_counts[page_id as usize].load(Ordering::Acquire) == 0u32
            },
            &mut free_pages,
        );
        let num_freed_pages = free_pages.len() - num_free_pages_when_gc_started;
        free_pages.sort_unstable();
        page_ids.extend_from_slice(&free_pages);
        let slot_range = self.compute_slot_range(self.epoch % 2 != 0);
        file.seek(io::SeekFrom::Start(slot_range.start as u64))?;
        write_slot(self.epoch, &page_ids, file)?;
        file.flush()?;
        file.fsyncdata()?;
        self.page_registry.update_page_ids_after_gc(page_ids);
        Ok(num_freed_pages)
    }
}

fn drain_filter(els: &mut Vec<PageId>, filter: impl Fn(PageId) -> bool, output: &mut Vec<PageId>) {
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
            super::super::compute_slot_len(page_ids.len() as u32),
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
            header_len: 16,
            num_pages: 10u32,
        };
        {
            let page_list = PageList::initialize_page_file(10, &mut file).unwrap();
            assert_eq!(page_list.epoch, 1);
            assert_eq!(page_list.header_info.header_len, 16);
            // The header AND the slots should fit in a single page.
            assert_eq!(PAGE_SIZE * 11, file.len());
            assert_eq!(page_list.first_page_offset % PAGE_SIZE as u64, 0u64);
        }
        {
            let mut page_list = PageList::load(header_info, &mut file).unwrap();

            assert_eq!(
                &page_list.page_registry.page_index.load().ord_to_id,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            );
            assert_eq!(page_list.epoch, 1);
            assert_eq!(page_list.header_info.num_pages, header_info.num_pages);
            for i in 0..header_info.num_pages as usize {
                assert_eq!(
                    page_list.page_registry.page_index.load().ord_to_id[i],
                    i as PageId
                );
            }
            assert_eq!(page_list.epoch, 1);
            page_list.gc(0, &mut file).unwrap();
            assert_eq!(page_list.epoch, 2);
        }
        {
            let mut page_list = PageList::load(header_info, &mut file).unwrap();
            let page_registry = page_list.page_registry.clone();
            assert_eq!(page_list.epoch, 2);
            assert_eq!(page_list.header_info.num_pages, header_info.num_pages);
            for i in 0..header_info.num_pages as usize {
                assert_eq!(page_registry.page_index.load().ord_to_id[i], i as PageId);
            }

            let ref_page0 = page_list.new_page_range_ref(0);
            let _ref_page1 = page_list.new_page_range_ref(1);
            let _ref_page2 = page_list.new_page_range_ref(2);
            let _ref_page3 = page_list.new_page_range_ref(3);
            drop(ref_page0);
            let ref_page4 = page_list.new_page_range_ref(4);
            let _ref_page5 = page_list.new_page_range_ref(5);
            drop(ref_page4);
            page_list.gc(6u64 * PAGE_SIZE as u64, &mut file).unwrap();
            assert_eq!(
                &page_registry.page_index.load().ord_to_id,
                &[1, 2, 3, 5, 0, 4, 6, 7, 8, 9]
            );
        }
    }

    #[test]
    fn test_page_list_gc_clone() {
        let mut file = MockFile::new();
        let mut page_list = PageList::initialize_page_file(5, &mut file).unwrap();
        let page_registry = page_list.page_registry.clone();

        let ref_page0 = page_list.new_page_range_ref(0);
        let ref_page1 = page_list.new_page_range_ref(1);
        let _ref_page2 = page_list.new_page_range_ref(2);
        let _ref_page1_clone = ref_page1.clone();
        drop(ref_page0);
        drop(ref_page1);

        page_list.gc(3u64 * PAGE_SIZE as u64, &mut file).unwrap();
        assert_eq!(
            &page_registry.page_index.load().ord_to_id,
            &[1, 2, 0, 3, 4,]
        );
    }
}
