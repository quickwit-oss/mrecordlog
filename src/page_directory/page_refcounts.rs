use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::page_directory::PageId;

pub struct PageIndex {
    // This is a page_ord -> page_id mapping.
    pub ord_to_id: Vec<u32>,
    // This is a page_id -> page_ord mapping.
    pub id_to_ord: Vec<u32>,
}

impl PageIndex {
    pub fn new(num_pages: usize) -> PageIndex {
        let ord_to_id: Vec<u32> = (0..num_pages as u32).collect();
        let id_to_ord = ord_to_id.clone();
        PageIndex {
            ord_to_id,
            id_to_ord,
        }
    }

    pub fn from_page_ids(ord_to_id: Vec<u32>) -> PageIndex {
        let mut page_index = PageIndex {
            ord_to_id,
            id_to_ord: Vec::new(),
        };
        page_index.rebuild_page_index();
        page_index
    }

    fn rebuild_page_index(&mut self) {
        self.id_to_ord.resize(self.ord_to_id.len(), 0u32);
        for (ord, &page_id) in self.ord_to_id.iter().enumerate() {
            self.id_to_ord[page_id as usize] = ord as u32;
        }
    }
}

pub struct PageRegistry {
    pub ref_counts: Box<[AtomicU32]>,
    pub page_index: ArcSwap<PageIndex>,
}

impl PageRegistry {
    pub fn with_page_ids(ord_to_id: Vec<PageId>) -> PageRegistry {
        let page_index = PageIndex::from_page_ids(ord_to_id);
        PageRegistry::with_page_index(page_index)
    }

    pub fn update_page_ids_after_gc(&self, ord_to_id: Vec<PageId>) {
        let page_index = PageIndex::from_page_ids(ord_to_id);
        self.page_index.store(Arc::new(page_index));
    }

    pub fn with_page_index(page_index: PageIndex) -> PageRegistry {
        let num_pages = page_index.ord_to_id.len();
        let ref_counts: Vec<AtomicU32> = std::iter::repeat_with(AtomicU32::default)
            .take(num_pages)
            .collect();
        PageRegistry {
            ref_counts: ref_counts.into_boxed_slice(),
            page_index: ArcSwap::new(Arc::new(page_index)),
        }
    }

    pub fn new(num_pages: usize) -> PageRegistry {
        let page_index = PageIndex::new(num_pages);
        Self::with_page_index(page_index)
    }
}

pub struct PageRangeRef {
    pub start_page_id: u32,
    pub num_pages: u16,
    pub page_registry: rclite::Arc<PageRegistry>,
}

impl Eq for PageRangeRef {}

impl PartialEq for PageRangeRef {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.start_page_id == other.start_page_id && self.num_pages == other.num_pages
    }
}

impl PageRangeRef {
    // Adds a page to the reference.
    pub fn add_page(&mut self) {
        let page_id = if self.num_pages == 0u16 {
            self.start_page_id
        } else {
            let page_index = self.page_registry.page_index.load();
            let start_page_ord = page_index.id_to_ord[self.start_page_id as usize];
            let page_ord = start_page_ord + self.num_pages as u32;
            page_index.ord_to_id[page_ord as usize]
        };
        self.num_pages += 1;
        self.page_registry.ref_counts[page_id as usize].fetch_add(1, Ordering::Release);
    }
}

impl Clone for PageRangeRef {
    fn clone(&self) -> PageRangeRef {
        let mut page_range_ref = PageRangeRef {
            start_page_id: self.start_page_id,
            num_pages: 0,
            page_registry: self.page_registry.clone(),
        };
        for _ in 0..self.num_pages {
            page_range_ref.add_page();
        }
        page_range_ref
    }
}

impl Drop for PageRangeRef {
    fn drop(&mut self) {
        let page_index = self.page_registry.page_index.load();
        let page_ids = if self.num_pages == 0u16 {
            &[self.start_page_id]
        } else {
            let start_page_ord = page_index.id_to_ord[self.start_page_id as usize];
            &page_index.ord_to_id[start_page_ord as usize..][..self.num_pages as usize]
        };
        for &page_id in page_ids {
            self.page_registry.ref_counts[page_id as usize].fetch_sub(1, Ordering::AcqRel);
        }
    }
}
