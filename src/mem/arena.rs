/// 256 KiB.
#[cfg(not(test))]
pub const PAGE_SIZE: usize = 1 << 18;

#[cfg(test)]
pub const PAGE_SIZE: usize = 7;

// TODO make it an array once we get a way to allocate array on the heap.
pub type Page = Box<[u8]>;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct PageId(usize);

/// An arena of fixed sized pages.
#[derive(Default)]
pub struct Arena {
    /// We use an array to store the list of pages.
    /// It can be seen as an efficient map from page id to pages.
    ///
    /// This map's len can-only grows. Its size is therefore the maximum number of pages
    /// that was ever allocated. One page being 1MB long, this is not a problem.
    ///
    /// If a page is not allocated, the corresponding entry is `None`.
    pages: Vec<Option<Page>>,
    /// `free_slots` slots keeps track of the pages that are not allocated.
    free_slots: Vec<PageId>,
    /// `free_page_ids` keeps track of the allocated pages that are
    /// available.
    free_page_ids: Vec<PageId>,
    /// Arena stats used to track how many pages should be freed.
    stats: ArenaStats,
}

// The idea here is that we keep track of the maximum number of pages used through time.
// To identify if it is worth deallocating pages, we look at the maximum number of pages
// in use in the last few minutes minutes.
//
// We then allow ourselves to free memory down to this value.
// Tracking exactly the maximum number of pages in use in the last 5 minutes is unnecessarily
// complicated.
//
// For instance, we could run an extra task or thread.
//
// Instead, we just run a routine whenever someone interacts with the GC.
// This routine only checks time 1 out of 256 calls.
//
// Pitfall: If pages are requests way less often than 256 times per minutes,
// this arena may take way too much time to release its memory.
struct ArenaStats {
    max_num_used_pages_former: usize,
    max_num_used_pages_current: usize,
}

impl Default for ArenaStats {
    fn default() -> ArenaStats {
        ArenaStats {
            max_num_used_pages_former: 0,
            max_num_used_pages_current: 0,
        }
    }
}

impl ArenaStats {
    /// This method happens when we are changing time window.
    fn roll(&mut self) {
        self.max_num_used_pages_former = self.max_num_used_pages_current;
        self.max_num_used_pages_current = 0;
    }

    /// Records the number of used pages, and returns an estimation of the maximum number of pages
    /// in the last 5 minutes.
    pub fn record_num_used_page(&mut self, num_used_pages: usize) -> usize {
        self.max_num_used_pages_current = self.max_num_used_pages_current.max(num_used_pages);
        self.max_num_used_pages_former
            .max(self.max_num_used_pages_current)
    }
}

impl Arena {
    /// Returns an allocated page id.
    pub fn acquire_page(&mut self) -> PageId {
        if let Some(page_id) = self.free_page_ids.pop() {
            assert!(self.pages[page_id.0].is_some());
            return page_id;
        }
        let page: Page = vec![0u8; PAGE_SIZE].into_boxed_slice();
        if let Some(free_slot) = self.free_slots.pop() {
            let slot = &mut self.pages[free_slot.0];
            assert!(slot.is_none());
            *slot = Some(page);
            free_slot
        } else {
            let new_page_id = self.pages.len();
            self.pages.push(Some(page));
            PageId(new_page_id)
        }
    }

    #[inline]
    pub fn page(&self, page_id: PageId) -> &[u8] {
        self.pages[page_id.0].as_ref().unwrap()
    }

    #[inline]
    pub fn page_mut(&mut self, page_id: PageId) -> &mut [u8] {
        self.pages[page_id.0].as_mut().unwrap()
    }

    pub fn release_page(&mut self, page_id: PageId) {
        self.free_page_ids.push(page_id);
        assert!(self.pages[page_id.0].is_some());
        self.gc();
    }

    /// Clients are expected roll the stats regularly.
    pub fn roll_and_gc(&mut self) {
        self.stats.roll();
        self.gc();
    }

    /// `gc` releases memory by some of the free pages.
    fn gc(&mut self) {
        let num_used_pages = self.num_used_pages();
        let max_used_num_pages_in_last_5_min = self.stats.record_num_used_page(num_used_pages);
        // We pick a target slightly higher than the maximum number of pages to avoid needless
        // allocations when we are experience a general increase
        // in memory usage.
        let target_num_pages = max_used_num_pages_in_last_5_min + 10;
        let num_pages_to_free = self.num_allocated_pages().saturating_sub(target_num_pages);
        let num_free_pages_to_keep = self.free_page_ids.len() - num_pages_to_free;
        for free_page_id in self.free_page_ids.drain(num_free_pages_to_keep..) {
            self.pages[free_page_id.0] = None;
            self.free_slots.push(free_page_id);
        }
    }

    /// Returns the number of pages that are allocated
    /// (regardless of whether they are in use or not).
    pub fn num_allocated_pages(&self) -> usize {
        self.pages.len() - self.free_slots.len()
    }

    /// Returns the number of pages that are allocated AND actually used.
    pub fn num_used_pages(&self) -> usize {
        self.pages.len() - self.free_slots.len() - self.free_page_ids.len()
    }

    pub fn unused_capacity(&self) -> usize {
        self.free_page_ids.len() * PAGE_SIZE
    }
}

#[cfg(test)]
mod tests {
    use mock_instant::MockClock;

    use super::*;

    #[test]
    fn test_arena_simple() {
        let mut arena = Arena::default();
        assert_eq!(arena.num_allocated_pages(), 0);
        assert_eq!(arena.acquire_page(), PageId(0));
        assert_eq!(arena.acquire_page(), PageId(1));
        arena.release_page(PageId(0));
        assert_eq!(arena.acquire_page(), PageId(0));
    }

    #[test]
    fn test_arena_gc() {
        let mut arena = Arena::default();
        assert_eq!(arena.num_allocated_pages(), 0);
        assert_eq!(arena.acquire_page(), PageId(0));
        assert_eq!(arena.acquire_page(), PageId(1));
        arena.release_page(PageId(1));
        assert_eq!(arena.num_allocated_pages(), 2);
        arena.gc();
        assert_eq!(arena.num_allocated_pages(), 2);
        assert_eq!(arena.acquire_page(), PageId(1));
        assert_eq!(arena.num_allocated_pages(), 2);
    }

    #[test]
    fn test_arena_stats() {
        let mut arena_stats = ArenaStats::default();
        for _ in 0..256 {
            assert_eq!(arena_stats.record_num_used_page(10), 10);
        }
        MockClock::advance(WINDOW.mul_f32(1.1f32));
        for _ in 0..256 {
            assert_eq!(arena_stats.record_num_used_page(1), 10);
        }
        MockClock::advance(WINDOW.mul_f32(1.1f32));
        for _ in 0..256 {
            arena_stats.record_num_used_page(1);
        }
        assert_eq!(arena_stats.record_num_used_page(1), 1);
        assert_eq!(arena_stats.record_num_used_page(2), 2);
        for _ in 0..256 {
            assert_eq!(arena_stats.record_num_used_page(1), 2);
        }
        MockClock::advance(WINDOW);
        for _ in 0..256 {
            assert_eq!(arena_stats.record_num_used_page(1), 2);
        }
    }
}
