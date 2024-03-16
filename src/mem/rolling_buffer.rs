use std::ops::{Bound, Range, RangeBounds};

use bytes::Buf;

use crate::mem::arena::{Arena, PageId, PAGE_SIZE};

/// `RollingBuffer` stores a short slice of an seemingly infinite buffer with offset spawning from
/// [0..u64::MAX).
///
/// It is possible to append bytes to the buffer with `.extend_from_slice(..)`,
/// or drop the bytes up to a given offset with `truncate_up_to()`.
#[derive(Default)]
pub struct RollingBuffer {
    page_ids: Vec<PageId>,
    range: Range<usize>,
}

fn num_pages_required(range: Range<usize>) -> usize {
    let Range { start, end } = range;
    if start >= end {
        // This is an important non-trivial edge case.
        // If the range is empty, we retain no pages.
        return 0;
    }
    let first_page = start / PAGE_SIZE;
    let last_page = end.saturating_sub(1) / PAGE_SIZE;
    last_page - first_page + 1
}

impl RollingBuffer {
    pub fn new() -> Self {
        RollingBuffer {
            page_ids: Vec::new(),
            range: 0..0,
        }
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn end_offset(&self) -> usize {
        self.range.end
    }

    pub fn capacity(&self) -> usize {
        self.page_ids.len() * PAGE_SIZE
    }

    pub fn clear(&mut self, arena: &mut Arena) {
        self.truncate_up_to_excluded(self.range.end, arena);
    }

    fn check_invariants(&self) {
        debug_assert_eq!(num_pages_required(self.range.clone()), self.page_ids.len());
    }

    /// Truncate the buffer, all of the bytes striclty below `new_start``.
    pub fn truncate_up_to_excluded(&mut self, new_start: usize, arena: &mut Arena) {
        assert!(new_start <= self.range.end);
        let num_pages = num_pages_required(new_start..self.range.end);
        assert!(num_pages <= self.page_ids.len());
        let num_pages_to_drop = self.page_ids.len() - num_pages;
        self.range.start = new_start;
        if num_pages_to_drop > 0 {
            for page_id in self.page_ids.drain(..(self.page_ids.len() - num_pages)) {
                arena.release_page(page_id);
            }
        }
        self.check_invariants();
    }

    /// Returns a chunk of available memory, either remaining from the last page,
    /// or acquires a new page from the arena.
    fn get_page_with_room<'a>(&mut self, arena: &'a mut Arena) -> &'a mut [u8] {
        let start_offset = self.range.end % PAGE_SIZE;
        if start_offset == 0 || self.page_ids.is_empty() {
            // The page is entirely used, or there are no pages at all.
            // Let's allocate a new page.
            let new_page_id = arena.acquire_page();
            self.page_ids.push(new_page_id);
        }
        let page_id = self.page_ids.last().copied().unwrap();
        let page = arena.page_mut(page_id);
        &mut page[start_offset..]
    }

    pub fn extend_from_slice(&mut self, mut slice: &[u8], arena: &mut Arena) {
        while !slice.is_empty() {
            let page = self.get_page_with_room(arena);
            let len = page.len().min(slice.len());
            let (head, queue) = slice.split_at(len);
            page[..len].copy_from_slice(head);
            slice = queue;
            self.range.end += len;
        }
        self.check_invariants();
    }

    fn get_range_aux<'slf, 'a: 'slf>(
        &'slf self,
        range: Range<usize>,
        arena: &'a Arena,
    ) -> PagesBuf<'slf> {
        let Range { start, end } = range;
        assert!(start >= self.range.start);
        assert!(end <= self.range.end);
        if end <= start {
            return PagesBuf {
                arena,
                start_offset: 0,
                page_ids: &[],
                remaining_len: 0,
            };
        }
        let start_page_id = start / PAGE_SIZE;
        let start_inner_page_id = self.range.start / PAGE_SIZE;
        let skip_pages = start_page_id - start_inner_page_id;
        let start_offset = start % PAGE_SIZE;
        PagesBuf {
            arena,
            start_offset,
            page_ids: &self.page_ids[skip_pages..],
            remaining_len: end - start,
        }
    }

    pub fn get_range<'slf, 'a: 'slf, R>(
        &'slf self,
        range_bounds: R,
        arena: &'a Arena,
    ) -> PagesBuf<'slf>
    where
        R: RangeBounds<usize> + 'static,
    {
        let start = match range_bounds.start_bound() {
            Bound::Included(pos) => *pos,
            Bound::Excluded(pos) => pos + 1,
            Bound::Unbounded => 0,
        };
        let end = match range_bounds.end_bound() {
            Bound::Included(pos) => pos + 1,
            Bound::Excluded(pos) => *pos,
            Bound::Unbounded => self.range.end,
        };
        self.get_range_aux(start..end, arena)
    }
}

#[derive(Clone, Copy)]
pub struct PagesBuf<'a> {
    arena: &'a Arena,
    page_ids: &'a [PageId],
    start_offset: usize,
    remaining_len: usize,
}

impl<'a> std::fmt::Debug for PagesBuf<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.to_cow().fmt(f)
    }
}

impl<'a> PagesBuf<'a> {
    pub fn to_cow(mut self) -> std::borrow::Cow<'a, [u8]> {
        if self.page_ids.len() <= 1 {
            let chunk = self.chunk_with_lifetime();
            return std::borrow::Cow::Borrowed(chunk);
        }
        let mut buf = Vec::with_capacity(self.remaining_len);
        while self.has_remaining() {
            let chunk = self.chunk_with_lifetime();
            buf.extend_from_slice(chunk);
            self.advance(chunk.len());
        }
        std::borrow::Cow::Owned(buf)
    }

    // Contrary to Buf::chunk, this method returns a slice with a `'a` lifetime (so it can outlive 'self).
    fn chunk_with_lifetime(&self) -> &'a [u8] {
        let Some(first_page_id) = self.page_ids.first().copied() else {
            return &[];
        };
        let current_page = &self.arena.page(first_page_id)[self.start_offset..];
        if current_page.len() > self.remaining_len {
            &current_page[..self.remaining_len]
        } else {
            current_page
        }
    }
}

impl<'a> Buf for PagesBuf<'a> {
    fn remaining(&self) -> usize {
        self.remaining_len
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.chunk_with_lifetime()
    }

    fn advance(&mut self, mut cnt: usize) {
        cnt = cnt.min(self.remaining_len);
        while cnt > 0 {
            if self.page_ids.is_empty() {
                return;
            }
            let page = self.chunk();
            if page.len() > cnt {
                self.start_offset += cnt;
                self.remaining_len -= cnt;
            } else {
                cnt -= page.len();
                self.remaining_len -= page.len();
                self.start_offset = 0;
                self.page_ids = &self.page_ids[1..];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_vec<B: Buf>(mut buf: B) -> Vec<u8> {
        let mut output = Vec::with_capacity(buf.remaining());
        while buf.has_remaining() {
            let chunk = buf.chunk();
            output.extend_from_slice(chunk);
            buf.advance(chunk.len());
        }
        output
    }

    #[test]
    fn test_rolling_buffer() {
        let mut arena = Arena::default();
        let text = b"hello happy tax payer";
        for new_start in 0..text.len() {
            let mut rolling_buffer = RollingBuffer::new();
            rolling_buffer.extend_from_slice(&b"hello"[..], &mut arena);
            rolling_buffer.extend_from_slice(&b" happy"[..], &mut arena);
            rolling_buffer.extend_from_slice(&b" tax payer"[..], &mut arena);
            rolling_buffer.truncate_up_to_excluded(new_start, &mut arena);
            for start in new_start..text.len() {
                for end in start..text.len() {
                    let bytes: Vec<u8> = to_vec(rolling_buffer.get_range(start..end, &arena));
                    assert_eq!(&text[start..end], &bytes[..]);
                }
            }
        }
    }

    #[test]
    fn test_rolling_buffer_clear() {
        let mut arena = Arena::default();
        let mut rolling_buffer = RollingBuffer::new();
        rolling_buffer.clear(&mut arena);
        assert_eq!(rolling_buffer.len(), 0);
        rolling_buffer.extend_from_slice(&b"abcdefghik"[..], &mut arena);
        assert_eq!(rolling_buffer.len(), 10);
        assert_eq!(arena.num_used_pages(), 2);
        rolling_buffer.clear(&mut arena);
        assert_eq!(rolling_buffer.len(), 0);
        assert_eq!(arena.num_used_pages(), 0);
    }

    #[test]
    fn test_num_pages_required() {
        assert_eq!(num_pages_required(0..0), 0);
        assert_eq!(num_pages_required(2..2), 0);
        assert_eq!(num_pages_required(2..1), 0);
        assert_eq!(num_pages_required(0..1), 1);
        assert_eq!(num_pages_required(0..PAGE_SIZE), 1);
        assert_eq!(num_pages_required(0..PAGE_SIZE + 1), 2);
        assert_eq!(num_pages_required(0..2 * PAGE_SIZE), 2);
        assert_eq!(num_pages_required(0..2 * PAGE_SIZE + 1), 3);
        assert_eq!(num_pages_required(PAGE_SIZE - 1..2 * PAGE_SIZE), 2);
        assert_eq!(num_pages_required(PAGE_SIZE - 1..2 * PAGE_SIZE + 1), 3);
        assert_eq!(num_pages_required(PAGE_SIZE..2 * PAGE_SIZE), 1);
        assert_eq!(num_pages_required(PAGE_SIZE..2 * PAGE_SIZE + 1), 2);
    }
}
