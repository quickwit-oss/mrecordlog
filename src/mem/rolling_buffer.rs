use std::ops::{Bound, Range, RangeBounds};

use bytes::Buf;

use crate::mem::arena::{Arena, PageId, PAGE_SIZE};

/// A rolling buffer stores a drainable buffer.
///
/// On
#[derive(Default)]
pub struct RollingBuffer {
    page_ids: Vec<PageId>,
    range: Range<usize>,
}

fn num_pages_required(range: Range<usize>) -> usize {
    let Range { start, end } = range;
    let first_page = start / PAGE_SIZE;
    let last_page = (end - 1) / PAGE_SIZE;
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

    pub fn capacity(&self) -> usize {
        self.page_ids.len() * PAGE_SIZE
    }

    pub fn clear(&mut self, arena: &mut Arena) {
        for page_id in self.page_ids.drain(..) {
            arena.release_page(page_id);
        }
        self.range = 0..0;
    }

    /// Truncate the buffer, dropping the first `truncate_len` bytes.
    pub fn truncate_to(&mut self, truncate_len: usize, arena: &mut Arena) {
        if truncate_len >= self.range.len() {
            self.clear(arena);
            return;
        }
        let num_pages = num_pages_required((self.range.start + truncate_len)..self.range.end);
        assert!(num_pages <= self.page_ids.len());
        let num_pages_to_drop = self.page_ids.len() - num_pages;
        let new_start = self.range.start + truncate_len - num_pages_to_drop * PAGE_SIZE;
        let new_end = self.range.end - num_pages_to_drop * PAGE_SIZE;
        self.range = new_start..new_end;
        if num_pages_to_drop > 0 {
            for page_id in self.page_ids.drain(..(self.page_ids.len() - num_pages)) {
                arena.release_page(page_id);
            }
        }
    }

    fn get_page_with_room<'a>(&mut self, arena: &'a mut Arena) -> &'a mut [u8] {
        let start_offset = self.range.end % PAGE_SIZE;
        if start_offset == 0 {
            // The page is entirely used.
            let new_page_id = arena.get_page_id();
            self.page_ids.push(new_page_id);
            let page = arena.page_mut(new_page_id);
            &mut page[..]
        } else {
            let page_id = self.page_ids.last().copied().unwrap();
            let page = arena.page_mut(page_id);
            &mut page[start_offset..]
        }
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
    }

    fn get_range_buf_aux<'slf, 'a: 'slf>(
        &'slf self,
        range: Range<usize>,
        arena: &'a Arena,
    ) -> impl Buf + 'slf {
        let start = (self.range.start + range.start).min(self.range.end);
        let end = (self.range.start + range.end).clamp(start, self.range.end);
        let len = end - start;
        let skip_pages = start / PAGE_SIZE;
        let start_offset = start % PAGE_SIZE;
        PagesBuf {
            arena,
            start_offset,
            page_ids: &self.page_ids[skip_pages..],
            remaining_len: len,
        }
        .take(len)
    }

    pub fn get_range_buf<'slf, 'a: 'slf, R>(
        &'slf self,
        range_bounds: R,
        arena: &'a Arena,
    ) -> impl Buf + 'slf
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
            Bound::Unbounded => self.len(),
        };
        self.get_range_buf_aux(start..end, arena)
    }

    pub fn get_range<'slf, 'a: 'slf, R>(
        &'slf self,
        range_bounds: R,
        arena: &'a Arena,
    ) -> impl Iterator<Item = &'a [u8]> + 'slf
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
            Bound::Unbounded => self.len(),
        };
        self.get_range_aux(start..end, arena)
    }

    pub fn get_range_aux<'slf, 'a: 'slf>(
        &'slf self,
        range: Range<usize>,
        arena: &'a Arena,
    ) -> impl Iterator<Item = &'a [u8]> + 'slf {
        let start = (self.range.start + range.start).min(self.range.end);
        let end = (self.range.start + range.end).clamp(start, self.range.end);
        let mut remaining_len = end - start;
        let skip_pages = start / PAGE_SIZE;
        let mut start_in_page = start % PAGE_SIZE;
        self.page_ids[skip_pages..]
            .iter()
            .copied()
            .map(move |page_id| {
                let page = &arena.page(page_id)[start_in_page..];
                start_in_page = 0;
                let page_len = page.len().min(remaining_len);
                remaining_len -= page_len;
                &page[..page_len]
            })
            .take_while(|page| !page.is_empty())
    }
}

struct PagesBuf<'a> {
    arena: &'a Arena,
    page_ids: &'a [PageId],
    start_offset: usize,
    remaining_len: usize,
}

impl<'a> Buf for PagesBuf<'a> {
    fn remaining(&self) -> usize {
        self.remaining_len
    }

    fn chunk(&self) -> &[u8] {
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

    fn advance(&mut self, mut cnt: usize) {
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
        for truncate_len in 0..text.len() {
            let expected = &text[truncate_len..];
            let mut rolling_buffer = RollingBuffer::new();
            rolling_buffer.extend_from_slice(&b"hello"[..], &mut arena);
            rolling_buffer.extend_from_slice(&b" happy"[..], &mut arena);
            rolling_buffer.extend_from_slice(&b" tax payer"[..], &mut arena);
            rolling_buffer.truncate_to(truncate_len, &mut arena);
            for start in 0..expected.len() {
                for end in start..expected.len() {
                    let bytes: Vec<u8> = to_vec(rolling_buffer.get_range_buf(start..end, &arena));
                    assert_eq!(&expected[start..end], &bytes[..]);
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
}
