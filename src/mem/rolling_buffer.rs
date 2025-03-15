use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds, RangeTo};

#[derive(Default)]
pub struct RollingBuffer {
    buffer: VecDeque<u8>,
}

impl RollingBuffer {
    pub fn new() -> Self {
        RollingBuffer {
            buffer: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.buffer.shrink_to_fit();
    }

    // Removes all of the data up to pos byte excluded (meaning pos is kept).
    //
    // If we notice that the rolling buffer was very large, this function may shrink
    // it.
    pub fn truncate_head(&mut self, first_pos_to_keep: RangeTo<usize>) {
        let target_capacity = self.len() * 9 / 8;
        self.buffer.drain(first_pos_to_keep);
        // In order to avoid leaking memory we shrink the buffer.
        // The last maximum length (= the length before drain)
        // is a good estimate of what we will need in the future.
        //
        // We add 1/8 to that in order to make sure that we don't end up
        // shrinking  / allocating for small variations.

        if self.buffer.capacity() > target_capacity {
            self.buffer.shrink_to(target_capacity);
        }
    }

    pub fn extend(&mut self, slice: &[u8]) {
        self.buffer.extend(slice.iter().copied());
    }

    pub fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<[u8]> {
        let start = match bounds.start_bound() {
            Bound::Included(pos) => *pos,
            Bound::Excluded(pos) => pos + 1,
            Bound::Unbounded => 0,
        };

        let end = match bounds.end_bound() {
            Bound::Included(pos) => pos + 1,
            Bound::Excluded(pos) => *pos,
            Bound::Unbounded => self.len(),
        };

        let (left_part_of_queue, right_part_of_queue) = self.buffer.as_slices();

        if end < left_part_of_queue.len() {
            Cow::Borrowed(&left_part_of_queue[start..end])
        } else if start >= left_part_of_queue.len() {
            let start = start - left_part_of_queue.len();
            let end = end - left_part_of_queue.len();

            Cow::Borrowed(&right_part_of_queue[start..end])
        } else {
            // VecDeque is a rolling buffer. As a result, we do not have
            // access to a continuous buffer.
            //
            // Here the requested slice cross the boundary and we need to allocate and copy the data
            // in a new buffer.
            let mut res = Vec::with_capacity(end - start);
            res.extend_from_slice(&left_part_of_queue[start..]);
            let end = end - left_part_of_queue.len();
            res.extend_from_slice(&right_part_of_queue[..end]);

            Cow::Owned(res)
        }
    }
}
