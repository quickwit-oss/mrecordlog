use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds, RangeTo};

/// Size of the chunks the buffer is made of.
///
/// Truncating the head of the buffer frees whole chunks: it never moves the remaining bytes.
/// A contiguous `VecDeque<u8>` had to be shrunk after a truncation to give memory back, which
/// copied every remaining byte (gigabytes for a busy queue) into a fresh allocation.
const CHUNK_NUM_BYTES: usize = 1 << 20;

/// A byte buffer appended at the tail and truncated at the head.
///
/// Bytes are stored in chunks of `CHUNK_NUM_BYTES`. Every chunk but the last one is full, so the
/// byte at (absolute) offset `head_offset + pos` is in chunk `(head_offset + pos) /
/// CHUNK_NUM_BYTES`.
#[derive(Default)]
pub struct RollingBuffer {
    chunks: VecDeque<Vec<u8>>,
    /// Number of bytes of the first chunk that have been truncated.
    head_offset: usize,
    len: usize,
}

impl RollingBuffer {
    pub fn new() -> Self {
        RollingBuffer::default()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        // Chunks are allocated with exactly `CHUNK_NUM_BYTES` of capacity and never grow.
        self.chunks.len() * CHUNK_NUM_BYTES
    }

    pub fn clear(&mut self) {
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.head_offset = 0;
        self.len = 0;
    }

    // Removes all of the data up to pos byte excluded (meaning pos is kept).
    //
    // The chunks that only hold removed bytes are freed.
    pub fn truncate_head(&mut self, first_pos_to_keep: RangeTo<usize>) {
        let num_bytes_to_remove = first_pos_to_keep.end;
        assert!(
            num_bytes_to_remove <= self.len,
            "truncate position {num_bytes_to_remove} is out of bounds ({})",
            self.len
        );
        if num_bytes_to_remove == self.len {
            self.clear();
            return;
        }
        self.len -= num_bytes_to_remove;
        self.head_offset += num_bytes_to_remove;
        let num_chunks_to_remove = self.head_offset / CHUNK_NUM_BYTES;
        self.chunks.drain(..num_chunks_to_remove);
        self.head_offset %= CHUNK_NUM_BYTES;
        // In order to avoid leaking memory, we shrink the chunk deque (a few bytes per chunk)
        // once it is mostly empty.
        if self.chunks.capacity() > 2 * self.chunks.len() + 16 {
            self.chunks.shrink_to(self.chunks.len() * 9 / 8 + 8);
        }
    }

    pub fn extend(&mut self, mut slice: &[u8]) {
        self.len += slice.len();
        while !slice.is_empty() {
            let chunk = match self.chunks.back_mut() {
                Some(chunk) if chunk.len() < CHUNK_NUM_BYTES => chunk,
                _ => {
                    self.chunks.push_back(Vec::with_capacity(CHUNK_NUM_BYTES));
                    self.chunks.back_mut().unwrap()
                }
            };
            let num_bytes = (CHUNK_NUM_BYTES - chunk.len()).min(slice.len());
            let (head, tail) = slice.split_at(num_bytes);
            chunk.extend_from_slice(head);
            slice = tail;
        }
    }

    pub fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<'_, [u8]> {
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
        assert!(
            start <= end && end <= self.len,
            "range {start}..{end} is out of bounds ({})",
            self.len
        );
        if start == end {
            return Cow::Borrowed(&[]);
        }
        let abs_start = self.head_offset + start;
        let abs_end = self.head_offset + end;
        let first_chunk = abs_start / CHUNK_NUM_BYTES;
        let last_chunk = (abs_end - 1) / CHUNK_NUM_BYTES;
        if first_chunk == last_chunk {
            let chunk_start = first_chunk * CHUNK_NUM_BYTES;
            return Cow::Borrowed(
                &self.chunks[first_chunk][abs_start - chunk_start..abs_end - chunk_start],
            );
        }
        // The requested range spans several chunks: we need to allocate and copy the data in a
        // new buffer.
        let mut res = Vec::with_capacity(end - start);
        for chunk_idx in first_chunk..=last_chunk {
            let chunk_start = chunk_idx * CHUNK_NUM_BYTES;
            let from = abs_start.max(chunk_start) - chunk_start;
            let to = abs_end.min(chunk_start + CHUNK_NUM_BYTES) - chunk_start;
            res.extend_from_slice(&self.chunks[chunk_idx][from..to]);
        }
        Cow::Owned(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks the rolling buffer against a plain `Vec<u8>`.
    struct Model {
        buffer: RollingBuffer,
        expected: Vec<u8>,
    }

    impl Model {
        fn check(&self) {
            assert_eq!(self.buffer.len(), self.expected.len());
            assert_eq!(&self.buffer.get_range(..)[..], &self.expected[..]);
            let len = self.expected.len();
            for (start, end) in [
                (0, len),
                (len / 3, len / 2),
                (len / 2, len),
                (len.saturating_sub(1), len),
                (len / 4, len / 4),
            ] {
                assert_eq!(
                    &self.buffer.get_range(start..end)[..],
                    &self.expected[start..end]
                );
            }
            // Chunks are only kept for unread data.
            assert!(self.buffer.capacity() <= len + 2 * CHUNK_NUM_BYTES);
        }

        fn extend(&mut self, num_bytes: usize, seed: u8) {
            let payload: Vec<u8> = (0..num_bytes)
                .map(|idx| (idx as u8).wrapping_mul(31).wrapping_add(seed))
                .collect();
            self.buffer.extend(&payload);
            self.expected.extend_from_slice(&payload);
            self.check();
        }

        fn truncate_head(&mut self, num_bytes: usize) {
            self.buffer.truncate_head(..num_bytes);
            self.expected.drain(..num_bytes);
            self.check();
        }
    }

    #[test]
    fn test_rolling_buffer_spanning_chunks() {
        let mut model = Model {
            buffer: RollingBuffer::new(),
            expected: Vec::new(),
        };
        model.check();
        model.extend(10, 1);
        model.extend(CHUNK_NUM_BYTES - 10, 2);
        assert_eq!(model.buffer.chunks.len(), 1);
        model.extend(1, 3);
        assert_eq!(model.buffer.chunks.len(), 2);
        model.extend(3 * CHUNK_NUM_BYTES + 17, 4);
        model.truncate_head(5);
        model.truncate_head(CHUNK_NUM_BYTES);
        assert_eq!(model.buffer.chunks.len(), 4);
        model.truncate_head(2 * CHUNK_NUM_BYTES - 5);
        assert_eq!(model.buffer.chunks.len(), 2);
        assert_eq!(model.buffer.head_offset, 0);
        model.extend(CHUNK_NUM_BYTES / 2, 5);
        let len = model.expected.len();
        model.truncate_head(len);
        assert_eq!(model.buffer.capacity(), 0);
        model.extend(7, 6);
    }

    #[test]
    fn test_rolling_buffer_random_operations() {
        let mut model = Model {
            buffer: RollingBuffer::new(),
            expected: Vec::new(),
        };
        // Simple xorshift: deterministic, no dependency.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for step in 0..200 {
            if next() % 3 == 0 {
                let len = model.expected.len();
                let num_bytes = if len == 0 {
                    0
                } else {
                    (next() as usize) % (len + 1)
                };
                model.truncate_head(num_bytes);
            } else {
                let num_bytes = (next() as usize) % (CHUNK_NUM_BYTES * 3 / 2);
                model.extend(num_bytes, step as u8);
            }
        }
    }
}
