use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};

#[derive(Debug)]
pub struct RollingBuffer {
    buffer: VecDeque<Vec<u8>>,
	start_pos: usize,
	end_pos: usize,
	end_segment: usize
}

impl Default for RollingBuffer {
    fn default() -> Self {
        RollingBuffer::new()
    }
}

impl RollingBuffer {
    pub fn new() -> Self {
        RollingBuffer {
            buffer: VecDeque::new(),
			start_pos: 0,
			end_segment: 0,
			end_pos: 0,
        }
    }

    pub fn len(&self) -> usize {
        dbg!(self);
        if self.buffer.is_empty() {
            return 0;
        }
        self.buffer.iter()
            .take(self.end_segment)
            .map(|buffer| buffer.len())
            .sum::<usize>() + self.end_pos - self.start_pos 
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.buffer.shrink_to_fit();
    }

    pub fn drain_start(&mut self, mut pos: usize) {
		// TODO reduce size somehow
        while pos > 0 {
            let first_buffer = self.buffer.pop_front().unwrap();
            let left = first_buffer.len() - self.start_pos;
            if left < pos {
                pos -= left;
                self.start_pos = 0;
                assert!(self.end_segment > 0);
                self.end_segment -= 1;
                self.buffer.push_back(first_buffer);
            } else {
                self.start_pos += pos;
                self.buffer.push_front(first_buffer);
                break;
            }
        }
    }

    pub fn extend(&mut self, mut slice: &[u8]) {
        let mut buffers = self.buffer.iter_mut().skip(self.end_segment)
            .zip(std::iter::once(self.end_pos).chain(std::iter::repeat(0)));
        while !slice.is_empty() {
            if let Some((buffer, start_pos)) = buffers.next() {
                let len = (buffer.len() - start_pos).min(slice.len());
                buffer[start_pos..][..len].copy_from_slice(&slice[..len]);
                slice = &slice[len..];
                self.start_pos = start_pos + len;
                if !slice.is_empty() {
                    self.end_segment += 1;   
                }
            } else {
                // TODO find a better heuristic for the buffer len
                let mut next_buffer = Vec::new();
                next_buffer.resize(slice.len(), 0);
                
                next_buffer[..slice.len()].copy_from_slice(&slice);
                self.buffer.push_back(next_buffer);
                self.end_segment += 1;
                break;
            }
        }
    }

    pub fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<[u8]> {
        let mut start = match bounds.start_bound() {
            Bound::Included(pos) => *pos,
            Bound::Excluded(pos) => pos + 1,
            Bound::Unbounded => 0,
        };

        let end = match bounds.end_bound() {
            Bound::Included(pos) => pos + 1,
            Bound::Excluded(pos) => *pos,
            Bound::Unbounded => self.len(),
        };

        let mut len = end - start;
        assert!(len <= self.len());

        let mut buffers = self.buffer.iter()
            .zip(std::iter::once(self.start_pos).chain(std::iter::repeat(0)));

        while let Some((buffer, buffer_start)) = buffers.next() {
            let buffer_len = buffer.len() - buffer_start;
            if buffer_len < start {
                start -= buffer_len;
                continue;
            }
            // start is in the current block, but is the end too?
            if start + len < buffer_len {
                let result = &buffer[buffer_start + start..][..len]; 
                return Cow::Borrowed(result);
            } else {
                let mut result = Vec::with_capacity(len);
                
                result.extend_from_slice(&buffer[buffer_start + start..]);
                len -= buffer_len - start;
                while len > 0 {
                    let (buffer, _) = buffers.next().unwrap();
                    let copy_len = buffer.len().min(len);
                    result.extend_from_slice(&buffer[..copy_len]);
                    len -= copy_len;
                }

                return Cow::Owned(result);
            }
        }
        panic!("requested range after end of data");


        /*
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
        }*/
    }
}

