use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};

use crate::error::AppendError;
use crate::rolling::FileNumber;

#[cfg(not(test))]
const BUFFER_SLICE_LEN: usize = 64 * 1024 * 1024; // 64 MiB
// use lower size in test to help proptests
#[cfg(test)]
const BUFFER_SLICE_LEN: usize = 16;

enum RollingBuffer {
    Short(ShortBuffer),
    Long(LongBuffer),
}

impl Default for RollingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl RollingBuffer {
    fn new() -> Self {
        RollingBuffer::Short(ShortBuffer::new())
    }

    fn len(&self) -> usize {
        match self {
            RollingBuffer::Short(short) => short.len(),
            RollingBuffer::Long(long) => long.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            RollingBuffer::Short(short) => short.clear(),
            RollingBuffer::Long(_) => {
                *self = RollingBuffer::new();
            }
        }
    }

    fn truncate_start(&mut self, pos: usize) {
        match self {
            RollingBuffer::Short(short) => short.truncate_start(pos),
            RollingBuffer::Long(long) => {
                if long.len() < BUFFER_SLICE_LEN {
                    let buffer = long.get_range(pos..).into_owned();
                    *self = RollingBuffer::Short(ShortBuffer::new_with_content(buffer));
                } else {
                    long.truncate_start(pos)
                }
            }
        }
    }

    fn extend(&mut self, slice: &[u8]) {
        match self {
            RollingBuffer::Short(short) => {
                if short.len() + slice.len() < 2 * BUFFER_SLICE_LEN {
                    short.extend(slice)
                } else {
                    let mut buffer = LongBuffer::new_with_content(&*short.get_range(..));
                    buffer.extend(slice);
                    *self = RollingBuffer::Long(buffer);
                }
            }
            RollingBuffer::Long(long) => long.extend(slice),
        }
    }

    fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<[u8]> {
        match self {
            RollingBuffer::Short(short) => short.get_range(bounds),
            RollingBuffer::Long(long) => long.get_range(bounds),
        }
    }
}

fn new_slice() -> Box<[u8; BUFFER_SLICE_LEN]> {
    // on opt-level>=1 this does not allocate on the stack
    // TODO this could use MaybeUninit to skip a memset. This would require
    // checking more thoroughly any read we get for out of bound access.
    Box::new([0; BUFFER_SLICE_LEN])
}

struct LongBuffer {
    // invariant: this must always have at least one element
    slices: VecDeque<Box<[u8; BUFFER_SLICE_LEN]>>,
    start_offset: usize,
    len: usize,
}

impl LongBuffer {
    fn new() -> Self {
        let mut slices = VecDeque::new();
        slices.push_back(new_slice());
        LongBuffer {
            slices,
            start_offset: 0,
            len: 0,
        }
    }

    fn new_with_content(mut content: &[u8]) -> Self {
        let total_len = content.len();
        let mut slices = VecDeque::new();

        // while we can fill whole slice, fill whole slice
        while content.len() > BUFFER_SLICE_LEN {
            let mut slice = new_slice();
            slice.copy_from_slice(&content[..BUFFER_SLICE_LEN]);
            slices.push_back(slice);
            content = &content[BUFFER_SLICE_LEN..];
        }

        // add a final non full slice
        let mut slice = new_slice();
        slice[..content.len()].copy_from_slice(content);
        slices.push_back(slice);

        LongBuffer {
            slices,
            start_offset: 0,
            len: total_len,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    /// convert a position to a slice and an offset.
    ///
    /// There are 2 representation for position that lands exactly at the end of a block:
    /// (block_id, BUFFER_SLICE_LEN) and (block_id + 1, 0). Depending on use, one can be
    /// more usefull than the other. In particular, when interested by the start of a range,
    /// the later is more appropriate, but when interested by the end of a range, the former
    /// is better. The `is_end_range` parameter allows to choose which one is prefered here.
    fn to_slice_and_offset(&self, pos: usize, is_end_slice: bool) -> (usize, usize) {
        let pos = pos + self.start_offset;
        let slice = pos / BUFFER_SLICE_LEN;
        let offset = pos % BUFFER_SLICE_LEN;
        if (offset == 0 && slice > 0) && is_end_slice {
            (slice - 1, BUFFER_SLICE_LEN)
        } else {
            (slice, offset)
        }
    }

    fn extend(&mut self, mut content: &[u8]) {
        let (slice, offset) = self.to_slice_and_offset(self.len(), true);

        // happy path: we can just copy to the end
        if BUFFER_SLICE_LEN - offset > content.len() {
            self.slices[slice][offset..][..content.len()].copy_from_slice(content);
            self.len += content.len();
            return;
        }

        // last slice is too full or content is too large. Start by feeling the end of last slice
        let left = BUFFER_SLICE_LEN - offset;
        self.slices[slice][offset..].copy_from_slice(&content[..left]);
        content = &content[left..];

        // then fill as many whole slices as needed.
        while content.len() > BUFFER_SLICE_LEN {
            let mut slice = new_slice();
            slice.copy_from_slice(&content[..BUFFER_SLICE_LEN]);
            self.slices.push_back(slice);
            content = &content[BUFFER_SLICE_LEN..];
        }

        // and fill a final non-full slice
        let mut slice = new_slice();
        slice[..content.len()].copy_from_slice(content);
        self.slices.push_back(slice);
        self.len += content.len();
    }

    fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<[u8]> {
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

        let (start_slice, start_offset) = self.to_slice_and_offset(start, false);
        let (end_slice, end_offset) = self.to_slice_and_offset(end, true);

        // start and end in same slice, just subslice
        if start_slice == end_slice {
            return Cow::Borrowed(&self.slices[start_slice][start_offset..end_offset]);
        }

        let mut result_buffer = Vec::with_capacity(end - start);

        // partially copy from start slice
        result_buffer.extend_from_slice(&self.slices[start_slice][start_offset..]);

        // then fill any full slice in between
        for slice_id in start_slice + 1..end_slice {
            result_buffer.extend_from_slice(&*self.slices[slice_id]);
        }

        // and partially copy from last slice
        result_buffer.extend_from_slice(&self.slices[end_slice][..end_offset]);
        debug_assert_eq!(end - start, result_buffer.len());
        Cow::Owned(result_buffer)
    }

    fn truncate_start(&mut self, pos: usize) {
        // deallocate unused slices
        let num_slice_used = self.to_slice_and_offset(self.len, true).0 + 1;
        let target_num_slice = num_slice_used * 9 / 8;
        while self.slices.len() > target_num_slice {
            self.slices.pop_back();
        }

        let (slice, offset) = self.to_slice_and_offset(pos, false);
        // pop as many slice as needed from the start of the queue to make `slice` go first
        for _ in 0..slice {
            let slice = self.slices.pop_front().unwrap();
            self.slices.push_back(slice);
        }
        // and set the new start_offset and len
        self.start_offset = offset;
        self.len -= pos;
    }
}

struct ShortBuffer {
    buffer: VecDeque<u8>,
}

impl ShortBuffer {
    fn new() -> Self {
        ShortBuffer {
            buffer: VecDeque::new(),
        }
    }

    fn new_with_content(content: Vec<u8>) -> Self {
        ShortBuffer {
            buffer: content.into(),
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.buffer.shrink_to_fit();
    }

    fn truncate_start(&mut self, pos: usize) {
        let target_capacity = self.len() * 9 / 8;
        self.buffer.drain(..pos);
        // In order to avoid leaking memory we shrink the buffer.
        // The last maximum length (= the length before drain)
        // is a good estimate of what we will need in the future.
        //
        // We add 1/8 to that in order to make sure that we don't end up
        // shrinking  / allocating for small variations.

        self.buffer.shrink_to(target_capacity);
    }

    fn extend(&mut self, slice: &[u8]) {
        self.buffer.extend(slice.iter().copied());
    }

    fn get_range(&self, bounds: impl RangeBounds<usize>) -> Cow<[u8]> {
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

#[derive(Clone)]
struct RecordMeta {
    start_offset: usize,
    // in a vec of RecordMeta, this field should be set only on the last record
    // which relate to that File.
    file_number: Option<FileNumber>,
}

#[derive(Default)]
pub struct MemQueue {
    // Concatenated records
    concatenated_records: RollingBuffer,
    start_position: u64,
    record_metas: Vec<RecordMeta>,
}

impl MemQueue {
    pub fn new() -> Self {
        MemQueue::with_next_position(0)
    }

    pub fn with_next_position(next_position: u64) -> Self {
        MemQueue {
            concatenated_records: RollingBuffer::new(),
            start_position: next_position,
            record_metas: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_metas.is_empty()
    }

    /// Returns what should be the next position.
    pub fn next_position(&self) -> u64 {
        self.start_position + self.record_metas.len() as u64
    }

    /// Appends a new record at a given position.
    /// Returns an error if the record was not added.
    ///
    /// AppendError if the record is strangely in the past or is too much in the future.
    pub fn append_record(
        &mut self,
        file_number: &FileNumber,
        target_position: u64,
        payload: &[u8],
    ) -> Result<(), AppendError> {
        let next_position = self.next_position();
        if next_position != 0 && next_position != target_position {
            if target_position > next_position {
                return Err(AppendError::Future);
            } else {
                return Err(AppendError::Past);
            }
        }
        if self.start_position == 0u64 && self.record_metas.is_empty() {
            self.start_position = target_position;
        }

        let file_number = if let Some(record_meta) = self.record_metas.last_mut() {
            if record_meta.file_number.as_ref() == Some(file_number) {
                record_meta.file_number.take().unwrap()
            } else {
                file_number.clone()
            }
        } else {
            file_number.clone()
        };

        let record_meta = RecordMeta {
            start_offset: self.concatenated_records.len(),
            file_number: Some(file_number),
        };
        self.record_metas.push(record_meta);
        self.concatenated_records.extend(payload);
        Ok(())
    }

    fn position_to_idx(&self, position: u64) -> Option<usize> {
        if self.start_position > position {
            return Some(0);
        }
        let idx = (position - self.start_position) as usize;
        if idx >= self.record_metas.len() {
            return None;
        }
        Some(idx)
    }

    pub fn range<R>(&self, range: R) -> impl Iterator<Item = (u64, Cow<[u8]>)> + '_
    where R: RangeBounds<u64> + 'static {
        let start_idx: usize = match range.start_bound() {
            Bound::Included(&start_from) => self
                .position_to_idx(start_from)
                .unwrap_or(self.record_metas.len()),
            Bound::Excluded(&start_from) => {
                // if the excluded start bound is before our start, we range over everything
                if self.start_position > start_from {
                    0
                } else {
                    self.position_to_idx(start_from)
                        .unwrap_or(self.record_metas.len())
                        + 1
                }
            }
            Bound::Unbounded => 0,
        };
        (start_idx..self.record_metas.len())
            .take_while(move |&idx| {
                let position = self.start_position + idx as u64;
                range.contains(&position)
            })
            .map(move |idx| {
                let position = self.start_position + idx as u64;
                let start_offset = self.record_metas[idx].start_offset;
                if let Some(next_record_meta) = self.record_metas.get(idx + 1) {
                    let end_offset = next_record_meta.start_offset;
                    (
                        position,
                        self.concatenated_records
                            .get_range(start_offset..end_offset),
                    )
                } else {
                    (
                        position,
                        self.concatenated_records.get_range(start_offset..),
                    )
                }
            })
    }

    /// Removes all records coming before position,
    /// and including the record at "position".
    pub fn truncate(&mut self, truncate_up_to_pos: u64) {
        if self.start_position > truncate_up_to_pos {
            return;
        }
        let first_record_to_keep =
            if let Some(first_record_to_keep) = self.position_to_idx(truncate_up_to_pos + 1) {
                first_record_to_keep
            } else {
                // clear the queue entirely
                self.start_position = self.next_position();
                self.concatenated_records.clear();
                self.record_metas.clear();
                return;
            };
        let start_offset_to_keep: usize = self.record_metas[first_record_to_keep].start_offset;
        self.record_metas.drain(..first_record_to_keep);
        for record_meta in &mut self.record_metas {
            record_meta.start_offset -= start_offset_to_keep;
        }
        self.concatenated_records
            .truncate_start(start_offset_to_keep);
        self.start_position += first_record_to_keep as u64;
    }

    pub fn size(&self) -> usize {
        self.concatenated_records.len()
            + self.record_metas.len() * std::mem::size_of::<RecordMeta>()
    }
}
