use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};

use crate::error::AppendError;
use crate::rolling::FileNumber;

#[derive(Default)]
struct RollingBuffer {
    buffer: VecDeque<u8>,
}

impl RollingBuffer {
    fn new() -> Self {
        RollingBuffer {
            buffer: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.buffer.shrink_to_fit();
    }

    async fn drain_start(&mut self, pos: usize) {
        let target_capacity = self.len() * 9 / 8;
        self.buffer.drain(..pos);
        // In order to avoid leaking memory we shrink the buffer.
        // The last maximum length (= the length before drain)
        // is a good estimate of what we will need in the future.
        //
        // We add 1/8 to that in order to make sure that we don't end up
        // shrinking  / allocating for small variations.

        if self.buffer.capacity() > target_capacity {
            let mut buffer = std::mem::take(&mut self.buffer);
            self.buffer = tokio::task::spawn_blocking(move || {
                buffer.shrink_to(target_capacity);
                buffer
            })
            .await
            .unwrap();
        }
    }

    async fn extend(&mut self, slice: &[u8]) {
        self.reserve(slice.len()).await;
        self.buffer.extend(slice.iter().copied());
    }

    /// This function is used to make sure we don't block an async executor on a long
    /// resize
    async fn reserve(&mut self, capacity: usize) {
        if self.buffer.capacity() - self.buffer.len() < capacity {
            // we need to realocate, which can be slow. Do that on a
            // blocking task
            let mut buffer = std::mem::take(&mut self.buffer);
            self.buffer = tokio::task::spawn_blocking(move || {
                buffer.reserve(capacity);
                buffer
            })
            .await
            .unwrap();
        }
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
    pub async fn append_record(
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
        self.concatenated_records.extend(payload).await;
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
    pub async fn truncate(&mut self, truncate_up_to_pos: u64) {
        if self.start_position > truncate_up_to_pos {
            return;
        }
        let first_record_to_keep =
            if let Some(first_record_to_keep) = self.position_to_idx(truncate_up_to_pos + 1) {
                first_record_to_keep
            } else {
                // clear the queue entirely
                // TODO it might make more sense to jump to truncate_up_to_pos instead of just
                // going on step forward.
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
            .drain_start(start_offset_to_keep)
            .await;
        self.start_position += first_record_to_keep as u64;
    }

    pub fn size(&self) -> usize {
        self.concatenated_records.len()
            + self.record_metas.len() * std::mem::size_of::<RecordMeta>()
    }
}
