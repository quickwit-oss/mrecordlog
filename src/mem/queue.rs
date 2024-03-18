use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};

use crate::error::AppendError;
use crate::mem::{Arena, RollingBuffer};
use crate::{FileNumber, Record};

#[derive(Clone, Debug)]
struct RecordMeta {
    start_offset: usize,
    // in a vec of RecordMeta, this field should be set only on the last record
    // which relate to that File.
    file_number: Option<FileNumber>,
    position: u64,
}

#[derive(Default)]
pub(crate) struct MemQueue {
    // Concatenated records
    concatenated_records: RollingBuffer,
    // If `record_metas` is not empty, `start_position` should be the position of the first record.
    start_position: u64,
    record_metas: VecDeque<RecordMeta>,
}

impl MemQueue {
    pub fn with_next_position(next_position: u64) -> Self {
        MemQueue {
            concatenated_records: RollingBuffer::new(),
            start_position: next_position,
            record_metas: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_metas.is_empty()
    }

    /// Returns the position of the last record appended to the queue.
    pub fn last_position(&self) -> Option<u64> {
        self.next_position().checked_sub(1)
    }

    /// Returns the last record stored in the queue.
    pub fn last_record<'a>(&'a self, arena: &'a Arena) -> Option<Record<'a>> {
        let record = self.record_metas.back()?;
        let record_payload = self
            .concatenated_records
            .get_range(record.start_offset.., arena);
        Some(Record {
            position: record.position,
            payload: record_payload,
        })
    }

    /// Returns what the next position should be.
    pub fn next_position(&self) -> u64 {
        self.record_metas
            .back()
            .map(|record| record.position + 1)
            .unwrap_or(self.start_position)
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
        arena: &mut Arena,
    ) -> Result<(), AppendError> {
        let next_position = self.next_position();
        if target_position < next_position {
            return Err(AppendError::Past);
        }

        if self.start_position == 0u64 && self.record_metas.is_empty() {
            self.start_position = target_position;
        }

        let file_number = if let Some(record_meta) = self.record_metas.back_mut() {
            if record_meta.file_number.as_ref() == Some(file_number) {
                record_meta.file_number.take().unwrap()
            } else {
                file_number.clone()
            }
        } else {
            file_number.clone()
        };

        let record_meta = RecordMeta {
            start_offset: self.concatenated_records.end_offset(),
            file_number: Some(file_number),
            position: target_position,
        };
        self.record_metas.push_back(record_meta);
        self.concatenated_records.extend_from_slice(payload, arena);
        Ok(())
    }

    /// Get the position of the record.
    ///
    /// Returns Ok(_) if the record was found, or Err(idx) with idx being the index just after
    /// where that element would have been if it existed.
    fn position_to_idx(&self, position: u64) -> Result<usize, usize> {
        self.record_metas
            .binary_search_by_key(&position, |record| record.position)
    }

    pub fn range<'a, R>(&'a self, range: R, arena: &'a Arena) -> impl Iterator<Item = Record> + 'a
    where R: RangeBounds<u64> + 'static {
        let start_idx: usize = match range.start_bound() {
            Bound::Included(&start_from) => {
                // if pos is included, we can use position_to_idx result directly
                self.position_to_idx(start_from)
                    .unwrap_or_else(std::convert::identity)
            }
            Bound::Excluded(&start_from) => {
                // if pos is excluded, an Err can be used directly, but an Ok must be incremented
                // by one to skip the element matching exactly.
                self.position_to_idx(start_from)
                    .map(|idx| idx + 1)
                    .unwrap_or_else(std::convert::identity)
            }
            Bound::Unbounded => 0,
        };
        (start_idx..self.record_metas.len())
            .take_while(move |idx| range.contains(&self.record_metas[*idx].position))
            .map(move |idx| {
                let record = &self.record_metas[idx];
                let position = record.position;
                let start_bound = Bound::Included(record.start_offset);
                let end_bound = if let Some(next_record_meta) = self.record_metas.get(idx + 1) {
                    Bound::Excluded(next_record_meta.start_offset)
                } else {
                    Bound::Unbounded
                };
                let payload = self
                    .concatenated_records
                    .get_range((start_bound, end_bound), arena);
                // let payload = concatenate_buffers(payload_buf);
                Record { position, payload }
            })
    }

    /// Removes all records coming before position, and including the record at "position".
    ///
    /// If truncating to a future position, make the queue go forward to that position.
    /// Return the number of record removed.
    pub fn truncate_up_to_included(&mut self, truncate_up_to_pos: u64, arena: &mut Arena) -> usize {
        if self.start_position > truncate_up_to_pos {
            return 0;
        }
        if truncate_up_to_pos + 1 >= self.next_position() {
            self.start_position = truncate_up_to_pos + 1;
            self.concatenated_records.clear(arena);
            let record_count = self.record_metas.len();
            self.record_metas.clear();
            return record_count;
        }
        let first_record_to_keep = self
            .position_to_idx(truncate_up_to_pos + 1)
            .unwrap_or_else(std::convert::identity);

        let start_offset_to_keep: usize = self.record_metas[first_record_to_keep].start_offset;
        self.record_metas.drain(..first_record_to_keep);
        self.concatenated_records
            .truncate_up_to_excluded(start_offset_to_keep, arena);
        self.start_position = truncate_up_to_pos + 1;
        first_record_to_keep
    }

    pub fn size(&self) -> usize {
        self.concatenated_records.len()
            + self.record_metas.len() * std::mem::size_of::<RecordMeta>()
    }

    pub fn capacity(&self) -> usize {
        self.concatenated_records.capacity()
            + self.record_metas.capacity() * std::mem::size_of::<RecordMeta>()
    }
}
