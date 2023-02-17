use std::ops::{Bound, RangeBounds};

use crate::error::AppendError;
use crate::rolling::FileNumber;

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
    concatenated_records: Vec<u8>,
    start_position: u64,
    record_metas: Vec<RecordMeta>,
}

impl MemQueue {
    pub fn with_next_position(next_position: u64) -> Self {
        MemQueue {
            concatenated_records: Vec::new(),
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
        self.concatenated_records.extend_from_slice(payload);
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

    pub fn range<R>(&self, range: R) -> impl Iterator<Item = (u64, &[u8])> + '_
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
                        &self.concatenated_records[start_offset..end_offset],
                    )
                } else {
                    (position, &self.concatenated_records[start_offset..])
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
        self.concatenated_records.drain(..start_offset_to_keep);
        self.start_position += first_record_to_keep as u64;
    }

    pub fn size(&self) -> usize {
        self.concatenated_records.len()
            + self.record_metas.len() * std::mem::size_of::<RecordMeta>()
    }
}
