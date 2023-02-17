use std::convert::{TryFrom, TryInto};

use crate::error::MultiRecordCorruption;
use crate::Serializable;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MultiPlexedRecord<'a> {
    /// Adds multiple records to a specific queue.
    AppendRecords {
        queue: &'a str,
        position: u64, //< not used, the payload contain the position for each record
        records: MultiRecord<'a>,
    },
    /// Records the truncation of a specific queue.
    Truncate { queue: &'a str, position: u64 },
    /// Records the next position of a given queue.
    /// If the queue does not exists, creates it.
    ///
    /// `position` is the position of the NEXT message to be appended.
    Touch { queue: &'a str, position: u64 },
    DeleteQueue {
        queue: &'a str,
        position: u64, //< not useful tbh
    },
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
enum RecordType {
    Truncate = 1,
    Touch = 2,
    DeleteQueue = 3,
    AppendRecords = 4,
}

impl TryFrom<u8> for RecordType {
    type Error = ();

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        match code {
            1 => Ok(RecordType::Truncate),
            2 => Ok(RecordType::Touch),
            3 => Ok(RecordType::DeleteQueue),
            4 => Ok(RecordType::AppendRecords),
            _ => Err(()),
        }
    }
}

fn serialize(
    record_type: RecordType,
    position: u64,
    queue: &str,
    payload: &[u8],
    buffer: &mut Vec<u8>,
) {
    assert!(queue.len() <= u16::MAX as usize);
    buffer.push(record_type as u8);
    buffer.extend_from_slice(&position.to_le_bytes());
    buffer.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    buffer.extend_from_slice(queue.as_bytes());
    buffer.extend(payload);
}

impl<'a> Serializable<'a> for MultiPlexedRecord<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        match *self {
            MultiPlexedRecord::AppendRecords {
                position,
                queue,
                records,
            } => {
                serialize(
                    RecordType::AppendRecords,
                    position,
                    queue,
                    records.buffer,
                    buffer,
                );
            }

            MultiPlexedRecord::Truncate { queue, position } => {
                serialize(RecordType::Truncate, position, queue, &[], buffer);
            }
            MultiPlexedRecord::Touch { queue, position } => {
                serialize(RecordType::Touch, position, queue, &[], buffer);
            }
            MultiPlexedRecord::DeleteQueue { position, queue } => {
                serialize(RecordType::DeleteQueue, position, queue, &[], buffer);
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<MultiPlexedRecord<'a>> {
        let enum_tag = RecordType::try_from(buffer[0]).ok()?;
        if buffer.len() < 8 {
            return None;
        }
        let position = u64::from_le_bytes(buffer[1..9].try_into().unwrap());
        let queue_len = u16::from_le_bytes(buffer[9..11].try_into().unwrap()) as usize;
        let queue = std::str::from_utf8(&buffer[11..][..queue_len]).ok()?;
        let payload = &buffer[11 + queue_len..];
        match enum_tag {
            RecordType::AppendRecords => Some(MultiPlexedRecord::AppendRecords {
                queue,
                position,
                records: MultiRecord::new(payload).ok()?,
            }),
            RecordType::Truncate => Some(MultiPlexedRecord::Truncate { queue, position }),
            RecordType::Touch => Some(MultiPlexedRecord::Touch { queue, position }),
            RecordType::DeleteQueue => Some(MultiPlexedRecord::DeleteQueue { queue, position }),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct MultiRecord<'a> {
    buffer: &'a [u8],
    byte_offset: usize,
}

impl<'a> MultiRecord<'a> {
    pub fn new(buffer: &[u8]) -> Result<MultiRecord, MultiRecordCorruption> {
        let mut mrecord = MultiRecord::new_unchecked(buffer);

        // verify the content is not corrupted
        for record in mrecord {
            record?;
        }

        mrecord.reset_position();

        Ok(mrecord)
    }

    pub fn new_unchecked(buffer: &[u8]) -> MultiRecord {
        MultiRecord {
            buffer,
            byte_offset: 0,
        }
    }

    pub fn serialize<'b, T: Iterator<Item = &'b [u8]>>(
        record_payloads: T,
        position: u64,
        output: &mut Vec<u8>,
    ) {
        Self::serialize_with_pos((position..).zip(record_payloads), output);
    }

    fn serialize_with_pos<'b>(
        record_payloads: impl Iterator<Item = (u64, &'b [u8])>,
        output: &mut Vec<u8>,
    ) {
        output.clear();
        for (position, record_payload) in record_payloads {
            assert!(record_payload.len() <= u32::MAX as usize);
            // TODO add assert for position monotonicity?
            output.extend_from_slice(&position.to_le_bytes());
            output.extend_from_slice(&(record_payload.len() as u32).to_le_bytes());
            output.extend_from_slice(record_payload);
        }
    }

    pub fn reset_position(&mut self) {
        self.byte_offset = 0;
    }
}

impl<'a> Iterator for MultiRecord<'a> {
    type Item = Result<(u64, &'a [u8]), MultiRecordCorruption>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.byte_offset == self.buffer.len() {
            // no more record
            return None;
        }

        let buffer = &self.buffer[self.byte_offset..];
        if buffer.len() < 10 {
            // too short: corrupted
            self.byte_offset = buffer.len();
            return Some(Err(MultiRecordCorruption));
        }

        let position = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(buffer[8..12].try_into().unwrap()) as usize;

        let buffer = &buffer[12..];

        if buffer.len() < len {
            self.byte_offset = buffer.len();
            return Some(Err(MultiRecordCorruption));
        }

        self.byte_offset += 12 + len;

        Some(Ok((position, &buffer[..len])))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::RecordType;

    #[test]
    fn test_record_type_serialize() {
        let mut num_record_types = 0;
        for code in 0u8..=255u8 {
            if let Ok(record_type) = RecordType::try_from(code) {
                assert_eq!(record_type as u8, code);
                num_record_types += 1;
            }
        }
        assert_eq!(num_record_types, 4);
    }
}
