use std::convert::{TryFrom, TryInto};

use crate::Serializable;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MultiPlexedRecord<'a> {
    /// Adds a new record to a specific queue.
    AppendRecord {
        queue: &'a str,
        position: u64,
        payload: &'a [u8],
    },
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
    AppendRecord = 0,
    Truncate = 1,
    Touch = 2,
    DeleteQueue = 3,
    AppendRecords = 4,
}

impl TryFrom<u8> for RecordType {
    type Error = ();

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        match code {
            0 => Ok(RecordType::AppendRecord),
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
            MultiPlexedRecord::AppendRecord {
                position,
                queue,
                payload,
            } => {
                serialize(RecordType::AppendRecord, position, queue, payload, buffer);
            }
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
            RecordType::AppendRecord => Some(MultiPlexedRecord::AppendRecord {
                queue,
                position,
                payload,
            }),
            RecordType::AppendRecords => Some(MultiPlexedRecord::AppendRecords {
                queue,
                position,
                records: MultiRecord::new(payload)?,
            }),
            RecordType::Truncate => Some(MultiPlexedRecord::Truncate { queue, position }),
            RecordType::Touch => Some(MultiPlexedRecord::Touch { queue, position }),
            RecordType::DeleteQueue => Some(MultiPlexedRecord::DeleteQueue { queue, position }),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MultiRecord<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> MultiRecord<'a> {
    pub fn new(buffer: &[u8]) -> Option<MultiRecord> {
        let mut mrecord = MultiRecord {
            buffer,
            position: 0,
        };
        // verify the content is not corrupted
        for record in mrecord {
            record?;
        }

        mrecord.reset_position();

        Some(mrecord)
    }

    pub fn new_unchecked(buffer: &[u8]) -> MultiRecord {
        MultiRecord {
            buffer,
            position: 0,
        }
    }

    pub fn serialize<'b, T: Iterator<Item = &'b [u8]>>(items: T, mut position: u64) -> Vec<u8> {
        let mut res = Vec::new();
        for item in items {
            res.extend_from_slice(&position.to_le_bytes());
            position += 1;
            res.extend_from_slice(&item.len().to_le_bytes());
            res.extend_from_slice(item);
        }
        res
    }

    pub fn reset_position(&mut self) {
        self.position = 0;
    }
}

impl<'a> Iterator for MultiRecord<'a> {
    type Item = Option<(u64, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.buffer.len() {
            // no more record
            return None;
        }

        let buffer = &self.buffer[self.position..];
        if buffer.len() < 16 {
            // too short: corrupted
            self.position = buffer.len();
            return Some(None);
        }

        let position = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        let len = u64::from_le_bytes(buffer[8..16].try_into().unwrap()) as usize;

        let buffer = &buffer[16..];

        if buffer.len() < len {
            self.position = buffer.len();
            return Some(None);
        }

        self.position += 16 + len;

        Some(Some((position, &buffer[..len])))
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
        assert_eq!(num_record_types, 5);
    }
}
