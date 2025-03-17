use std::convert::{TryFrom, TryInto};
use std::ops::RangeToInclusive;

use bytes::Buf;
use tracing::error;

use crate::error::MultiRecordCorruption;
use crate::Serializable;

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) enum MultiPlexedRecord<'a> {
    /// Adds multiple records to a specific queue.
    AppendRecords {
        queue: &'a str,
        position: u64, //< not used, the payload contain the position for each record
        records: MultiRecord<'a>,
    },
    /// Records the truncation of a specific queue, up to and including the position.
    Truncate {
        queue: &'a str,
        truncate_range: RangeToInclusive<u64>,
    },
    /// Records the next position of a given queue.
    /// If the queue does not exists, creates it.
    ///
    /// `position` is the position of the NEXT message to be appended.
    RecordPosition { queue: &'a str, position: u64 },
    DeleteQueue {
        queue: &'a str,
        position: u64, //< not useful tbh
    },
}

impl std::fmt::Debug for MultiPlexedRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::AppendRecords {
                queue,
                position,
                records,
            } => f
                .debug_struct("AppendRecords")
                .field("queue", queue)
                .field("position", position)
                .field("records_len", &records.count())
                .finish(),
            Self::Truncate {
                queue,
                truncate_range,
            } => f
                .debug_struct("Truncate")
                .field("queue", queue)
                .field("range", truncate_range)
                .finish(),
            Self::RecordPosition { queue, position } => f
                .debug_struct("RecordPosition")
                .field("queue", queue)
                .field("position", position)
                .finish(),
            Self::DeleteQueue { queue, position } => f
                .debug_struct("DeleteQueue")
                .field("queue", queue)
                .field("position", position)
                .finish(),
        }
    }
}

impl<'a> MultiPlexedRecord<'a> {
    #[allow(dead_code)]
    pub fn queue_id(&self) -> &'a str {
        match self {
            Self::AppendRecords { queue, .. } => queue,
            Self::Truncate { queue, .. } => queue,
            Self::RecordPosition { queue, .. } => queue,
            Self::DeleteQueue { queue, .. } => queue,
        }
    }
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

            MultiPlexedRecord::Truncate {
                queue,
                truncate_range,
            } => {
                serialize(RecordType::Truncate, truncate_range.end, queue, &[], buffer);
            }
            MultiPlexedRecord::RecordPosition { queue, position } => {
                serialize(RecordType::Touch, position, queue, &[], buffer);
            }
            MultiPlexedRecord::DeleteQueue { position, queue } => {
                serialize(RecordType::DeleteQueue, position, queue, &[], buffer);
            }
        }
    }

    fn deserialize(buffer: &'a [u8]) -> Option<MultiPlexedRecord<'a>> {
        const HEADER_LEN: usize = 11;
        if buffer.len() < HEADER_LEN {
            error!(buffer=?buffer, "multiplexed record buffer too short");
            return None;
        }
        let (header, body) = buffer.split_at(HEADER_LEN);
        let enum_tag = RecordType::try_from(header[0]).ok()?;
        let position = u64::from_le_bytes(header[1..9].try_into().unwrap());
        let queue_len = u16::from_le_bytes(header[9..HEADER_LEN].try_into().unwrap()) as usize;
        if body.len() < queue_len {
            error!(
                queue_len = queue_len,
                body_len = body.len(),
                "record body too short"
            );
            return None;
        }
        let (queue_bytes, payload) = body.split_at(queue_len);
        let Ok(queue) = std::str::from_utf8(queue_bytes) else {
            let truncated_len = queue_bytes.len().min(10);
            error!(queue_name_bytes=?queue_bytes[..truncated_len], "non-utf8 queue name");
            return None;
        };
        match enum_tag {
            RecordType::AppendRecords => Some(MultiPlexedRecord::AppendRecords {
                queue,
                position,
                records: MultiRecord::new(payload).ok()?,
            }),
            RecordType::Truncate => Some(MultiPlexedRecord::Truncate {
                queue,
                truncate_range: ..=position,
            }),
            RecordType::Touch => Some(MultiPlexedRecord::RecordPosition { queue, position }),
            RecordType::DeleteQueue => Some(MultiPlexedRecord::DeleteQueue { queue, position }),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct MultiRecord<'a> {
    /// The buffer contains concatenated items following this pattern:
    /// <u64 position><u32 len><len bytes>
    /// The two integers are encoded as little endian.
    buffer: &'a [u8],
    /// Offset into the buffer above used while iterating over the serialized items.
    byte_offset: usize,
}

impl MultiRecord<'_> {
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

    pub fn serialize<T: Iterator<Item = impl Buf>>(
        record_payloads: T,
        position: u64,
        output: &mut Vec<u8>,
    ) {
        Self::serialize_with_pos((position..).zip(record_payloads), output);
    }

    fn serialize_with_pos(
        record_payloads: impl Iterator<Item = (u64, impl Buf)>,
        output: &mut Vec<u8>,
    ) {
        output.clear();
        for (position, mut record_payload) in record_payloads {
            assert!(record_payload.remaining() <= u32::MAX as usize);
            // TODO add assert for position monotonicity?
            let record_payload = &mut record_payload;
            output.extend_from_slice(&position.to_le_bytes());
            output.extend_from_slice(&(record_payload.remaining() as u32).to_le_bytes());
            while record_payload.has_remaining() {
                let chunk = record_payload.chunk();
                output.extend_from_slice(record_payload.chunk());
                record_payload.advance(chunk.len());
            }
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
        const HEADER_LEN: usize = 12;
        let buffer = &self.buffer[self.byte_offset..];
        if buffer.len() < HEADER_LEN {
            // too short: corrupted
            self.byte_offset = buffer.len();
            return Some(Err(MultiRecordCorruption));
        }

        let position = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(buffer[8..HEADER_LEN].try_into().unwrap()) as usize;

        let buffer = &buffer[HEADER_LEN..];

        if buffer.len() < len {
            self.byte_offset = buffer.len();
            return Some(Err(MultiRecordCorruption));
        }

        self.byte_offset += HEADER_LEN + len;

        Some(Ok((position, &buffer[..len])))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::{MultiPlexedRecord, MultiRecord, RecordType};
    use crate::Serializable;

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

    #[test]
    fn test_multirecord_deserialization_ok() {
        let mut buffer: Vec<u8> = vec![];
        MultiRecord::serialize(
            [b"123".as_slice(), b"4567".as_slice()].into_iter(),
            5,
            &mut buffer,
        );
        match MultiRecord::new(&buffer) {
            Err(_) => panic!("Parsing serialized buffers should work"),
            Ok(record) => {
                let items: Vec<_> = record
                    .into_iter()
                    .map(|item| item.expect("Deserializing item should work"))
                    .collect();
                assert_eq!(
                    items,
                    vec![(5u64, b"123".as_slice()), (6u64, b"4567".as_slice())]
                );
            }
        }
    }

    #[test]
    fn test_multirecord_deserialization_corruption() {
        let mut buffer: Vec<u8> = vec![];
        MultiRecord::serialize(
            [b"123".as_slice(), b"4567".as_slice()].into_iter(),
            5,
            &mut buffer,
        );
        let mut num_errors = 0;
        for num_truncated_bytes in 1..buffer.len() {
            // This should not panic. Typically, this will be an error, but
            // deserializing can also succeed (but will have wrong data).
            num_errors +=
                MultiRecord::new(&buffer[..buffer.len() - num_truncated_bytes]).is_err() as i32;
        }
        assert!(num_errors >= 1);
    }

    #[test]
    fn test_multiplexedrecord_deserialization_ok() {
        let mut buffer_multirecord: Vec<u8> = vec![];
        MultiRecord::serialize([b"123".as_slice()].into_iter(), 2, &mut buffer_multirecord);
        let record = MultiPlexedRecord::AppendRecords {
            queue: "queue_name",
            position: 10,
            records: MultiRecord::new_unchecked(&buffer_multirecord),
        };
        let mut buffer_multiplexed: Vec<u8> = vec![];
        record.serialize(&mut buffer_multiplexed);
        match MultiPlexedRecord::deserialize(&buffer_multiplexed) {
            None => panic!("Deserialization should work"),
            Some(parsed_record) => assert_eq!(parsed_record, record),
        }
    }

    #[test]
    fn test_multiplexedrecord_deserialization_corruption() {
        let mut buffer_multirecord: Vec<u8> = vec![];
        MultiRecord::serialize([b"123".as_slice()].into_iter(), 2, &mut buffer_multirecord);
        let record = MultiPlexedRecord::AppendRecords {
            queue: "queue_name",
            position: 10,
            records: MultiRecord::new_unchecked(&buffer_multirecord),
        };
        let mut buffer_multiplexed: Vec<u8> = vec![];
        record.serialize(&mut buffer_multiplexed);

        let mut num_errors = 0;
        for num_truncated_bytes in 1..buffer_multiplexed.len() {
            // This should not panic. Typically, this will be an error, but
            // deserializing can also succeed (but will have wrong data).
            num_errors += MultiPlexedRecord::deserialize(
                &buffer_multiplexed[..buffer_multiplexed.len() - num_truncated_bytes],
            )
            .is_none() as i32;
        }
        assert!(num_errors >= 1);
    }
}
