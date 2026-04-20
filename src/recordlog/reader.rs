use crate::error::ReadRecordError;
use crate::frame::{FrameReader, ReadFrameError};
use crate::{BlockRead, Serializable};

pub struct RecordReader<R> {
    pub(crate) frame_reader: FrameReader<R>,
    record_buffer: Vec<u8>,
    // true if we are in the middle of reading a multifragment record.
    // This is useful, as it makes it possible to drop a record
    // if one of its fragment was corrupted.
    within_record: bool,
}

impl<R: BlockRead + Unpin> RecordReader<R> {
    pub fn open(reader: R) -> Self {
        let frame_reader = FrameReader::open(reader);
        RecordReader {
            frame_reader,
            record_buffer: Vec::with_capacity(10_000),
            within_record: false,
        }
    }

    pub fn start_session(&mut self) -> R::Session {
        self.frame_reader.start_session()
    }

    /// Deserialize a record without actually consuming data.
    pub fn record<'a, S: Serializable<'a>>(&'a self) -> Option<S> {
        S::deserialize(&self.record_buffer)
    }

    /// Advance cursor and deserialize the next record.
    pub fn read_record<'a, S: Serializable<'a>>(
        &'a mut self,
        session: &mut R::Session,
    ) -> Result<Option<S>, ReadRecordError> {
        let has_record = self.go_next(session)?;
        if has_record {
            let record = self.record().ok_or(ReadRecordError::Corruption)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    // Attempts to position the reader to the next record and return
    // true or false whether such a record is available or not.
    pub fn go_next(&mut self, session: &mut R::Session) -> Result<bool, ReadRecordError> {
        loop {
            let frame = self.frame_reader.read_frame(session);
            match frame {
                Ok((frame_type, frame_payload)) => {
                    if frame_type.is_first_frame_of_record() {
                        self.within_record = true;
                        self.record_buffer.clear();
                    }
                    if self.within_record {
                        self.record_buffer.extend_from_slice(frame_payload);
                        if frame_type.is_last_frame_of_record() {
                            self.within_record = false;
                            return Ok(true);
                        }
                    }
                }
                Err(ReadFrameError::Corruption) => {
                    self.within_record = false;
                    return Err(ReadRecordError::Corruption);
                }
                Err(ReadFrameError::IoError(io_err)) => {
                    self.within_record = false;
                    return Err(ReadRecordError::IoError(io_err));
                }
                Err(ReadFrameError::NotAvailable) => {
                    return Ok(false);
                }
            }
        }
    }
}
