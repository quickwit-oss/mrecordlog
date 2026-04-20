use std::io;
use std::ops::{RangeBounds, RangeToInclusive};
use std::path::Path;

use bytes::Buf;
use tracing::{debug, info, warn};

use crate::error::{
    AppendError, CreateQueueError, DeleteQueueError, MissingQueue, ReadRecordError, TruncateError,
};
use crate::page_directory::{PageListReader, PageListWriter, PageRangeRef};
use crate::record::{MultiPlexedRecord, MultiRecord};
use crate::recordlog::RecordWriter;
use crate::{mem, PersistAction, PersistPolicy, PersistState, Record, ResourceUsage};

#[derive(Copy, Clone, Debug)]
pub struct Preferences {
    pub persist_policy: PersistPolicy,
    pub num_bytes: u64,
}

impl Default for Preferences {
    fn default() -> Preferences {
        Preferences {
            persist_policy: PersistPolicy::Always(PersistAction::Flush),
            num_bytes: 10_000_000,
        }
    }
}

pub struct MultiRecordLog {
    record_log_writer: crate::recordlog::RecordWriter<PageListWriter>,
    in_mem_queues: mem::MemQueues,
    next_persist: PersistState,
    // A simple buffer we reuse to avoid allocation.
    multi_record_spare_buffer: Vec<u8>,
}

impl MultiRecordLog {
    /// Open the multi record log, flushing after each operation, but not fsyncing.
    pub fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(directory_path, Preferences::default())
    }

    /// Open the multi record log, syncing following the provided policy.
    pub fn open_with_prefs(
        directory_path: &Path,
        preferences: Preferences,
    ) -> Result<Self, ReadRecordError> {
        let Preferences {
            persist_policy,
            num_bytes,
        } = preferences;
        // io errors are non-recoverable
        // TODO set num pages
        let queue_file = directory_path.join(&Path::new("mrecordlog.wal"));
        // TODO stop hard coding
        let directory = crate::page_directory::Directory::create_or_open(&queue_file, num_bytes)?;
        let page_reader = PageListReader::new(directory)?;
        let mut record_reader = crate::recordlog::RecordReader::open(page_reader);
        let mut in_mem_queues = crate::mem::MemQueues::default();
        debug!("loading wal");
        loop {
            let mut session = record_reader.start_session();
            let Ok(record) = record_reader.read_record::<MultiPlexedRecord>(&mut session) else {
                warn!("Detected corrupted record: some data may have been lost");
                continue;
            };
            if let Some(record) = record {
                match record {
                    MultiPlexedRecord::AppendRecords {
                        queue,
                        records,
                        position,
                    } => {
                        if !in_mem_queues.contains_queue(queue) {
                            in_mem_queues.ack_position(queue, position, &session);
                        }
                        for record in records {
                            // if this fails, it means some corruption wasn't detected at a lower
                            // level, or we wrote invalid data.
                            let (position, payload) = record?;
                            // this can fail if queue doesn't exist (it was created just above, so
                            // it does), or if the position is in the past. This can happen if the
                            // queue is deleted and recreated in a block which get skipped for
                            // corruption. In that case, maybe we should ack_position() and try
                            // to insert again?
                            in_mem_queues
                                .append_record(queue, &session, position, payload)
                                .map_err(|_| ReadRecordError::Corruption)?;
                        }
                    }
                    MultiPlexedRecord::Truncate {
                        truncate_range,
                        queue,
                    } => {
                        in_mem_queues.truncate(queue, truncate_range, &session);
                    }
                    MultiPlexedRecord::RecordPosition { queue, position } => {
                        in_mem_queues.ack_position(queue, position, &session);
                    }
                    MultiPlexedRecord::DeleteQueue { queue, position: _ } => {
                        // can fail if we don't know about the queue getting deleted. It's fine to
                        // just ignore the error, the queue no longer exists either way.
                        let _ = in_mem_queues.delete_queue(queue);
                    }
                }
            } else {
                break;
            }
        }
        // io errors are non-recoverable
        let record_log_writer: RecordWriter<PageListWriter> = record_reader.into_writer()?;
        let multi_record_log = MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            next_persist: persist_policy.into(),
            multi_record_spare_buffer: Vec::new(),
        };
        Ok(multi_record_log)
    }

    /// Creates a new queue.
    ///
    /// Returns an error if the queue already exists.
    pub fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        info!(queue = queue, "create queue");
        if self.queue_exists(queue) {
            return Err(CreateQueueError::AlreadyExists);
        }
        let mut session = self.record_log_writer.start_session()?;
        let record = MultiPlexedRecord::RecordPosition { queue, position: 0 };
        self.record_log_writer.write_record(record, &mut session)?;
        self.persist(PersistAction::FlushAndFsync)?;
        self.in_mem_queues.create_queue(queue, session)?;
        Ok(())
    }

    pub fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        info!(queue = queue, "delete queue");
        let position = self.in_mem_queues.next_position(queue)?;
        let record = MultiPlexedRecord::DeleteQueue { queue, position };
        let mut session = self.record_log_writer.start_session()?;
        self.record_log_writer.write_record(record, &mut session)?;
        self.in_mem_queues.delete_queue(queue)?;
        self.persist(PersistAction::FlushAndFsync)?;
        Ok(())
    }

    pub fn queue_exists(&self, queue: &str) -> bool {
        self.in_mem_queues.contains_queue(queue)
    }

    pub fn list_queues(&self) -> impl Iterator<Item = &str> {
        self.in_mem_queues.list_queues()
    }

    /// Appends a record to the log.
    ///
    /// The local_position argument can optionally be passed to enforce idempotence.
    /// TODO if an io Error is encounterred, the in mem queue and the record log will
    /// be in an inconsistent state.
    pub fn append_record(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payload: impl Buf,
    ) -> Result<Option<u64>, AppendError> {
        self.append_records(queue, position_opt, std::iter::once(payload))
    }

    /// Appends multiple records to the log.
    ///
    /// This operation is atomic: either all records get stored, or none do.
    /// However this function succeeding does not necessarily means records where stored, be sure
    /// to call [`Self::persist`] to make sure changes are persisted if you don't use
    /// [`PersistPolicy::Always`] (which is the default).
    pub fn append_records<T: Iterator<Item = impl Buf>>(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payloads: T,
    ) -> Result<Option<u64>, AppendError> {
        let next_position = self.in_mem_queues.next_position(queue)?;
        if let Some(position) = position_opt {
            // we accept position in the future, and move forward as required.
            if position + 1 == next_position {
                return Ok(None);
            } else if position < next_position {
                return Err(AppendError::Past);
            }
        }
        let position = position_opt.unwrap_or(next_position);

        let mut multi_record_spare_buffer = std::mem::take(&mut self.multi_record_spare_buffer);
        MultiRecord::serialize(payloads, position, &mut multi_record_spare_buffer);
        if multi_record_spare_buffer.is_empty() {
            self.multi_record_spare_buffer = multi_record_spare_buffer;
            // empty transaction: don't persist it
            return Ok(None);
        }

        let records = MultiRecord::new_unchecked(&multi_record_spare_buffer);
        let multi_record = MultiPlexedRecord::AppendRecords {
            position,
            queue,
            records,
        };

        let mut session: PageRangeRef = self.record_log_writer.start_session()?;
        self.record_log_writer
            .write_record(multi_record, &mut session)?;
        self.persist_on_policy()?;

        let mem_queue = self.in_mem_queues.get_queue_mut(queue)?;
        let mut max_position = position;
        for record in records {
            // we just serialized it, we know it's valid
            let (position, payload) = record.unwrap();
            mem_queue.append_record(&session, position, payload)?;
            max_position = position;
        }

        self.multi_record_spare_buffer = multi_record_spare_buffer;
        Ok(Some(max_position))
    }

    /// Truncates the queue up to a given `position`, included. This method immediately
    /// truncates the underlying in-memory queue whereas the backing log files are deleted
    /// asynchronously when they become exclusively composed of deleted records.
    ///
    /// This method will always truncate the record log and release the associated memory.
    /// It returns the number of records deleted.
    pub fn truncate(
        &mut self,
        queue: &str,
        truncate_range: RangeToInclusive<u64>,
    ) -> Result<usize, TruncateError> {
        info!(range=?truncate_range, queue = queue, "truncate queue");
        if !self.queue_exists(queue) {
            return Err(TruncateError::MissingQueue(queue.to_string()));
        }
        let mut session = self.record_log_writer.start_session()?;
        let truncate_record = MultiPlexedRecord::Truncate {
            truncate_range,
            queue,
        };
        self.record_log_writer
            .write_record(truncate_record, &mut session)?;
        let removed_count = self
            .in_mem_queues
            .truncate(queue, truncate_range, &session)
            .unwrap_or(0);
        // self.run_gc_if_necessary()?;
        self.persist_on_policy()?;
        Ok(removed_count)
    }

    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Result<impl Iterator<Item = Record>, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        self.in_mem_queues.range(queue, range)
    }

    /// Flush if the policy says it should be done
    fn persist_on_policy(&mut self) -> io::Result<()> {
        if let Some(persist_action) = self.next_persist.should_persist() {
            self.persist(persist_action)?;
            self.next_persist.update_persisted();
        }
        Ok(())
    }

    /// Flush and optionnally fsync data
    pub fn persist(&mut self, persist_action: PersistAction) -> io::Result<()> {
        self.record_log_writer.persist(persist_action)
    }

    /// Returns the position of the last record appended to the queue.
    pub fn last_position(&self, queue: &str) -> Result<Option<u64>, MissingQueue> {
        self.in_mem_queues.last_position(queue)
    }

    /// Returns the last record stored in the queue.
    pub fn last_record(&self, queue: &str) -> Result<Option<Record<'_>>, MissingQueue> {
        self.in_mem_queues.last_record(queue)
    }

    // Return the amount of memory and disk space used by mrecordlog.
    pub fn resource_usage(&self) -> ResourceUsage {
        let page_list_writer = self.record_log_writer.get_underlying_wrt();
        let num_pages = page_list_writer.num_pages();
        let num_used_pages = page_list_writer.num_used_pages();
        let (memory_used_bytes, memory_allocated_bytes) = self.in_mem_queues.size();
        ResourceUsage {
            memory_used_bytes,
            memory_allocated_bytes,
            num_pages,
            num_used_pages,
        }
    }
}
