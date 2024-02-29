use std::borrow::Cow;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Buf;
use tracing::{debug, event_enabled, info, warn, Level};

use crate::error::{
    AppendError, CreateQueueError, DeleteQueueError, MissingQueue, ReadRecordError, TruncateError,
};
use crate::mem;
use crate::mem::MemQueue;
use crate::record::{MultiPlexedRecord, MultiRecord};
use crate::recordlog::RecordWriter;
use crate::rolling::RollingWriter;

pub struct MultiRecordLog {
    record_log_writer: crate::recordlog::RecordWriter<RollingWriter>,
    in_mem_queues: mem::MemQueues,
    next_sync: SyncState,
    // A simple buffer we reuse to avoid allocation.
    multi_record_spare_buffer: Vec<u8>,
}

/// Policy for synchonizing and flushing data
pub enum SyncPolicy {
    /// Sync and flush at each operation
    OnAppend,
    /// Sync and flush regularly. Sync is realized on the first operation after the delay since
    /// last sync elapsed. This means if no new operation arrive, some content may not get
    /// flushed for a while.
    OnDelay(Duration),
}

#[derive(Debug)]
enum SyncState {
    OnAppend,
    OnDelay {
        next_sync: Instant,
        interval: Duration,
    },
}

impl SyncState {
    fn should_sync(&self) -> bool {
        match self {
            SyncState::OnAppend => true,
            SyncState::OnDelay { next_sync, .. } => *next_sync < Instant::now(),
        }
    }

    fn update_synced(&mut self) {
        match self {
            SyncState::OnAppend => (),
            SyncState::OnDelay {
                ref mut next_sync,
                interval,
            } => *next_sync = Instant::now() + *interval,
        }
    }
}

impl From<SyncPolicy> for SyncState {
    fn from(val: SyncPolicy) -> SyncState {
        match val {
            SyncPolicy::OnAppend => SyncState::OnAppend,
            SyncPolicy::OnDelay(dur) => SyncState::OnDelay {
                next_sync: Instant::now() + dur,
                interval: dur,
            },
        }
    }
}

impl MultiRecordLog {
    /// Open the multi record log, syncing after each operation.
    pub fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(directory_path, SyncPolicy::OnAppend)
    }

    /// Open the multi record log, syncing following the provided policy.
    pub fn open_with_prefs(
        directory_path: &Path,
        sync_policy: SyncPolicy,
    ) -> Result<Self, ReadRecordError> {
        // io errors are non-recoverable
        let rolling_reader = crate::rolling::RollingReader::open(directory_path)?;
        let mut record_reader = crate::recordlog::RecordReader::open(rolling_reader);
        let mut in_mem_queues = crate::mem::MemQueues::default();
        debug!("loading wal");
        loop {
            let file_number = record_reader.read().current_file().clone();
            let Ok(record) = record_reader.read_record() else {
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
                            in_mem_queues.ack_position(queue, position);
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
                                .append_record(queue, &file_number, position, payload)
                                .map_err(|_| ReadRecordError::Corruption)?;
                        }
                    }
                    MultiPlexedRecord::Truncate { position, queue } => {
                        in_mem_queues.truncate(queue, position);
                    }
                    MultiPlexedRecord::RecordPosition { queue, position } => {
                        in_mem_queues.ack_position(queue, position);
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
        let record_log_writer: RecordWriter<RollingWriter> = record_reader.into_writer()?;
        let mut multi_record_log = MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            next_sync: sync_policy.into(),
            multi_record_spare_buffer: Vec::new(),
        };
        multi_record_log.run_gc_if_necessary()?;
        Ok(multi_record_log)
    }

    #[cfg(test)]
    pub fn list_file_numbers(&self) -> Vec<u64> {
        let rolling_writer = self.record_log_writer.get_underlying_wrt();
        rolling_writer.list_file_numbers()
    }

    /// Creates a new queue.
    ///
    /// Returns an error if the queue already exists.
    pub fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        info!(queue = queue, "create queue");
        if self.queue_exists(queue) {
            return Err(CreateQueueError::AlreadyExists);
        }
        let record = MultiPlexedRecord::RecordPosition { queue, position: 0 };
        self.record_log_writer.write_record(record)?;
        self.sync()?;
        self.in_mem_queues.create_queue(queue)?;
        Ok(())
    }

    pub fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        info!(queue = queue, "delete queue");
        let position = self.in_mem_queues.next_position(queue)?;
        let record = MultiPlexedRecord::DeleteQueue { queue, position };
        self.record_log_writer.write_record(record)?;
        self.in_mem_queues.delete_queue(queue)?;
        self.run_gc_if_necessary()?;
        self.sync()?;
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
    /// to call [`Self::sync`] to make sure changes are persisted if you don't use
    /// [`SyncPolicy::OnAppend`] (which is the default).
    pub fn append_records<'a, T: Iterator<Item = impl Buf>>(
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
        let file_number = self.record_log_writer.current_file().clone();

        let mut multi_record_spare_buffer = std::mem::take(&mut self.multi_record_spare_buffer);
        MultiRecord::serialize(payloads, position, &mut multi_record_spare_buffer);
        if multi_record_spare_buffer.is_empty() {
            self.multi_record_spare_buffer = multi_record_spare_buffer;
            // empty transaction: don't persist it
            return Ok(None);
        }

        let records = MultiRecord::new_unchecked(&multi_record_spare_buffer);
        let record = MultiPlexedRecord::AppendRecords {
            position,
            queue,
            records,
        };
        self.record_log_writer.write_record(record)?;
        self.sync_on_policy()?;

        let mem_queue = self.in_mem_queues.get_queue_mut(queue)?;

        let mut max_position = position;
        for record in records {
            // we just serialized it, we know it's valid
            let (position, payload) = record.unwrap();
            mem_queue.append_record(&file_number, position, payload)?;
            max_position = position;
        }

        self.multi_record_spare_buffer = multi_record_spare_buffer;
        Ok(Some(max_position))
    }

    fn record_empty_queues_position(&mut self) -> io::Result<()> {
        let mut has_empty_queues = false;
        for (queue_id, queue) in self.in_mem_queues.empty_queues() {
            let next_position = queue.next_position();
            let record = MultiPlexedRecord::RecordPosition {
                queue: queue_id,
                position: next_position,
            };
            self.record_log_writer.write_record(record)?;
            has_empty_queues = true
        }
        if has_empty_queues {
            // We need to sync here! We are remove files from the FS
            // so we need to make sure our empty queue positions are properly persisted.
            self.sync()?;
        }
        Ok(())
    }

    /// Truncates the queue up to `position`, included. This method immediately truncates the
    /// underlying in-memory queue whereas the backing log files are deleted asynchronously when
    /// they become exclusively composed of deleted records.
    ///
    /// This method will always truncate the record log and release the associated memory.
    /// It returns the number of records deleted.
    pub fn truncate(&mut self, queue: &str, position: u64) -> Result<usize, TruncateError> {
        info!(position = position, queue = queue, "truncate queue");
        if !self.queue_exists(queue) {
            return Err(TruncateError::MissingQueue(queue.to_string()));
        }
        self.record_log_writer
            .write_record(MultiPlexedRecord::Truncate { position, queue })?;
        let removed_count = self.in_mem_queues.truncate(queue, position).unwrap_or(0);
        self.run_gc_if_necessary()?;
        self.sync_on_policy()?;
        Ok(removed_count)
    }

    fn run_gc_if_necessary(&mut self) -> io::Result<()> {
        debug!("run_gc_if_necessary");
        if self
            .record_log_writer
            .directory()
            .has_files_that_can_be_deleted()
        {
            // We are about to delete files.
            // Let's make sure we record the offsets of the empty queues
            // so that we don't lose that information after dropping the files.
            //
            // But first we clone the current file number to make sure that the file that will
            // contain the truncate positions it self won't be GC'ed.
            let _file_number = self.record_log_writer.current_file().clone();
            self.record_empty_queues_position()?;
            self.record_log_writer.directory().gc()?;
        }
        // only execute the following if we are above the debug  level in tokio tracing
        if event_enabled!(Level::DEBUG) {
            for queue in self.list_queues() {
                let queue: &MemQueue = self.in_mem_queues.get_queue(queue).unwrap();
                let first_pos = queue.range(..).next().map(|(pos, _)| pos);
                let last_pos = queue.last_position();
                debug!(first_pos=?first_pos, last_pos=?last_pos, "queue positions after gc");
            }
        }
        Ok(())
    }

    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Result<impl Iterator<Item = (u64, Cow<[u8]>)> + '_, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        self.in_mem_queues.range(queue, range)
    }

    fn sync_on_policy(&mut self) -> io::Result<()> {
        if self.next_sync.should_sync() {
            self.sync()?;
            self.next_sync.update_synced();
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.record_log_writer.flush()
    }

    /// Returns the position of the last record appended to the queue.
    pub fn last_position(&self, queue: &str) -> Result<Option<u64>, MissingQueue> {
        self.in_mem_queues.last_position(queue)
    }

    /// Returns the last record stored in the queue.
    pub fn last_record(&self, queue: &str) -> Result<Option<(u64, Cow<[u8]>)>, MissingQueue> {
        self.in_mem_queues.last_record(queue)
    }

    /// Returns the quantity of data stored in the in memory queue.
    pub fn memory_usage(&self) -> usize {
        self.in_mem_queues.size()
    }

    /// Returns the used disk space.
    ///
    /// This is typically higher than what [`Self::memory_usage`] reports as records are first
    /// marked as truncated, and only get deleted once all other records in the same file are
    /// truncated too.
    pub fn disk_usage(&self) -> usize {
        self.record_log_writer.size()
    }
}
