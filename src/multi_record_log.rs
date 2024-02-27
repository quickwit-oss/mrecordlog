use std::borrow::Cow;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Buf;
use tracing::{debug, event_enabled, warn, Level};

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
    next_persist: PersistState,
    // A simple buffer we reuse to avoid allocation.
    multi_record_spare_buffer: Vec<u8>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistAction {
    /// The buffer will be flushed to the OS, but not necessarily to the disk.
    Flush,
    /// The buffer will be flushed to the OS, and the OS will be asked to flush
    /// it to the disk.
    FlushAndFsync,
}

impl PersistAction {
    fn is_fsync(self) -> bool {
        self == PersistAction::FlushAndFsync
    }
}

/// We have two type of operations on the mrecordlog.
///
/// Critical records are relatively rare and really need to be persisted:
/// - RecordPosition { queue: &'a str, position: u64 },
/// - DeleteQueue.
///
/// For these operations, we want to always flush and fsync.
///
/// On the other hand,
/// - Truncate
/// - AppendRecords
/// are considered are more frequent and one might want to sacrifice
/// persistence guarantees for performance.
///
/// The `PersistPolicy` defines the trade-off applied for the second kind of
/// operations.
#[derive(Clone, Debug)]
pub enum PersistPolicy {
    /// Only persist data when asked for, and when critical records are written
    DoNothing(PersistAction),
    /// Pesiste data once every interval, and when critical records are written
    OnDelay {
        interval: Duration,
        action: PersistAction,
    },
    /// Persist data after each action
    Always(PersistAction),
}

#[derive(Debug)]
enum PersistState {
    OnAppend(PersistAction),
    OnDelay {
        next_sync: Instant,
        interval: Duration,
        action: PersistAction,
    },
    OnRequest(PersistAction),
}

impl PersistState {
    fn should_persist(&self) -> bool {
        match self {
            PersistState::OnAppend(_) => true,
            PersistState::OnDelay { next_sync, .. } => *next_sync < Instant::now(),
            PersistState::OnRequest(_) => false,
        }
    }

    fn update_persisted(&mut self) {
        match self {
            PersistState::OnAppend(_) | PersistState::OnRequest(_) => (),
            PersistState::OnDelay {
                ref mut next_sync,
                interval,
                ..
            } => *next_sync = Instant::now() + *interval,
        }
    }

    fn action(&self) -> PersistAction {
        match self {
            PersistState::OnAppend(action) => *action,
            PersistState::OnDelay { action, .. } => *action,
            PersistState::OnRequest(action) => *action,
        }
    }
}

impl From<PersistPolicy> for PersistState {
    fn from(val: PersistPolicy) -> PersistState {
        match val {
            PersistPolicy::Always(action) => PersistState::OnAppend(action),
            PersistPolicy::OnDelay { interval, action } => PersistState::OnDelay {
                next_sync: Instant::now() + interval,
                interval,
                action,
            },
            PersistPolicy::DoNothing(action) => PersistState::OnRequest(action),
        }
    }
}

impl MultiRecordLog {
    /// Open the multi record log, flushing after each operation, but not fsyncing.
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(directory_path, PersistPolicy::Always(PersistAction::Flush)).await
    }

    /// Open the multi record log, syncing following the provided policy.
    pub async fn open_with_prefs(
        directory_path: &Path,
        persist_policy: PersistPolicy,
    ) -> Result<Self, ReadRecordError> {
        // io errors are non-recoverable
        let rolling_reader = crate::rolling::RollingReader::open(directory_path).await?;
        let mut record_reader = crate::recordlog::RecordReader::open(rolling_reader);
        let mut in_mem_queues = crate::mem::MemQueues::default();
        debug!("loading wal");
        loop {
            let file_number = record_reader.read().current_file().clone();
            let Ok(record) = record_reader.read_record().await else {
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
                                .await
                                .map_err(|_| ReadRecordError::Corruption)?;
                        }
                    }
                    MultiPlexedRecord::Truncate { position, queue } => {
                        in_mem_queues.truncate(queue, position).await;
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
        let record_log_writer: RecordWriter<RollingWriter> = record_reader.into_writer().await?;
        let mut multi_record_log = MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            next_persist: persist_policy.into(),
            multi_record_spare_buffer: Vec::new(),
        };
        multi_record_log.run_gc_if_necessary().await?;
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
    pub async fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError> {
        if self.queue_exists(queue) {
            return Err(CreateQueueError::AlreadyExists);
        }
        let record = MultiPlexedRecord::RecordPosition { queue, position: 0 };
        self.record_log_writer.write_record(record).await?;
        self.persist_and_maybe_fsync().await?;
        self.in_mem_queues.create_queue(queue)?;
        Ok(())
    }

    pub async fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        let position = self.in_mem_queues.next_position(queue)?;
        let record = MultiPlexedRecord::DeleteQueue { queue, position };
        self.record_log_writer.write_record(record).await?;
        self.in_mem_queues.delete_queue(queue)?;
        self.run_gc_if_necessary().await?;
        self.persist_and_maybe_fsync().await?;
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
    pub async fn append_record(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payload: impl Buf,
    ) -> Result<Option<u64>, AppendError> {
        self.append_records(queue, position_opt, std::iter::once(payload))
            .await
    }

    /// Appends multiple records to the log.
    ///
    /// This operation is atomic: either all records get stored, or none do.
    /// However this function succeeding does not necessarily means records where stored, be sure
    /// to call [`Self::sync`] to make sure changes are persisted if you don't use
    /// [`SyncPolicy::OnAppend`] (which is the default).
    pub async fn append_records<'a, T: Iterator<Item = impl Buf>>(
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
        self.record_log_writer.write_record(record).await?;
        self.persist_on_policy().await?;

        let mut max_position = position;
        for record in records {
            // we just serialized it, we know it's valid
            let (position, payload) = record.unwrap();
            self.in_mem_queues
                .append_record(queue, &file_number, position, payload)
                .await?;
            max_position = position;
        }

        self.multi_record_spare_buffer = multi_record_spare_buffer;
        Ok(Some(max_position))
    }

    async fn record_empty_queues_position(&mut self) -> io::Result<()> {
        let mut has_empty_queues = false;
        for (queue_id, queue) in self.in_mem_queues.empty_queues() {
            let next_position = queue.next_position();
            let record = MultiPlexedRecord::RecordPosition {
                queue: queue_id,
                position: next_position,
            };
            self.record_log_writer.write_record(record).await?;
            has_empty_queues = true
        }
        if has_empty_queues {
            // We need to sync here! We are remove files from the FS
            // so we need to make sure our empty queue positions are properly persisted.
            self.persist_and_maybe_fsync().await?;
        }
        Ok(())
    }

    /// Truncates the queue up to `position`, included. This method immediately truncates the
    /// underlying in-memory queue whereas the backing log files are deleted asynchronously when
    /// they become exclusively composed of deleted records.
    ///
    /// This method will always truncate the record log and release the associated memory.
    /// It returns the number of records deleted.
    pub async fn truncate(&mut self, queue: &str, position: u64) -> Result<usize, TruncateError> {
        debug!(position = position, queue = queue, "truncate queue");
        if !self.queue_exists(queue) {
            return Err(TruncateError::MissingQueue(queue.to_string()));
        }
        self.record_log_writer
            .write_record(MultiPlexedRecord::Truncate { position, queue })
            .await?;
        let removed_count = self
            .in_mem_queues
            .truncate(queue, position)
            .await
            .unwrap_or(0);
        self.run_gc_if_necessary().await?;
        self.persist_on_policy().await?;
        Ok(removed_count)
    }

    async fn run_gc_if_necessary(&mut self) -> io::Result<()> {
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
            self.record_empty_queues_position().await?;
            self.record_log_writer.directory().gc().await?;
        }
        // only execute the following if we are above the debug  level in tokio tracing
        if event_enabled!(Level::DEBUG) {
            for queue in self.list_queues() {
                let queue: &MemQueue = self.in_mem_queues.get_queue(queue).unwrap();
                let first_pos = queue.range(..).next().map(|(pos, _)| pos);
                let last_pos = queue.last_position();
                debug!(first_pos=?first_pos, last_pos=?last_pos, "queue");
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

    /// Flush if the policy says it should be done
    async fn persist_on_policy(&mut self) -> io::Result<()> {
        if self.next_persist.should_persist() {
            self.persist_and_maybe_fsync().await?;
            self.next_persist.update_persisted();
        }
        Ok(())
    }

    async fn persist_and_maybe_fsync(&mut self) -> io::Result<()> {
        self.persist(self.next_persist.action().is_fsync()).await
    }

    /// Flush and optionnally fsync data
    pub async fn persist(&mut self, fsync: bool) -> io::Result<()> {
        self.record_log_writer.flush(fsync).await
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
