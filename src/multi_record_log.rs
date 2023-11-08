use std::borrow::Cow;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Buf;
use tracing::warn;

use crate::error::{
    AppendError, CreateQueueError, DeleteQueueError, MissingQueue, ReadRecordError, TruncateError,
};
use crate::mem;
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
    pub async fn open(directory_path: &Path) -> Result<Self, ReadRecordError> {
        Self::open_with_prefs(directory_path, SyncPolicy::OnAppend).await
    }

    /// Open the multi record log, syncing following the provided policy.
    pub async fn open_with_prefs(
        directory_path: &Path,
        sync_policy: SyncPolicy,
    ) -> Result<Self, ReadRecordError> {
        // io errors are non-recoverable
        let rolling_reader = crate::rolling::RollingReader::open(directory_path).await?;
        let mut record_reader = crate::recordlog::RecordReader::open(rolling_reader);
        let mut in_mem_queues = crate::mem::MemQueues::default();
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
        Ok(MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            next_sync: sync_policy.into(),
            multi_record_spare_buffer: Vec::new(),
        })
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
        self.sync().await?;
        self.in_mem_queues.create_queue(queue)?;
        Ok(())
    }

    pub async fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError> {
        let position = self.in_mem_queues.next_position(queue)?;
        let record = MultiPlexedRecord::DeleteQueue { queue, position };
        self.record_log_writer.write_record(record).await?;
        self.sync().await?;
        self.in_mem_queues.delete_queue(queue)?;
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
        self.sync_on_policy().await?;

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

    async fn record_empty_queues_position(&mut self) -> Result<(), TruncateError> {
        for (queue_id, queue) in self.in_mem_queues.empty_queues() {
            let next_position = queue.next_position();
            let record = MultiPlexedRecord::RecordPosition {
                queue: queue_id,
                position: next_position,
            };
            self.record_log_writer.write_record(record).await?;
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
        if !self.queue_exists(queue) {
            return Err(TruncateError::MissingQueue(queue.to_string()));
        }
        self.record_log_writer
            .write_record(MultiPlexedRecord::Truncate { position, queue })
            .await?;
        if self
            .record_log_writer
            .directory()
            .has_files_that_can_be_deleted()
        {
            // We are about to delete files.
            // Let's make sure we record the offsets of the empty queues
            // so that we don't lose that information after dropping the files.
            self.record_empty_queues_position().await?;
            self.record_log_writer.directory().gc().await?;
        }
        self.sync_on_policy().await?;
        let removed_count = self
            .in_mem_queues
            .truncate(queue, position)
            .await
            .unwrap_or(0);
        Ok(removed_count)
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

    async fn sync_on_policy(&mut self) -> io::Result<()> {
        if self.next_sync.should_sync() {
            self.sync().await?;
            self.next_sync.update_synced();
        }
        Ok(())
    }

    pub async fn sync(&mut self) -> io::Result<()> {
        self.record_log_writer.flush().await
    }

    pub fn last_position(&self, queue: &str) -> Result<Option<u64>, MissingQueue> {
        self.in_mem_queues.last_position(queue)
    }

    /// Returns the quantity of data stored in the in memory queue.
    pub fn in_memory_size(&self) -> usize {
        self.in_mem_queues.size()
    }

    /// Returns the used disk space.
    ///
    /// This is typically higher than what [`Self::in_memory_size`] reports as records are first
    /// marked as truncated, and only get deleted once all other records in the same file are
    /// truncated too.
    pub fn on_disk_size(&self) -> usize {
        self.record_log_writer.size()
    }
}
