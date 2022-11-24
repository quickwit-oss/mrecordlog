use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::error::{
    AppendError, CreateQueueError, DeleteQueueError, ReadRecordError, TruncateError,
};
use crate::mem;
use crate::record::{MultiPlexedRecord, MultiRecord};
use crate::recordlog::RecordWriter;
use crate::rolling::RollingWriter;

pub struct MultiRecordLog {
    record_log_writer: crate::recordlog::RecordWriter<RollingWriter>,
    in_mem_queues: mem::MemQueues,
    next_sync: SyncState,
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
        let rolling_reader = crate::rolling::RollingReader::open(directory_path).await?;
        let mut record_reader = crate::recordlog::RecordReader::open(rolling_reader);
        let mut in_mem_queues = crate::mem::MemQueues::default();
        loop {
            let file_number = record_reader.read().current_file().clone();
            if let Some(record) = record_reader.read_record().await? {
                match record {
                    MultiPlexedRecord::AppendRecords {
                        queue,
                        records,
                        position,
                    } => {
                        if !in_mem_queues.contains_queue(queue) {
                            in_mem_queues
                                .touch(queue, position, &file_number)
                                .map_err(|_| ReadRecordError::Corruption)?;
                        }
                        for record in records {
                            let (position, payload) = record?;
                            in_mem_queues
                                .append_record(queue, &file_number, position, payload)
                                .map_err(|_| ReadRecordError::Corruption)?;
                        }
                    }
                    MultiPlexedRecord::Truncate { position, queue } => {
                        in_mem_queues.truncate(queue, position);
                    }
                    MultiPlexedRecord::Touch { queue, position } => {
                        in_mem_queues
                            .touch(queue, position, &file_number)
                            .map_err(|_| ReadRecordError::Corruption)?;
                    }
                    MultiPlexedRecord::DeleteQueue { queue, position: _ } => {
                        in_mem_queues
                            .delete_queue(queue)
                            .map_err(|_| ReadRecordError::Corruption)?;
                    }
                }
            } else {
                break;
            }
        }
        let record_log_writer: RecordWriter<RollingWriter> = record_reader.into_writer().await?;
        Ok(MultiRecordLog {
            record_log_writer,
            in_mem_queues,
            next_sync: sync_policy.into(),
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
        let record = MultiPlexedRecord::Touch { queue, position: 0 };
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
    /// The local_position argument can optionally be passed to enforce nilpotence.
    /// TODO if an io Error is encounterred, the in mem queue and the record log will
    /// be in an inconsistent state.
    pub async fn append_record(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payload: &[u8],
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
    pub async fn append_records<'a, T: Iterator<Item = &'a [u8]>>(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payloads: T,
    ) -> Result<Option<u64>, AppendError> {
        let next_position = self.in_mem_queues.next_position(queue)?;
        if let Some(position) = position_opt {
            if position > next_position {
                return Err(AppendError::Future);
            } else if position + 1 == next_position {
                return Ok(None);
            } else if position < next_position {
                return Err(AppendError::Past);
            }
        }
        let position = position_opt.unwrap_or(next_position);
        let file_number = self.record_log_writer.current_file().clone();

        let records = MultiRecord::serialize(payloads, position);
        if records.is_empty() {
            // empty transaction: don't persist it
            return Ok(None);
        }

        let records = MultiRecord::new_unchecked(&records);
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
                .append_record(queue, &file_number, position, payload)?;
            max_position = position;
        }
        Ok(Some(max_position))
    }

    async fn touch_empty_queues(&mut self) -> Result<(), TruncateError> {
        for (queue_id, queue) in self.in_mem_queues.empty_queues() {
            let next_position = queue.next_position();
            let file_number = self.record_log_writer.current_file().clone();
            let record = MultiPlexedRecord::Touch {
                queue: queue_id,
                position: next_position,
            };
            self.record_log_writer.write_record(record).await?;
            queue.touch(&file_number, next_position)?;
        }
        Ok(())
    }

    /// Truncates the queue log.
    ///
    /// This method will always truncate the record log, and release the associated memory.
    pub async fn truncate(&mut self, queue: &str, position: u64) -> Result<(), TruncateError> {
        if position >= self.in_mem_queues.next_position(queue)? {
            return Err(TruncateError::Future);
        }
        self.in_mem_queues.truncate(queue, position);
        self.record_log_writer
            .write_record(MultiPlexedRecord::Truncate { position, queue })
            .await?;
        self.touch_empty_queues().await?;
        self.record_log_writer.gc().await?;
        self.sync_on_policy().await?;
        Ok(())
    }

    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Option<impl Iterator<Item = (u64, &[u8])> + '_>
    where
        R: RangeBounds<u64> + 'static,
    {
        // We do not rely on `entry` in order to avoid
        // the allocation.
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
}
