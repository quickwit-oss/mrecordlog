use std::collections::HashMap;
use std::ops::RangeBounds;

use crate::error::{AlreadyExists, AppendError, MissingQueue, TouchError};
use crate::mem::MemQueue;
use crate::rolling::FileNumber;

#[derive(Default)]
pub struct MemQueues {
    queues: HashMap<String, MemQueue>,
}

impl MemQueues {
    /// The file number argument is here unused. Its point is just to make sure we
    /// flushed the file before updating the in memory queue.
    pub fn create_queue(&mut self, queue: &str) -> Result<(), AlreadyExists> {
        if self.queues.contains_key(queue) {
            return Err(AlreadyExists);
        }
        self.queues.insert(queue.to_string(), MemQueue::default());
        Ok(())
    }

    pub fn delete_queue(&mut self, queue: &str) -> Result<(), MissingQueue> {
        if self.queues.remove(queue).is_none() {
            return Err(MissingQueue(queue.to_string()));
        }
        Ok(())
    }

    pub fn empty_queue_positions(&mut self) -> impl Iterator<Item = (&'_ str, &mut MemQueue)> + '_ {
        self.queues.iter_mut().filter_map(|(queue, mem_queue)| {
            if mem_queue.is_empty() {
                Some((queue.as_str(), mem_queue))
            } else {
                None
            }
        })
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
        Some(self.queues.get(queue)?.range(range))
    }

    fn get_queue(&self, queue: &str) -> Result<&MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

    fn get_queue_mut(&mut self, queue: &str) -> Result<&mut MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get_mut(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

    pub fn append_record(
        &mut self,
        queue: &str,
        file_number: &FileNumber,
        target_position: u64,
        payload: &[u8],
    ) -> Result<(), AppendError> {
        self.get_queue_mut(queue)?
            .append_record(file_number, target_position, payload)?;
        Ok(())
    }

    pub fn contains_queue(&self, queue: &str) -> bool {
        self.queues.contains_key(queue)
    }

    pub fn list_queues(&self) -> impl Iterator<Item = &str> {
        self.queues.keys().map(|queue| queue.as_str())
    }

    fn get_or_create_queue_mut(&mut self, queue: &str) -> &mut MemQueue {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        if !self.queues.contains_key(queue) {
            self.queues.insert(queue.to_string(), MemQueue::default());
        }
        self.queues.get_mut(queue).unwrap()
    }

    pub fn touch(
        &mut self,
        queue: &str,
        start_position: u64,
        file_number: &FileNumber,
    ) -> Result<(), TouchError> {
        if self.queues.contains_key(queue) {
            let queue = self.get_queue_mut(queue).unwrap();
            queue.touch(file_number, start_position)?;
        } else {
            self.queues.insert(
                queue.to_string(),
                MemQueue::with_next_position(start_position, file_number.clone()),
            );
        }
        Ok(())
    }

    pub fn next_position(&self, queue: &str) -> Result<u64, MissingQueue> {
        Ok(self.get_queue(queue)?.next_position())
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    //
    /// If there are no records `<= position`, the method will
    /// not do anything.
    ///
    /// If one or more files should be removed,
    /// returns the range of the files that should be removed
    pub fn truncate(&mut self, queue: &str, position: u64) {
        self.get_or_create_queue_mut(queue).truncate(position);
    }
}
