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

    /// Returns all sub-queues which are currently empty.
    pub fn empty_queues(&mut self) -> impl Iterator<Item = (&'_ str, &mut MemQueue)> + '_ {
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

    /// Ensure that the queue is empty and start_position = next_position.
    ///
    /// Returns an error if the queue already exists and contains elements,
    /// or is empty but has a next_position that does not match.
    pub fn ack_position(&mut self, queue: &str, next_position: u64) -> Result<(), TouchError> {
        if let Some(queue) = self.queues.get(queue) {
            // It is possible for `ack_position` to be called when a queue already exists.
            //
            // For instance, we may have recorded the position of an empty stale queue
            // twice in the same file. Nothing prevents that from happening today.
            //
            // Another possibility is if an IO error occured right after recording position
            // and before deleting files.
            if !queue.is_empty() || queue.next_position() != next_position {
                return Err(TouchError);
            }
        } else {
            // The queue does not exist! Let's create it and set the right `next_position`.
            self.queues.insert(
                queue.to_string(),
                MemQueue::with_next_position(next_position),
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
        if let Ok(queue) = self.get_queue_mut(queue) {
            queue.truncate(position);
        }
    }

    pub fn size(&self) -> usize {
        self.queues
            .iter()
            .map(|(name, queue)| name.len() + queue.size())
            .sum()
    }
}
