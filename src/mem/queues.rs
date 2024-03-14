use std::collections::HashMap;
use std::ops::RangeBounds;

use tracing::{info, warn};

use crate::error::{AlreadyExists, AppendError, MissingQueue};
use crate::mem::{Arena, MemQueue};
use crate::{FileNumber, Record};

#[derive(Default)]
pub(crate) struct MemQueues {
    queues: HashMap<String, MemQueue>,
    pub(crate) arena: Arena,
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
        info!(queue = queue, "deleting queue");
        if self.queues.remove(queue).is_none() {
            warn!(queue = queue, "attempted to remove a non-existing queue");
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
    ) -> Result<impl Iterator<Item = Record> + '_, MissingQueue>
    where
        R: RangeBounds<u64> + 'static,
    {
        if let Some(queue) = self.queues.get(queue) {
            Ok(queue.range(range, &self.arena))
        } else {
            Err(MissingQueue(queue.to_string()))
        }
    }

    pub(crate) fn get_queue(&self, queue: &str) -> Result<&MemQueue, MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        self.queues
            .get(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))
    }

    pub(crate) fn get_queue_mut(
        &mut self,
        queue: &str,
    ) -> Result<(&mut MemQueue, &mut Arena), MissingQueue> {
        // We do not rely on `entry` in order to avoid
        // the allocation.
        let queue = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))?;
        Ok((queue, &mut self.arena))
    }

    pub fn append_record(
        &mut self,
        queue: &str,
        file_number: &FileNumber,
        target_position: u64,
        payload: &[u8],
    ) -> Result<(), AppendError> {
        let queue = self
            .queues
            .get_mut(queue)
            .ok_or_else(|| MissingQueue(queue.to_string()))?;
        queue.append_record(file_number, target_position, payload, &mut self.arena)
    }

    pub fn contains_queue(&self, queue: &str) -> bool {
        self.queues.contains_key(queue)
    }

    pub fn list_queues(&self) -> impl Iterator<Item = &str> {
        self.queues.keys().map(|queue| queue.as_str())
    }

    /// Ensure that the queue is empty and start_position = next_position.
    ///
    /// If the queue doesn't exist, create it. If it does, but isn't empty or the position doesn't
    /// match, truncate it and make it go forward to the requested position.
    ///
    /// This operation is meant only to rebuild the in memory queue from its on-disk state.
    pub fn ack_position(&mut self, queue_name: &str, next_position: u64) {
        if let Some(queue) = self.queues.get(queue_name) {
            // It is possible for `ack_position` to be called when a queue already exists.
            //
            // For instance, we may have recorded the position of an empty stale queue
            // twice in the same file. Nothing prevents that from happening today.
            //
            // Another possibility is if an IO error occured right after recording position
            // and before deleting files.
            if !queue.is_empty() || queue.next_position() != next_position {
                // if we are here, some updates to the queue were lost/corrupted, but it's no
                // big deal as they were no longer considered part of the active state. We can
                // delete and recreate the queue to put it in the expected state.
                self.queues.remove(queue_name);
                self.queues.insert(
                    queue_name.to_string(),
                    MemQueue::with_next_position(next_position),
                );
            }
        } else {
            // The queue does not exist! Let's create it and set the right `next_position`.
            self.queues.insert(
                queue_name.to_string(),
                MemQueue::with_next_position(next_position),
            );
        }
    }

    /// Returns the position of the last record appended to the queue.
    pub fn last_position(&self, queue: &str) -> Result<Option<u64>, MissingQueue> {
        Ok(self.get_queue(queue)?.last_position())
    }

    /// Returns the last record stored in the queue.
    pub fn last_record(&self, queue: &str) -> Result<Option<Record>, MissingQueue> {
        Ok(self.get_queue(queue)?.last_record(&self.arena))
    }

    pub fn next_position(&self, queue: &str) -> Result<u64, MissingQueue> {
        Ok(self.get_queue(queue)?.next_position())
    }

    /// Removes records up to the supplied `position`,
    /// including the position itself.
    ///
    /// If there are no records `<= position`, the method will
    /// not do anything.
    pub fn truncate(&mut self, queue_id: &str, position: u64) -> Option<usize> {
        let queue = self.queues.get_mut(queue_id)?;
        Some(queue.truncate(position, &mut self.arena))
    }

    /// Return a tuple of (size, capacity) of memory used by the memqueues
    pub fn size(&self) -> (usize, usize) {
        let size = self
            .queues
            .iter()
            .map(|(name, queue)| {
                dbg!(queue.size());
                name.len() + queue.size()
            })
            .sum();

        let capacity = self
            .queues
            .iter()
            .map(|(name, queue)| name.capacity() + queue.capacity())
            .sum::<usize>()
            + self.arena.unused_capacity();

        (size, capacity)
    }
}
