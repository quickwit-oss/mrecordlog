mod queue;
mod queues;

pub(crate) use self::queue::MemQueue;
pub(crate) use self::queues::MemQueues;

#[cfg(test)]
mod tests;
