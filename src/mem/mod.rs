mod queue;
mod queues;

pub use self::queue::MemQueue;
pub use self::queues::MemQueues;

#[cfg(test)]
mod tests;
