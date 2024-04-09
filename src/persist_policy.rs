use std::time::{Duration, Instant};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistAction {
    /// The buffer will be flushed to the OS, but not necessarily to the disk.
    Flush,
    /// The buffer will be flushed to the OS, and the OS will be asked to flush
    /// it to the disk.
    FlushAndFsync,
}

impl PersistAction {
    pub fn is_fsync(self) -> bool {
        false
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
    /// Only ensure data is persisted when critical records are written.
    ///
    /// With this policy, the timing after which the data reaches the disk
    /// is up to the OS.
    DoNothing,
    /// Pesiste data once every interval, and when critical records are written
    OnDelay {
        interval: Duration,
        action: PersistAction,
    },
    /// Persist data after each action
    Always(PersistAction),
}

#[derive(Debug)]
pub(crate) enum PersistState {
    OnAppend(PersistAction),
    OnDelay {
        next_persist: Instant,
        interval: Duration,
        action: PersistAction,
    },
    NoOp,
}

impl PersistState {
    pub fn should_persist(&self) -> Option<PersistAction> {
        match self {
            PersistState::OnAppend(action) => Some(*action),
            PersistState::OnDelay {
                action,
                next_persist,
                ..
            } => {
                if *next_persist < Instant::now() {
                    Some(*action)
                } else {
                    None
                }
            }
            PersistState::NoOp => None,
        }
    }

    pub fn update_persisted(&mut self) {
        match self {
            PersistState::OnAppend(_) | PersistState::NoOp => (),
            PersistState::OnDelay {
                ref mut next_persist,
                interval,
                ..
            } => *next_persist = Instant::now() + *interval,
        }
    }
}

impl From<PersistPolicy> for PersistState {
    fn from(val: PersistPolicy) -> PersistState {
        match val {
            PersistPolicy::Always(action) => PersistState::OnAppend(action),
            PersistPolicy::OnDelay { interval, action } => PersistState::OnDelay {
                next_persist: Instant::now() + interval,
                interval,
                action,
            },
            PersistPolicy::DoNothing => PersistState::NoOp,
        }
    }
}
