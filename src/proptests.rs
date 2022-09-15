use std::collections::HashMap;
use std::ops::Range;

use proptest::prelude::prop;
use proptest::prop_oneof;
use proptest::strategy::Just;
use proptest::strategy::Strategy;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use crate::MultiRecordLog;

struct PropTestEnv {
    tempdir: TempDir,
    record_log: MultiRecordLog,
    state: HashMap<&'static str, Range<usize>>,
}

impl PropTestEnv {
    pub async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mut record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        record_log.create_queue("q1").await.unwrap();
        record_log.create_queue("q2").await.unwrap();
        let mut state: HashMap<&'static str, Range<usize>> = HashMap::default();
        state.insert("q1", 0..0);
        state.insert("q2", 0..0);
        PropTestEnv {
            tempdir,
            record_log,
            state: HashMap::default(),
        }
    }

    pub async fn apply(&mut self, op: Operation) {
        // match op {
        //     Operation::PartialWrite => {

        //     },
        //     Operation::Reopen => {
        //         self.reload().await.unwrap();
        //     },
        //     Operation::Append { queue } => {
        //         self.record_log.append_record(queue, position_opt, payload)
        //     },
        //     Operation::RedundantAppend { queue } => {

        //     },
        //     Operation::Truncate { queue, pos } => {

        //     },
        // }
    }

    pub async fn reload(&mut self) {
        self.record_log = MultiRecordLog::open(&self.tempdir.path()).await.unwrap();
    }

    pub async fn append(&mut self, queue: &str, pos: u64) {
        self.record_log
            .append_record(queue, None, &[])
            .await
            .unwrap();
    }

    pub async fn truncate(&mut self, queue: &str, pos: u64) {
        self.record_log.truncate(queue, pos).await.unwrap();
    }
}

fn queue_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("q1"), Just("q2"),]
}

fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
        Just(Operation::PartialWrite),
        Just(Operation::Reopen),
        queue_strategy().prop_map(|queue| Operation::Append { queue }),
        queue_strategy().prop_map(|queue| Operation::RedundantAppend { queue }),
        (queue_strategy(), (0u64..10u64))
            .prop_map(|(queue, pos)| Operation::Truncate { queue, pos })
    ]
}

fn operations_strategy() -> impl Strategy<Value = Vec<Operation>> {
    prop::collection::vec(operation_strategy(), 1..100)
}

proptest::proptest! {
    #[test]
    fn test_proptest_multirecord(ops in operations_strategy()) {
        Runtime::new().unwrap().block_on(async {
            let mut env = PropTestEnv::new().await;
            for op in ops {
                env.apply(op).await;
            }
        });
    }
}

#[derive(Debug, Clone)]
enum Operation {
    PartialWrite,
    Reopen,
    Append { queue: &'static str },
    RedundantAppend { queue: &'static str },
    Truncate { queue: &'static str, pos: u64 },
}

#[tokio::test]
async fn test_multi_record() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        // multi_record_log.create_queue("queue").await.unwrap();
        // assert_eq!(
        //     multi_record_log
        //         .append_record("queue", None, b"1")
        //         .await
        //         .unwrap(),
        //     Some(0)
        // );
    }
    // {
    //     let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
    //     assert_eq!(
    //         multi_record_log
    //             .append_record("queue", None, b"2")
    //             .await
    //             .unwrap(),
    //         Some(1)
    //     );
    //     assert_eq!(multi_record_log.first_last_files(), (0, 2));
    // }
    // {
    //     let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
    //     multi_record_log.truncate("queue", 1).await.unwrap();
    //     assert_eq!(multi_record_log.first_last_files(), (3, 3));
    // }
    // {
    //     let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
    //     assert_eq!(
    //         multi_record_log
    //             .append_record("queue", None, b"hello")
    //             .await
    //             .unwrap(),
    //         Some(2)
    //     );
    // }
}
