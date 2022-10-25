use std::collections::HashMap;
use std::ops::Range;

use proptest::prelude::prop;
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use crate::record::MultiPlexedRecord;
use crate::{MultiRecordLog, Serializable};

struct PropTestEnv {
    tempdir: TempDir,
    record_log: MultiRecordLog,
    state: HashMap<&'static str, Range<u64>>,
}

impl PropTestEnv {
    pub async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mut record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        record_log.create_queue("q1").await.unwrap();
        record_log.create_queue("q2").await.unwrap();
        let mut state = HashMap::default();
        state.insert("q1", 0..0);
        state.insert("q2", 0..0);
        PropTestEnv {
            tempdir,
            record_log,
            state,
        }
    }

    pub async fn apply(&mut self, op: Operation) {
        match op {
            Operation::Reopen => {
                self.reload().await;
            }
            Operation::Append { queue } => {
                self.append(queue).await;
            }
            Operation::RedundantAppend { queue } => {
                self.double_append(queue).await;
            }
            Operation::Truncate { queue, pos } => {
                self.truncate(queue, pos).await;
            }
        }
    }

    pub async fn reload(&mut self) {
        self.record_log = MultiRecordLog::open(&self.tempdir.path()).await.unwrap();
        for (queue, range) in &self.state {
            assert_eq!(
                self.record_log.range(queue, ..).unwrap().count() as u64,
                range.end - range.start,
            );
        }
    }

    pub async fn append(&mut self, queue: &str) {
        let range = self.state.get_mut(queue).unwrap();

        let res = self
            .record_log
            .append_record(queue, Some(range.end), &[])
            .await
            .unwrap()
            .unwrap();

        assert_eq!(range.end, res);
        range.end += 1;
    }

    pub async fn double_append(&mut self, queue: &str) {
        let range = self.state.get_mut(queue).unwrap();

        let res = self
            .record_log
            .append_record(queue, Some(range.end), &[])
            .await
            .unwrap()
            .unwrap();

        assert!(self
            .record_log
            .append_record(queue, Some(range.end), &[])
            .await
            .unwrap()
            .is_none());

        assert_eq!(range.end, res);
        range.end += 1;
    }

    pub async fn truncate(&mut self, queue: &str, pos: u64) {
        let range = self.state.get_mut(queue).unwrap();
        if range.contains(&pos) {
            // invalid operation
            range.start = pos + 1;
            self.record_log.truncate(queue, pos).await.unwrap();
        } else if pos >= range.end {
            // invalid usage
            self.record_log.truncate(queue, pos).await.unwrap_err();
        } else {
            // should be a no-op
            self.record_log.truncate(queue, pos).await.unwrap();
        }
    }
}

fn queue_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("q1"), Just("q2"),]
}

fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
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

fn random_bytevec_strategy(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(proptest::num::u8::ANY, 0..max_len)
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

    #[test]
    fn test_proptest_multiplexed_record_roundtrip((kind, queue, position, payload) in
        (proptest::num::u8::ANY, random_bytevec_strategy(65536),
        proptest::num::u64::ANY, random_bytevec_strategy(65536))) {
        let queue = match String::from_utf8(queue) {
            Ok(queue) => queue,
            Err(_) => return Ok(()),
        };
        let queue = &queue;
        let record = match kind%4 {
            0 => MultiPlexedRecord::AppendRecord {
                queue,
                position,
                payload: &payload,
            },
            1 => MultiPlexedRecord::Truncate {
                queue,
                position},
            2 => MultiPlexedRecord::Touch {queue, position},
            3 => MultiPlexedRecord::DeleteQueue {queue, position},
            4.. => unreachable!(),
        };

        let mut buffer = Vec::new();

        record.serialize(&mut buffer);

        let deser = MultiPlexedRecord::deserialize(&buffer).unwrap();
        assert_eq!(record, deser);
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Reopen,
    Append { queue: &'static str },
    RedundantAppend { queue: &'static str },
    Truncate { queue: &'static str, pos: u64 },
}

#[tokio::test]
async fn test_multi_record() {
    let tempdir = tempfile::tempdir().unwrap();
    eprintln!("dir={:?}", tempdir);
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"1")
                .await
                .unwrap(),
            Some(0)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"22")
                .await
                .unwrap(),
            Some(1)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.truncate("queue", 0).await.unwrap();
        assert_eq!(
            multi_record_log
                .range("queue", ..)
                .unwrap()
                .collect::<Vec<_>>(),
            [(1, &b"22"[..])],
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"hello")
                .await
                .unwrap(),
            Some(2)
        );
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .range("queue", ..)
                .unwrap()
                .collect::<Vec<_>>(),
            [(1, &b"22"[..]), (2, &b"hello"[..]),]
        );
    }
}
