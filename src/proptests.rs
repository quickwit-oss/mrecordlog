use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;

use proptest::prelude::prop;
use proptest::prop_oneof;
use proptest::strategy::{Just, Strategy};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use crate::record::{MultiPlexedRecord, MultiRecord};
use crate::{MultiRecordLog, Serializable};

struct PropTestEnv {
    tempdir: TempDir,
    record_log: MultiRecordLog,
    state: HashMap<&'static str, (Range<u64>, u64)>,
    block_to_write: Vec<u8>,
}

impl PropTestEnv {
    pub async fn new(block_size: usize) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mut record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        record_log.create_queue("q1").await.unwrap();
        record_log.create_queue("q2").await.unwrap();
        let mut state = HashMap::default();
        state.insert("q1", (0..0, 0));
        state.insert("q2", (0..0, 0));
        PropTestEnv {
            tempdir,
            record_log,
            state,
            block_to_write: vec![b'A'; block_size],
        }
    }

    pub async fn apply(&mut self, op: Operation) {
        match op {
            Operation::Reopen => {
                self.reload().await;
            }
            Operation::MultiAppend {
                queue,
                count,
                skip_one_pos,
            } => {
                self.multi_append(queue, count, skip_one_pos).await;
            }
            Operation::RedundantAppend {
                queue,
                skip_one_pos,
            } => {
                self.double_append(queue, skip_one_pos).await;
            }
            Operation::Truncate { queue, pos } => {
                self.truncate(queue, pos).await;
            }
        }
    }

    pub async fn reload(&mut self) {
        self.record_log = MultiRecordLog::open(self.tempdir.path()).await.unwrap();
        for (queue, (_range, count)) in &self.state {
            assert_eq!(
                self.record_log.range(queue, ..).unwrap().count() as u64,
                *count,
            );
        }
    }

    pub async fn double_append(&mut self, queue: &str, skip_one_pos: bool) {
        let state = self.state.get_mut(queue).unwrap();

        let new_pos = state.0.end + skip_one_pos as u64;
        let res = self
            .record_log
            .append_records(queue, Some(new_pos), std::iter::once(&b"BB"[..]))
            .await
            .unwrap()
            .unwrap();

        assert!(self
            .record_log
            .append_records(queue, Some(new_pos), std::iter::once(&b"BB"[..]))
            .await
            .unwrap()
            .is_none());

        assert_eq!(new_pos, res);
        state.0.end = new_pos + 1;
        state.1 += 1;
    }

    pub async fn multi_append(&mut self, queue: &str, count: u64, skip_one_pos: bool) {
        let state = self.state.get_mut(queue).unwrap();

        let new_pos = state.0.end + skip_one_pos as u64;
        let res = self
            .record_log
            .append_records(
                queue,
                Some(new_pos),
                std::iter::repeat(&self.block_to_write[..]).take(count as usize),
            )
            .await
            .unwrap();

        if count != 0 {
            let res = res.unwrap();
            assert_eq!(new_pos + count - 1, res);
            state.0.end = new_pos + count;
            state.1 += count;
        }
    }

    pub async fn truncate(&mut self, queue: &str, pos: u64) {
        let state = self.state.get_mut(queue).unwrap();
        if state.0.contains(&pos) {
            state.0.start = pos + 1;
            state.1 -= self.record_log.truncate(queue, pos).await.unwrap() as u64;
        } else if pos >= state.0.end {
            // advance the queue to the position.
            state.0 = (pos + 1)..(pos + 1);
            state.1 = 0;
            self.record_log.truncate(queue, pos).await.unwrap();
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
        (queue_strategy(), proptest::bool::ANY).prop_map(|(queue, skip_one_pos)| {
            Operation::RedundantAppend {
                queue,
                skip_one_pos,
            }
        }),
        (queue_strategy(), 0u64..10u64).prop_map(|(queue, pos)| Operation::Truncate { queue, pos }),
        (queue_strategy(), 0u64..10u64, proptest::bool::ANY).prop_map(
            |(queue, count, skip_one_pos)| Operation::MultiAppend {
                queue,
                count,
                skip_one_pos
            }
        ),
    ]
}

fn operations_strategy() -> impl Strategy<Value = Vec<Operation>> {
    prop::collection::vec(operation_strategy(), 1..100)
}

fn random_bytevec_strategy(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(proptest::num::u8::ANY, 0..max_len)
}

fn random_multi_record_strategy(
    max_record_count: usize,
    max_len: usize,
) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(random_bytevec_strategy(max_len), 1..max_record_count)
}

#[test]
fn test_scenario_end_on_full_file() {
    use Operation::*;
    let ops = [
        MultiAppend {
            queue: "q1",
            count: 1,
            skip_one_pos: false,
        },
        MultiAppend {
            queue: "q1",
            count: 1,
            skip_one_pos: false,
        },
        MultiAppend {
            queue: "q2",
            count: 1,
            skip_one_pos: false,
        },
        MultiAppend {
            queue: "q2",
            count: 1,
            skip_one_pos: false,
        },
        MultiAppend {
            queue: "q2",
            count: 1,
            skip_one_pos: false,
        },
        Reopen,
        RedundantAppend {
            queue: "q2",
            skip_one_pos: false,
        },
        Reopen,
    ];
    Runtime::new().unwrap().block_on(async {
        // this value is crafted to make so exactly two full files are stored,
        // but no 3rd is created: if anything about the format change, this test
        // will become useless (but won't fail spuriously).
        let mut env = PropTestEnv::new(52381).await;
        for op in ops {
            env.apply(op).await;
        }
    });
}

#[test]
fn test_scenario_big_records() {
    use Operation::*;
    let ops = [
        MultiAppend {
            queue: "q1",
            count: 2,
            skip_one_pos: false,
        },
        MultiAppend {
            queue: "q2",
            count: 4,
            skip_one_pos: false,
        },
        Reopen,
        MultiAppend {
            queue: "q2",
            count: 1,
            skip_one_pos: false,
        },
        Reopen,
        RedundantAppend {
            queue: "q2",
            skip_one_pos: false,
        },
        Reopen,
    ];
    Runtime::new().unwrap().block_on(async {
        let mut env = PropTestEnv::new(1 << 26).await;
        for op in ops {
            env.apply(op).await;
        }
    });
}

fn queue_name_len() -> impl Strategy<Value = usize> {
    prop_oneof![
        100 => 1..10usize,
        1 => 65_534..=65_536usize
    ]
}

fn queue_name_strategy() -> impl Strategy<Value = String> {
    queue_name_len().prop_flat_map(|num_bytes| {
        proptest::collection::vec(proptest::prelude::any::<char>(), num_bytes).prop_map(
            move |chars| {
                let mut s: String = String::new();
                s.extend(chars);
                let boundaries = (0..num_bytes).rev();
                for boundary in boundaries {
                    if s.is_char_boundary(boundary) {
                        s.truncate(boundary);
                        break;
                    }
                }
                s
            },
        )
    })
}

proptest::proptest! {
    #[test]
    fn test_proptest_multirecord((ops, block_size) in (operations_strategy(), 0usize..65535)) {
        Runtime::new().unwrap().block_on(async {
            let mut env = PropTestEnv::new(block_size).await;
            for op in ops {
                env.apply(op).await;
            }
        });
    }

    #[test]
    fn test_proptest_multiplexed_record_roundtrip((kind, queue, position, payload) in
        (0u8..4u8, queue_name_strategy(), proptest::num::u64::ANY, random_multi_record_strategy(64, 65536))) {
        let mut buffer = Vec::new();
        MultiRecord::serialize(payload.iter().map(|p| p.as_ref()), position, &mut buffer);
        let record = match kind {
            0 => MultiPlexedRecord::AppendRecords {
                queue: &queue,
                position,
                records: MultiRecord::new(&buffer).unwrap(),
            },
            1 => MultiPlexedRecord::Truncate {
                queue: &queue,
                position},
            2 => MultiPlexedRecord::RecordPosition {queue: &queue, position},
            3 => MultiPlexedRecord::DeleteQueue {queue: &queue, position},
            4.. => unreachable!(),
        };

        let mut buffer = Vec::new();

        record.serialize(&mut buffer);

        let deser = MultiPlexedRecord::deserialize(&buffer).unwrap();
        assert_eq!(record, deser);
        if let MultiPlexedRecord::AppendRecords { records, .. } = deser {
            assert!(records
                        .map(|record| record.unwrap().1)
                        .zip(payload)
                        .all(|(record, payload)| record == payload));
        }
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Reopen,
    MultiAppend {
        queue: &'static str,
        count: u64,
        skip_one_pos: bool,
    },
    RedundantAppend {
        queue: &'static str,
        skip_one_pos: bool,
    },
    Truncate {
        queue: &'static str,
        pos: u64,
    },
}

#[tokio::test]
async fn test_multi_record() {
    let tempdir = tempfile::tempdir().unwrap();
    eprintln!("dir={tempdir:?}");
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"1"[..])
                .await
                .unwrap(),
            Some(0)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"22"[..])
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
            [(1, Cow::Borrowed(&b"22"[..]))],
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"hello"[..])
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
            [
                (1, Cow::Borrowed(&b"22"[..])),
                (2, Cow::Borrowed(&b"hello"[..])),
            ]
        );
    }
}
