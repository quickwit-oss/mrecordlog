use std::sync::Arc;

use super::*;
use crate::error::{AlreadyExists, AppendError};
use crate::Record;

#[test]
fn test_mem_queues_already_exists() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    assert!(matches!(
        mem_queues.create_queue("droopy", ()),
        Err(AlreadyExists)
    ));
}

#[test]
fn test_mem_queues() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    mem_queues.create_queue("fable", ()).unwrap();
    {
        assert!(mem_queues.append_record("droopy", &(), 0, b"hello").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 1, b"happy").is_ok());
    }

    {
        assert!(mem_queues.append_record("fable", &(), 0, b"maitre").is_ok());
        assert!(mem_queues
            .append_record("fable", &(), 1, b"corbeau")
            .is_ok());
    }

    {
        assert!(mem_queues.append_record("droopy", &(), 2, b"tax").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 3, b"payer").is_ok());
        assert_eq!(
            mem_queues.range("droopy", 0..).unwrap().next(),
            Some(Record::new(0, b"hello"))
        );
        let droopy: Vec<Record> = mem_queues.range("droopy", 1..).unwrap().collect();
        assert_eq!(
            &droopy,
            &[
                Record::new(1, b"happy"),
                Record::new(2, b"tax"),
                Record::new(3, b"payer"),
            ],
        );
    }
    let fable: Vec<Record> = mem_queues.range("fable", 1..).unwrap().collect();
    assert_eq!(&fable, &[Record::new(1, b"corbeau")]);
}

#[test]
fn test_mem_queues_truncate() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    {
        assert!(mem_queues.append_record("droopy", &(), 0, b"hello").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 1, b"happy").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 2, b"tax").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 3, b"payer").is_ok());
        assert!(mem_queues.append_record("droopy", &(), 4, b"!").is_ok());
        mem_queues
            .append_record("droopy", &(), 5, b"payer")
            .unwrap();
    }
    mem_queues.truncate("droopy", ..=3, &());
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(4, b"!"), Record::new(5, b"payer"),]
    );
}

#[test]
fn test_mem_queues_skip_advance() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    assert!(mem_queues.append_record("droopy", &(), 0, b"hello").is_ok());
    assert!(mem_queues.append_record("droopy", &(), 2, b"happy").is_ok());
    assert!(mem_queues.append_record("droopy", &(), 3, b"happy").is_ok());
    assert!(mem_queues
        .append_record("droopy", &(), 1, b"happy")
        .is_err());
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            Record::new(0, b"hello"),
            Record::new(2, b"happy"),
            Record::new(3, b"happy"),
        ]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 1..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(2, b"happy"), Record::new(3, b"happy"),]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 2..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(2, b"happy"), Record::new(3, b"happy"),]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 3..).unwrap().collect();
    assert_eq!(&droopy[..], &[Record::new(3, b"happy")]);
}

#[test]
fn test_mem_queues_append_in_the_past_yield_error() {
    let mut mem_queues: MemQueues<()> = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    assert!(mem_queues.append_record("droopy", &(), 0, b"hello").is_ok());
    assert!(mem_queues.append_record("droopy", &(), 1, b"happy").is_ok());
    assert!(matches!(
        mem_queues.append_record("droopy", &(), 0, b"happy"),
        Err(AppendError::Past)
    ));
}

#[test]
fn test_mem_queues_append_idempotence() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    assert!(mem_queues.append_record("droopy", &(), 0, b"hello").is_ok());
    assert!(matches!(
        mem_queues
            .append_record("droopy", &(), 0, b"different")
            .unwrap_err(),
        AppendError::Past
    ));
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy, &[Record::new(0, b"hello")]);
}

#[test]
fn test_mem_queues_non_zero_first_el() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy", ()).unwrap();
    assert!(mem_queues.append_record("droopy", &(), 5, b"hello").is_ok());
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(droopy, &[Record::new(5, b"hello")]);
}

#[test]
fn test_mem_queues_keep_ref_count() {
    let has_been_dropped = |ref_count: &Arc<usize>| Arc::strong_count(ref_count) == 1;

    let mut mem_queues = MemQueues::default();

    let ref_counts = (0..4).map(|i| Arc::new(i)).collect::<Vec<_>>();

    assert!(ref_counts.iter().all(has_been_dropped));

    mem_queues
        .create_queue("droopy", ref_counts[0].clone())
        .unwrap();
    mem_queues
        .append_record("droopy", &ref_counts[0], 0, b"hello")
        .unwrap();

    assert!(!has_been_dropped(&ref_counts[0]));

    mem_queues
        .append_record("droopy", &ref_counts[0], 1, b"hello")
        .unwrap();

    assert!(!has_been_dropped(&ref_counts[0]));

    mem_queues
        .append_record("droopy", &ref_counts[0], 2, b"hello")
        .unwrap();

    assert!(!has_been_dropped(&ref_counts[0]));

    mem_queues
        .append_record("droopy", &ref_counts[1], 3, b"hello")
        .unwrap();

    assert!(!has_been_dropped(&ref_counts[0]));
    assert!(!has_been_dropped(&ref_counts[1]));

    mem_queues.truncate("droopy", ..=1, &ref_counts[1]);

    assert!(!has_been_dropped(&ref_counts[0]));
    assert!(!has_been_dropped(&ref_counts[1]));

    mem_queues
        .append_record("droopy", &ref_counts[2], 4, b"hello")
        .unwrap();

    assert!(!has_been_dropped(&ref_counts[0]));
    assert!(!has_been_dropped(&ref_counts[1]));
    assert!(!has_been_dropped(&ref_counts[2]));

    mem_queues.truncate("droopy", ..=3, &ref_counts[2]);

    assert!(has_been_dropped(&ref_counts[0]));
    assert!(has_been_dropped(&ref_counts[1]));
    assert!(!has_been_dropped(&ref_counts[2]));

    mem_queues.truncate("droopy", ..=4, &ref_counts[3]);

    let empty_queues = mem_queues.empty_queues().collect::<Vec<_>>();
    assert_eq!(empty_queues.len(), 1);
    assert_eq!(empty_queues[0].0, "droopy");

    mem_queues.ack_position("droopy", 5, &ref_counts[3]);

    assert!(has_been_dropped(&ref_counts[2]));
}
