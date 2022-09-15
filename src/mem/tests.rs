use super::*;
use crate::error::{AlreadyExists, AppendError};
use crate::rolling::FileNumber;

#[test]
fn test_mem_queues_already_exists() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(matches!(
        mem_queues.create_queue("droopy"),
        Err(AlreadyExists)
    ));
}

#[test]
fn test_mem_queues() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    mem_queues.create_queue("fable").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 0, b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 1, b"happy")
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 0, b"maitre")
            .is_ok());
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 1, b"corbeau")
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 2, b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 3, b"payer")
            .is_ok());
        assert_eq!(
            mem_queues.range("droopy", 0..).unwrap().next(),
            Some((0, &b"hello"[..]))
        );
        let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 1..).unwrap().collect();
        assert_eq!(
            &droopy,
            &[(1, &b"happy"[..]), (2, &b"tax"[..]), (3, &b"payer"[..])],
        );
    }
    let fable: Vec<(u64, &[u8])> = mem_queues.range("fable", 1..).unwrap().collect();
    assert_eq!(&fable, &[(1, &b"corbeau"[..])]);
}

#[test]
fn test_mem_queues_truncate() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &1.into(), 0, b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 1, b"happy")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 2, b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 3, b"payer")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 4, b"!")
            .is_ok());
        mem_queues
            .append_record("droopy", &1.into(), 5, b"payer")
            .unwrap();
    }
    mem_queues.truncate("droopy", 3);
    let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy[..], &[(4, &b"!"[..]), (5, &b"payer"[..]),]);
}

#[test]
fn test_mem_queues_skip_yield_error() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    let append_res = mem_queues.append_record("droopy", &1.into(), 2, b"happy");
    assert!(matches!(append_res, Err(AppendError::Future)));
    assert!(matches!(
        mem_queues.append_record("droopy", &1.into(), 3, b"happy"),
        Err(AppendError::Future)
    ));
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .is_ok());
    let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy[..], &[(0, &b"hello"[..]), (1, &b"happy"[..])]);
}

#[test]
fn test_mem_queues_append_in_the_past_yield_error() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .is_ok());
    assert!(matches!(
        mem_queues.append_record("droopy", &1.into(), 0, b"happy"),
        Err(AppendError::Past)
    ));
}

#[test]
fn test_mem_queues_append_nilpotence() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    assert!(matches!(
        mem_queues
            .append_record("droopy", &1.into(), 0, b"different")
            .unwrap_err(),
        AppendError::Past
    ));
    let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy, &[(0, &b"hello"[..])]);
}

#[test]
fn test_mem_queues_non_zero_first_el() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 5, b"hello")
        .is_ok());
    let droopy: Vec<(u64, &[u8])> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(droopy, &[(5, &b"hello"[..])]);
}
