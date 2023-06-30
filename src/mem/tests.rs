use std::borrow::Cow;

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

#[tokio::test]
async fn test_mem_queues() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    mem_queues.create_queue("fable").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 0, b"hello")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 1, b"happy")
            .await
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 0, b"maitre")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 1, b"corbeau")
            .await
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 2, b"tax")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 3, b"payer")
            .await
            .is_ok());
        assert_eq!(
            mem_queues.range("droopy", 0..).unwrap().next(),
            Some((0, Cow::Borrowed(&b"hello"[..])))
        );
        let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 1..).unwrap().collect();
        assert_eq!(
            &droopy,
            &[
                (1, Cow::Borrowed(&b"happy"[..])),
                (2, Cow::Borrowed(&b"tax"[..])),
                (3, Cow::Borrowed(&b"payer"[..]))
            ],
        );
    }
    let fable: Vec<(u64, Cow<[u8]>)> = mem_queues.range("fable", 1..).unwrap().collect();
    assert_eq!(&fable, &[(1, Cow::Borrowed(&b"corbeau"[..]))]);
}

#[tokio::test]
async fn test_mem_queues_truncate() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &1.into(), 0, b"hello")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 1, b"happy")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 2, b"tax")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 3, b"payer")
            .await
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 4, b"!")
            .await
            .is_ok());
        mem_queues
            .append_record("droopy", &1.into(), 5, b"payer")
            .await
            .unwrap();
    }
    mem_queues.truncate("droopy", 3).await;
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            (4, Cow::Borrowed(&b"!"[..])),
            (5, Cow::Borrowed(&b"payer"[..])),
        ]
    );
}

#[tokio::test]
async fn test_mem_queues_skip_advance() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .await
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 2, b"happy")
        .await
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 3, b"happy")
        .await
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .await
        .is_err());
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            (0, Cow::Borrowed(&b"hello"[..])),
            (2, Cow::Borrowed(&b"happy"[..])),
            (3, Cow::Borrowed(&b"happy"[..])),
        ]
    );
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 1..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            (2, Cow::Borrowed(&b"happy"[..])),
            (3, Cow::Borrowed(&b"happy"[..])),
        ]
    );
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 2..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            (2, Cow::Borrowed(&b"happy"[..])),
            (3, Cow::Borrowed(&b"happy"[..])),
        ]
    );
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 3..).unwrap().collect();
    assert_eq!(&droopy[..], &[(3, Cow::Borrowed(&b"happy"[..])),]);
}

#[tokio::test]
async fn test_mem_queues_append_in_the_past_yield_error() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .await
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .await
        .is_ok());
    assert!(matches!(
        mem_queues
            .append_record("droopy", &1.into(), 0, b"happy")
            .await,
        Err(AppendError::Past)
    ));
}

#[tokio::test]
async fn test_mem_queues_append_idempotence() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .await
        .is_ok());
    assert!(matches!(
        mem_queues
            .append_record("droopy", &1.into(), 0, b"different")
            .await
            .unwrap_err(),
        AppendError::Past
    ));
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy, &[(0, Cow::Borrowed(&b"hello"[..]))]);
}

#[tokio::test]
async fn test_mem_queues_non_zero_first_el() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 5, b"hello")
        .await
        .is_ok());
    let droopy: Vec<(u64, Cow<[u8]>)> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(droopy, &[(5, Cow::Borrowed(&b"hello"[..]))]);
}

#[tokio::test]
async fn test_mem_queues_keep_filenum() {
    let mut mem_queues = MemQueues::default();

    let files = (0..4).map(FileNumber::for_test).collect::<Vec<_>>();

    assert!(files.iter().all(FileNumber::can_be_deleted));

    mem_queues.create_queue("droopy").unwrap();
    mem_queues
        .append_record("droopy", &files[0], 0, b"hello")
        .await
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[0], 1, b"hello")
        .await
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[0], 2, b"hello")
        .await
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[1], 3, b"hello")
        .await
        .unwrap();

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());

    mem_queues.truncate("droopy", 1).await;

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[2], 4, b"hello")
        .await
        .unwrap();

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());
    assert!(!files[2].can_be_deleted());

    mem_queues.truncate("droopy", 3).await;

    assert!(files[0].can_be_deleted());
    assert!(files[1].can_be_deleted());
    assert!(!files[2].can_be_deleted());

    mem_queues.truncate("droopy", 4).await;

    let empty_queues = mem_queues.empty_queues().collect::<Vec<_>>();
    assert_eq!(empty_queues.len(), 1);
    assert_eq!(empty_queues[0].0, "droopy");

    mem_queues.ack_position("droopy", 5);

    assert!(files[2].can_be_deleted());
}
