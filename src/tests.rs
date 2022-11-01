use crate::MultiRecordLog;

fn read_all_records<'a>(multi_record_log: &'a MultiRecordLog, queue: &str) -> Vec<&'a [u8]> {
    let mut records = Vec::new();
    let mut next_pos = u64::default();
    for (pos, payload) in multi_record_log.range(queue, next_pos..).unwrap() {
        assert_eq!(pos, next_pos);
        records.push(payload);
        next_pos += 1;
    }
    records
}

#[tokio::test]
async fn test_multi_record_log_new() {
    let tempdir = tempfile::tempdir().unwrap();
    MultiRecordLog::open(tempdir.path()).await.unwrap();
}

#[tokio::test]
async fn test_multi_record_log_create_queue() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[tokio::test]
async fn test_multi_record_log_create_queue_after_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[tokio::test]
async fn test_multi_record_log_simple() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        multi_record_log
            .append_record("queue", None, b"hello")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue", None, b"happy")
            .await
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[tokio::test]
async fn test_multi_record_log_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue").await.unwrap();
        multi_record_log
            .append_record("queue", None, b"hello")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue", None, b"happy")
            .await
            .unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[tokio::test]
async fn test_multi_record_log() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.create_queue("queue1").await.unwrap();
        multi_record_log.create_queue("queue2").await.unwrap();
        multi_record_log
            .append_record("queue1", None, b"hello")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue2", None, b"maitre")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue1", None, b"happy")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue1", None, b"tax")
            .await
            .unwrap();
        multi_record_log
            .append_record("queue2", None, b"corbeau")
            .await
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[b"hello".as_slice(), b"happy".as_slice(), b"tax".as_slice()]
        );
        assert_eq!(
            &read_all_records(&multi_record_log, "queue2"),
            &[b"maitre".as_slice(), b"corbeau".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log
            .append_record("queue1", None, b"bubu")
            .await
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[
                b"hello".as_slice(),
                b"happy".as_slice(),
                b"tax".as_slice(),
                b"bubu".as_slice()
            ]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[tokio::test]
async fn test_multi_record_position_known_after_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
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
                .append_record("queue", None, b"2")
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).await.unwrap();
        multi_record_log.truncate("queue", 1).await.unwrap();
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
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
}

#[tokio::test]
async fn test_truncate_range_correct_pos() {
    let tempdir = tempfile::tempdir().unwrap();
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
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"2")
                .await
                .unwrap(),
            Some(1)
        );
        multi_record_log.truncate("queue", 1).await.unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, b"3")
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            multi_record_log
                .range("queue", ..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[(2, &b"3"[..])]
        );

        assert_eq!(
            multi_record_log
                .range("queue", 2..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[(2, &b"3"[..])]
        );

        use std::ops::Bound;
        assert_eq!(
            multi_record_log
                .range("queue", (Bound::Excluded(1), Bound::Unbounded))
                .unwrap()
                .collect::<Vec<_>>(),
            &[(2, &b"3"[..])]
        );
    }
}
