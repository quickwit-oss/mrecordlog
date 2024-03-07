use std::borrow::Cow;

use bytes::Buf;

use crate::{MultiRecordLog, Record};

fn read_all_records<'a>(multi_record_log: &'a MultiRecordLog, queue: &str) -> Vec<Cow<'a, [u8]>> {
    let mut records = Vec::new();
    let mut next_pos = u64::default();
    for Record { position, payload } in multi_record_log.range(queue, next_pos..).unwrap() {
        assert_eq!(position, next_pos);
        records.push(payload);
        next_pos += 1;
    }
    records
}

#[test]
fn test_multi_record_log_new() {
    let tempdir = tempfile::tempdir().unwrap();
    MultiRecordLog::open(tempdir.path()).unwrap();
}

#[test]
fn test_multi_record_log_create_queue() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[test]
fn test_multi_record_log_create_queue_after_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[test]
fn test_multi_record_log_simple() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue", None, &b"happy"[..])
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log_chained() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record(
                "queue",
                None,
                b"world".chain(&b" "[..]).chain(&b"order"[..]),
            )
            .unwrap();
        multi_record_log
            .append_record(
                "queue",
                None,
                b"nice"[..].chain(&b" "[..]).chain(&b"day"[..]),
            )
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"world order".as_slice(), b"nice day".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue", None, &b"happy"[..])
            .unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue1").unwrap();
        multi_record_log.create_queue("queue2").unwrap();
        multi_record_log
            .append_record("queue1", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue2", None, &b"maitre"[..])
            .unwrap();
        multi_record_log
            .append_record("queue1", None, &b"happy"[..])
            .unwrap();
        multi_record_log
            .append_record("queue1", None, &b"tax"[..])
            .unwrap();
        multi_record_log
            .append_record("queue2", None, &b"corbeau"[..])
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
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log
            .append_record("queue1", None, &b"bubu"[..])
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

#[test]
fn test_multi_record_position_known_after_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"1"[..])
                .unwrap(),
            Some(0)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"2"[..])
                .unwrap(),
            Some(1)
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.truncate("queue", 1).unwrap();
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"hello"[..])
                .unwrap(),
            Some(2)
        );
    }
}

#[test]
fn test_multi_insert_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_records(
                    "queue",
                    None,
                    [b"1", b"2", b"3", b"4"].into_iter().map(|r| r.as_slice())
                )
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[
                b"1".as_slice(),
                b"2".as_slice(),
                b"3".as_slice(),
                b"4".as_slice()
            ]
        );

        multi_record_log.truncate("queue", 0).unwrap();
        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"2".as_slice(), b"3".as_slice(), b"4".as_slice()]
        )
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.truncate("queue", 1).unwrap();

        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"3".as_slice(), b"4".as_slice()]
        )
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"3".as_slice(), b"4".as_slice()]
        )
    }
}

#[test]
fn test_truncate_range_correct_pos() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"1"[..])
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"2"[..])
                .unwrap(),
            Some(1)
        );
        multi_record_log.truncate("queue", 1).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"3"[..])
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            multi_record_log
                .range("queue", ..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2u64, b"3")]
        );

        assert_eq!(
            multi_record_log
                .range("queue", 2..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2, b"3")]
        );

        use std::ops::Bound;
        assert_eq!(
            multi_record_log
                .range("queue", (Bound::Excluded(1), Bound::Unbounded))
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2, b"3")]
        );
    }
}

#[test]
fn test_multi_record_size() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(multi_record_log.memory_usage().size, 0);
        assert_eq!(multi_record_log.memory_usage().capacity, 0);

        multi_record_log.create_queue("queue").unwrap();
        let size_mem_create = multi_record_log.memory_usage();
        assert!(size_mem_create.size > 0);
        assert!(size_mem_create.capacity >= size_mem_create.size);

        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        let size_mem_append = multi_record_log.memory_usage();
        assert!(size_mem_append.size > size_mem_create.size);
        assert!(size_mem_append.capacity >= size_mem_append.size);
        assert!(size_mem_append.capacity >= size_mem_create.capacity);

        multi_record_log.truncate("queue", 0).unwrap();
        let size_mem_truncate = multi_record_log.memory_usage();
        assert!(size_mem_truncate.size < size_mem_append.size);
    }
}

#[test]
fn test_open_corrupted() {
    // a single frame is 32k. We write more than 2 frames worth of data, corrupt one,
    // and verify we still read more than half the records successfully.
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();

        // 8192 * 8bytes = 64k without overhead.
        for i in 0..8192 {
            multi_record_log
                .append_record("queue", Some(i), format!("{i:08}").as_bytes())
                .unwrap();
        }
    }
    {
        use std::fs::OpenOptions;
        use std::io::*;
        // corrupt the file
        let file = std::fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|file| !file.file_name().to_str().unwrap().starts_with('.'))
            .unwrap();

        let mut file = OpenOptions::new().write(true).open(file.path()).unwrap();
        // jump somewhere in the middle
        file.seek(SeekFrom::Start(10240)).unwrap();
        file.write_all(b"this will corrupt the file. Good :-)")
            .unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();

        let mut count = 0;
        for Record { position, payload } in multi_record_log.range("queue", ..).unwrap() {
            assert_eq!(payload, format!("{position:08}").as_bytes());
            count += 1;
        }
        assert!(count > 4096);
    }
}

#[test]
fn test_create_twice() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue1").unwrap();
        multi_record_log
            .append_record("queue1", None, &b"hello"[..])
            .unwrap();
        multi_record_log.create_queue("queue1").unwrap_err();
        assert_eq!(multi_record_log.range("queue1", ..).unwrap().count(), 1);
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(multi_record_log.range("queue1", ..).unwrap().count(), 1);
    }
}

#[test]
fn test_last_position() {
    let tempdir = tempfile::tempdir().unwrap();

    let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    multi_record_log.last_position("queue1").unwrap_err();

    multi_record_log.create_queue("queue1").unwrap();
    let last_pos = multi_record_log.last_position("queue1").unwrap();
    assert!(last_pos.is_none());

    multi_record_log
        .append_record("queue1", None, &b"hello"[..])
        .unwrap();

    let last_pos = multi_record_log.last_position("queue1").unwrap().unwrap();
    assert_eq!(last_pos, 0);

    multi_record_log.truncate("queue1", 0).unwrap();

    let last_pos = multi_record_log.last_position("queue1").unwrap().unwrap();
    assert_eq!(last_pos, 0);
}

#[test]
fn test_last_record() {
    let tempdir = tempfile::tempdir().unwrap();

    let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    multi_record_log.last_position("queue1").unwrap_err();

    multi_record_log.create_queue("queue1").unwrap();
    let last_record = multi_record_log.last_position("queue1").unwrap();
    assert!(last_record.is_none());

    multi_record_log
        .append_record("queue1", None, &b"hello"[..])
        .unwrap();

    let Record { position, payload } = multi_record_log.last_record("queue1").unwrap().unwrap();
    assert_eq!(position, 0);
    assert_eq!(payload, &b"hello"[..]);

    multi_record_log.truncate("queue1", 0).unwrap();

    let last_record = multi_record_log.last_record("queue1").unwrap();
    assert!(last_record.is_none());
}
