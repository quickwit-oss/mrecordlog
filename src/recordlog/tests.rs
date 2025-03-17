use super::{RecordReader, RecordWriter};
use crate::block_read_write::ArrayReader;
use crate::error::ReadRecordError;
use crate::frame::HEADER_LEN;
use crate::{PersistAction, BLOCK_NUM_BYTES};

#[test]
fn test_no_data() {
    let data = vec![0u8; BLOCK_NUM_BYTES * 4];
    let mut reader = RecordReader::open(ArrayReader::from(&data[..]));
    let mut session = reader.start_session();
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
}

#[test]
fn test_empty_record() {
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    writer.write_record("", &mut session).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let mut session = reader.start_session();
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), Some(""));
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
}

#[test]
fn test_simple_record() {
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    let record = "hello";
    writer.write_record(record, &mut session).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let mut session = reader.start_session();
    assert!(matches!(
        reader.read_record::<&str>(&mut session),
        Ok(Some("hello"))
    ));
    assert!(matches!(reader.read_record::<&str>(&mut session), Ok(None)));
}

fn make_long_entry(len: usize) -> String {
    "A".repeat(len)
}

#[test]
fn test_spans_over_more_than_one_block() {
    let long_entry: String = make_long_entry(80_000);
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    writer
        .write_record(long_entry.as_str(), &mut session)
        .unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let mut session = reader.start_session();
    let record_payload: &str = reader.read_record(&mut session).unwrap().unwrap();
    assert_eq!(record_payload, &long_entry);
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
}

#[test]
fn test_block_requires_padding() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN - 1 - 8);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    writer
        .write_record(long_record.as_str(), &mut session)
        .unwrap();
    writer.write_record(short_record, &mut session).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buffer: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
    let mut session = reader.start_session();
    assert_eq!(
        reader.read_record::<&str>(&mut session).unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(
        reader.read_record::<&str>(&mut session).unwrap(),
        Some(short_record)
    );
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
}

#[test]
fn test_first_chunk_empty() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    writer.write_record(&long_record[..], &mut session).unwrap();
    writer.write_record(short_record, &mut session).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let mut session = reader.start_session();
    assert_eq!(
        reader.read_record::<&str>(&mut session).unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(
        reader.read_record::<&str>(&mut session).unwrap(),
        Some(short_record)
    );
    assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
}

#[test]
fn test_behavior_upon_corruption() {
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{i}")).collect();
    let mut writer = RecordWriter::in_memory();
    let mut session = writer.start_session().unwrap();
    for record in &records {
        writer.write_record(record.as_str(), &mut session).unwrap();
    }
    writer.persist(PersistAction::Flush).unwrap();
    let mut buffer: Vec<u8> = writer.into_writer().into();
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        let mut session = reader.start_session();
        for record in &records {
            assert_eq!(
                reader.read_record::<&str>(&mut session).unwrap(),
                Some(record.as_str())
            );
        }
        assert_eq!(reader.read_record::<&str>(&mut session).unwrap(), None);
    }
    // Introducing a corruption.
    buffer[1_000] = 3;
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        let mut session = reader.start_session();
        for record in &records[0..72] {
            // bug at i=72
            assert_eq!(
                reader.read_record::<&str>(&mut session).unwrap(),
                Some(record.as_str())
            );
        }
        assert!(matches!(
            reader.read_record::<&str>(&mut session),
            Err(ReadRecordError::Corruption)
        ));
    }
}
