use super::*;
use crate::{BlockRead, BlockWrite, BLOCK_NUM_BYTES};

#[tokio::test]
async fn test_read_write() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut buffer = [0u8; BLOCK_NUM_BYTES];
    {
        let rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
        assert!(&rolling_reader.block().iter().all(|&b| b == 0));
        let mut writer: RollingWriter = rolling_reader.into_writer().await.unwrap();
        buffer.fill(0u8);
        writer.write(&buffer[..]).await.unwrap();
        buffer.fill(1u8);
        writer.write(&buffer[..]).await.unwrap();
        buffer.fill(2u8);
        writer.write(&buffer[..]).await.unwrap();
        writer.flush().await.unwrap();
    }
    let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
    assert!(rolling_reader.block().iter().all(|&b| b == 0));
    assert!(rolling_reader.next_block().await.unwrap());
    assert!(rolling_reader.block().iter().all(|&b| b == 1));
    assert!(rolling_reader.next_block().await.unwrap());
    assert!(rolling_reader.block().iter().all(|&b| b == 2));
}

#[tokio::test]
async fn test_read_write_2nd_block() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut buffer = [0u8; BLOCK_NUM_BYTES];
    {
        let rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
        let mut writer: RollingWriter = rolling_reader.into_writer().await.unwrap();
        for i in 1..=10 {
            buffer.fill(i);
            writer.write(&buffer[..]).await.unwrap();
        }
        writer.flush().await.unwrap();
    }
    {
        let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
        assert!(rolling_reader.block().iter().all(|&b| b == 1));
        assert!(rolling_reader.next_block().await.unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 2));
        assert!(rolling_reader.next_block().await.unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 3));
        let mut writer: RollingWriter = rolling_reader.into_writer().await.unwrap();
        for i in 13..=23 {
            buffer.fill(i);
            writer.write(&buffer[..]).await.unwrap();
        }
        writer.flush().await.unwrap();
    }
    {
        let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
        assert!(rolling_reader.block().iter().all(|&b| b == 1));
        assert!(rolling_reader.next_block().await.unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 2));
        for i in 13..=23 {
            assert!(rolling_reader.next_block().await.unwrap());
            assert!(rolling_reader.block().iter().all(|&b| b == i));
        }
    }
}

#[tokio::test]
async fn test_directory_single_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    {
        let directory = Directory::open(tmp_dir.path()).await.unwrap();
        let first_file = directory.first_file_number();
        assert_eq!(first_file.unroll(&directory.files), &[0]);
    }
    let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).await.unwrap();
    for _ in 0..NUM_BLOCKS_PER_FILE - 1 {
        assert!(rolling_reader.next_block().await.unwrap());
    }
    assert!(!rolling_reader.next_block().await.unwrap());
}

#[tokio::test]
async fn test_directory_simple() {
    let tmp_dir = tempfile::tempdir().unwrap();
    {
        let mut writer: RollingWriter = RollingReader::open(tmp_dir.path())
            .await
            .unwrap()
            .into_writer()
            .await
            .unwrap();
        let buf = vec![1u8; FRAME_NUM_BYTES as usize];
        for _ in 0..(NUM_BLOCKS_PER_FILE + 1) {
            writer.write(&buf).await.unwrap();
        }
    }
    {
        let directory = Directory::open(tmp_dir.path()).await.unwrap();
        let first_file: &FileNumber = directory.first_file_number();
        assert_eq!(first_file.unroll(&directory.files), &[0, 1]);
    }
}

#[tokio::test]
async fn test_directory_truncate() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_0: FileNumber;
    let file_1: FileNumber;
    let file_2: FileNumber;
    let file_3: FileNumber;
    {
        let reader = RollingReader::open(tmp_dir.path()).await.unwrap();
        file_0 = reader.current_file().clone();
        assert!(!file_0.can_be_deleted());
        let mut writer: RollingWriter = reader.into_writer().await.unwrap();
        let buf = vec![1u8; FRAME_NUM_BYTES as usize];
        assert_eq!(&writer.current_file().unroll(&writer.directory.files), &[0]);
        for _ in 0..NUM_BLOCKS_PER_FILE + 1 {
            writer.write(&buf).await.unwrap();
        }
        assert_eq!(&writer.list_file_numbers(), &[0, 1]);
        file_1 = writer.current_file().clone();
        assert_eq!(file_1.file_number(), 1);
        for _ in 0..NUM_BLOCKS_PER_FILE {
            writer.write(&buf).await.unwrap();
        }
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2]);
        file_2 = writer.current_file().clone();
        assert_eq!(file_2.file_number(), 2);
        for _ in 0..NUM_BLOCKS_PER_FILE {
            writer.write(&buf).await.unwrap();
        }
        file_3 = writer.current_file().clone();
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2, 3]);
        assert!(!file_0.can_be_deleted());
        drop(file_1);
        writer.gc().await.unwrap();
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2, 3]);
        drop(file_0);
        writer.gc().await.unwrap();
        assert_eq!(&writer.list_file_numbers(), &[2, 3]);
        drop(file_2);
        writer.gc().await.unwrap();
        assert_eq!(&writer.list_file_numbers(), &[3]);
        drop(file_3);
        writer.gc().await.unwrap();
        assert_eq!(&writer.list_file_numbers(), &[3]);
    }
}
