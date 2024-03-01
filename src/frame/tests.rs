use std::io;

use crate::block_read_write::{ArrayReader, VecBlockWriter};
use crate::frame::header::{FrameType, HEADER_LEN};
use crate::frame::{FrameReader, FrameWriter, ReadFrameError};
use crate::BLOCK_NUM_BYTES;

#[test]
fn test_frame_simple() {
    let block_writer = {
        let wrt: VecBlockWriter = VecBlockWriter::default();
        let mut frame_writer = FrameWriter::create(wrt);
        frame_writer
            .write_frame(FrameType::First, &b"abc"[..])
            .unwrap();
        frame_writer
            .write_frame(FrameType::Middle, &b"de"[..])
            .unwrap();
        frame_writer
            .write_frame(FrameType::Last, &b"fgh"[..])
            .unwrap();
        frame_writer.flush(false).unwrap();
        frame_writer.into_writer()
    };
    let buffer: Vec<u8> = block_writer.into();
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buffer[..]));
    let read_frame_res = frame_reader.read_frame();
    assert_eq!(read_frame_res.unwrap(), (FrameType::First, &b"abc"[..]));
    assert_eq!(
        frame_reader.read_frame().unwrap(),
        (FrameType::Middle, &b"de"[..])
    );
    assert_eq!(
        frame_reader.read_frame().unwrap(),
        (FrameType::Last, &b"fgh"[..])
    );
    assert!(matches!(
        frame_reader.read_frame().unwrap_err(),
        ReadFrameError::NotAvailable
    ));
}

#[test]
fn test_frame_corruption_in_payload() -> io::Result<()> {
    let mut buf: Vec<u8> = {
        let mut frame_writer = FrameWriter::create(VecBlockWriter::default());
        frame_writer.write_frame(FrameType::First, &b"abc"[..])?;
        frame_writer.flush(false)?;
        frame_writer.write_frame(FrameType::Middle, &b"de"[..])?;
        frame_writer.flush(false)?;
        frame_writer.into_writer().into()
    };
    buf[8] = 0u8;
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buf[..]));
    assert!(matches!(
        frame_reader.read_frame(),
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame(),
        Ok((FrameType::Middle, b"de"))
    ));
    Ok(())
}

fn repeat_empty_frame_util(repeat: usize) -> Vec<u8> {
    let mut frame_writer = FrameWriter::create(VecBlockWriter::default());
    for _ in 0..repeat {
        frame_writer.write_frame(FrameType::Full, &b""[..]).unwrap();
    }
    frame_writer.flush(false).unwrap();
    frame_writer.into_writer().into()
}

#[test]
fn test_simple_multiple_blocks() -> io::Result<()> {
    let num_frames = 1 + BLOCK_NUM_BYTES / HEADER_LEN;
    let buffer = repeat_empty_frame_util(num_frames);
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buffer[..]));
    for _ in 0..num_frames {
        let read_frame_res = frame_reader.read_frame();
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame(),
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}

#[test]
fn test_multiple_blocks_corruption_on_length() -> io::Result<()> {
    // We end up with 4681 frames on the first block.
    // 1 frame on the second block
    let num_frames = 1 + crate::BLOCK_NUM_BYTES / HEADER_LEN;
    let mut buffer = repeat_empty_frame_util(num_frames);
    buffer[2000 * HEADER_LEN + 5] = 255u8;
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buffer[..]));
    for _ in 0..2000 {
        let read_frame_res = frame_reader.read_frame();
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame(),
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame(),
        Ok((FrameType::Full, &[]))
    ));
    assert!(matches!(
        frame_reader.read_frame(),
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}
