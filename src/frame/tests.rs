use std::io;

use crate::block_read_write::{ArrayReader, VecBlockWriter};
use crate::frame::header::{FrameType, HEADER_LEN};
use crate::frame::{FrameReader, FrameWriter, ReadFrameError};
use crate::{BlockWrite as _, PersistAction, BLOCK_NUM_BYTES};

#[test]
fn test_frame_simple() {
    let block_writer = {
        let mut wrt: VecBlockWriter = VecBlockWriter::default();
        let mut session = wrt.start_write_session().unwrap();
        let mut frame_writer = FrameWriter::create(wrt);

        frame_writer
            .write_frame(FrameType::First, &b"abc"[..], &mut session)
            .unwrap();
        frame_writer
            .write_frame(FrameType::Middle, &b"de"[..], &mut session)
            .unwrap();
        frame_writer
            .write_frame(FrameType::Last, &b"fgh"[..], &mut session)
            .unwrap();
        frame_writer.persist(PersistAction::Flush).unwrap();
        frame_writer.into_writer()
    };
    let buffer: Vec<u8> = block_writer.into();
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buffer[..]));
    let mut session = frame_reader.start_session();
    let read_frame_res = frame_reader.read_frame(&mut session);
    assert_eq!(read_frame_res.unwrap(), (FrameType::First, &b"abc"[..]));
    assert_eq!(
        frame_reader.read_frame(&mut session).unwrap(),
        (FrameType::Middle, &b"de"[..])
    );
    assert_eq!(
        frame_reader.read_frame(&mut session).unwrap(),
        (FrameType::Last, &b"fgh"[..])
    );
    assert!(matches!(
        frame_reader.read_frame(&mut session).unwrap_err(),
        ReadFrameError::NotAvailable
    ));
}

#[test]
fn test_frame_corruption_in_payload() -> io::Result<()> {
    let mut buf: Vec<u8> = {
        let mut frame_writer = FrameWriter::create(VecBlockWriter::default());
        let mut session = frame_writer.start_session()?;
        frame_writer.write_frame(FrameType::First, &b"abc"[..], &mut session)?;
        frame_writer.persist(PersistAction::Flush)?;
        frame_writer.write_frame(FrameType::Middle, &b"de"[..], &mut session)?;
        frame_writer.persist(PersistAction::Flush)?;
        frame_writer.into_writer().into()
    };
    buf[8] = 0u8;
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buf[..]));
    let mut session = frame_reader.start_session();
    assert!(matches!(
        frame_reader.read_frame(&mut session),
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame(&mut session),
        Ok((FrameType::Middle, b"de"))
    ));
    Ok(())
}

fn repeat_empty_frame_util(repeat: usize) -> Vec<u8> {
    let mut frame_writer = FrameWriter::create(VecBlockWriter::default());
    let mut session = frame_writer.start_session().unwrap();
    for _ in 0..repeat {
        frame_writer
            .write_frame(FrameType::Full, &b""[..], &mut session)
            .unwrap();
    }
    frame_writer.persist(PersistAction::Flush).unwrap();
    frame_writer.into_writer().into()
}

#[test]
fn test_simple_multiple_blocks() -> io::Result<()> {
    let num_frames = 1 + BLOCK_NUM_BYTES / HEADER_LEN;
    let buffer = repeat_empty_frame_util(num_frames);
    let mut frame_reader = FrameReader::open(ArrayReader::from(&buffer[..]));
    let mut session = frame_reader.start_session();
    for _ in 0..num_frames {
        let read_frame_res = frame_reader.read_frame(&mut session);
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame(&mut &mut &mut &mut &mut &mut &mut &mut session),
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
    let mut session = frame_reader.start_session();
    for _ in 0..2000 {
        let read_frame_res = frame_reader.read_frame(&mut session);
        assert!(matches!(read_frame_res, Ok((FrameType::Full, &[]))));
    }
    assert!(matches!(
        frame_reader.read_frame(&mut session),
        Err(ReadFrameError::Corruption)
    ));
    assert!(matches!(
        frame_reader.read_frame(&mut session),
        Ok((FrameType::Full, &[]))
    ));
    assert!(matches!(
        frame_reader.read_frame(&mut session),
        Err(ReadFrameError::NotAvailable)
    ));
    Ok(())
}
