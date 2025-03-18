use std::io;

use crate::page_directory::FileLike;

pub(crate) struct MockFile {
    buf: Vec<u8>,
    cursor: usize,
    fsynced: bool,
}

impl MockFile {
    pub fn new() -> MockFile {
        MockFile {
            buf: Vec::new(),
            cursor: 0,
            fsynced: false,
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

impl FileLike for MockFile {
    fn fsyncdata(&mut self) -> io::Result<()> {
        self.fsynced = true;
        Ok(())
    }

    fn set_len(&mut self, num_bytes: u64) -> io::Result<()> {
        self.buf.resize(num_bytes as usize, 0u8);
        Ok(())
    }
}

impl io::Read for MockFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end = (self.cursor + buf.len()).min(self.buf.len());
        let len = end - self.cursor;
        buf[..len].copy_from_slice(&self.buf[self.cursor..end]);
        self.cursor = end;
        Ok(len)
    }
}

impl io::Write for MockFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cursor + buf.len() > self.buf.len() {
            self.buf.resize(self.cursor + buf.len(), 0u8);
        }
        self.fsynced = false;
        self.buf[self.cursor..][..buf.len()].copy_from_slice(buf);
        self.cursor += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Seek for MockFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match pos {
            io::SeekFrom::Start(offset) => self.cursor = offset as usize,
            io::SeekFrom::End(offset) => self.cursor = self.buf.len() as usize + offset as usize,
            io::SeekFrom::Current(offset) => self.cursor += offset as usize,
        }
        Ok(self.cursor as u64)
    }
}
