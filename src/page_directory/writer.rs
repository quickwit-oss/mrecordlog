use crate::{BlockWrite, BLOCK_NUM_BYTES};

use super::page_list::PageList;


pub struct PageListWriter {
    page_lists: PageList,
    cursor: usize,
}

impl<'a> BlockWrite for PageListWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        todo!();
    }

    fn persist(&mut self, persist_action: crate::PersistAction) -> std::io::Result<()> {
        todo!()
    }

    fn num_bytes_remaining_in_block(&self) -> usize {
        if self.cursor == BLOCK_NUM_BYTES {
            return 0;
        }
        BLOCK_NUM_BYTES - (self.cursor  % BLOCK_NUM_BYTES)
    }
}
