use std::collections::BTreeSet;
use std::sync::Arc;

pub struct FileTracker {
    files: BTreeSet<FileNumber>,
}

impl FileTracker {
    pub fn new() -> FileTracker {
        FileTracker::from_file_numbers(vec![0]).unwrap()
    }

    pub fn first(&self) -> &FileNumber {
        self.files.iter().next().unwrap()
    }

    pub fn take_first_unused(&mut self) -> Option<FileNumber> {
        // if len is 1, we need to keep that element to keep self.files not empty
        if self.files.len() < 2 {
            return None;
        }

        let first = self.files.iter().next().unwrap();
        if first.can_be_deleted() {
            let idx = *first.file_number;
            self.files.take(&idx)
        } else {
            None
        }
    }

    pub fn next(&self, curr: &FileNumber) -> Option<FileNumber> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.files
            .range((Excluded(*curr.file_number), Unbounded))
            .next()
            .cloned()
    }

    pub fn inc(&mut self, curr: &FileNumber) -> FileNumber {
        use std::ops::Bound::{Excluded, Unbounded};
        if let Some(file) = self
            .files
            .range((Excluded(*curr.file_number), Unbounded))
            .next()
        {
            return file.clone();
        }
        let new_number = *curr.file_number + 1u64;
        let new_file_number = FileNumber::new(new_number);
        self.files.insert(new_file_number.clone());
        new_file_number
    }

    pub fn from_file_numbers(file_numbers: Vec<u64>) -> Option<FileTracker> {
        if file_numbers.is_empty() {
            return None;
        }

        let files = file_numbers
            .into_iter()
            .map(|k| FileNumber::new(k))
            .collect();

        Some(FileTracker { files })
    }
}

#[derive(Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct FileNumber {
    file_number: Arc<u64>,
}

impl FileNumber {
    fn new(file_number: u64) -> Self {
        FileNumber {
            file_number: Arc::new(file_number),
        }
    }

    pub fn can_be_deleted(&self) -> bool {
        Arc::strong_count(&self.file_number) == 1
    }

    #[cfg(test)]
    pub fn unroll(&self, tracker: &FileTracker) -> Vec<u64> {
        let mut file = self.clone();
        let mut file_numbers = Vec::new();
        loop {
            file_numbers.push(file.file_number());
            if let Some(next_file) = tracker.next(&file) {
                file = next_file;
            } else {
                return file_numbers;
            }
        }
    }

    pub fn filename(&self) -> String {
        format!("wal-{:020}", self.file_number)
    }

    #[cfg(test)]
    pub fn file_number(&self) -> u64 {
        *self.file_number
    }

    #[cfg(test)]
    pub fn for_test(file_number: u64) -> Self {
        FileNumber::new(file_number)
    }
}

impl std::borrow::Borrow<u64> for FileNumber {
    fn borrow(&self) -> &u64 {
        &self.file_number
    }
}

#[cfg(test)]
impl From<u64> for FileNumber {
    fn from(file_number: u64) -> Self {
        FileNumber::for_test(file_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_number_starts_at_0() {
        let file = FileNumber::default();
        assert_eq!(file.file_number(), 0u64);
    }

    #[test]
    fn test_file_number_can_be_deleted() {
        let file = FileNumber::default();
        assert!(file.can_be_deleted());
    }

    #[test]
    fn test_file_number_with_clone_cannot_be_deleted() {
        let file = FileNumber::default();
        let _file_clone = file.clone();
        assert!(!file.can_be_deleted());
    }

    #[test]
    fn test_file_number_cannot_be_deleted_after_cloned_dropped() {
        let file = FileNumber::default();
        let file_clone = file.clone();
        assert!(!file_clone.can_be_deleted());
        drop(file);
        assert!(file_clone.can_be_deleted());
    }
}
