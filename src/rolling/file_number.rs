use std::cmp::Reverse;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct FileNumber {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    pub(crate) file_number: u32,
    next_file_number: Arc<Mutex<Option<FileNumber>>>,
}

impl fmt::Debug for FileNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileNumber")
            .field("file_number", &self.inner.file_number)
            .finish()
    }
}

impl Ord for FileNumber {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.file_number.cmp(&other.inner.file_number)
    }
}

impl PartialOrd for FileNumber {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for FileNumber {}

impl PartialEq for FileNumber {
    fn eq(&self, other: &Self) -> bool {
        self.inner.file_number == other.inner.file_number
    }
}

impl FileNumber {
    fn new(file_number: u32) -> Self {
        FileNumber {
            inner: Arc::new(Inner {
                file_number,
                next_file_number: Arc::new(Mutex::new(None)),
            }),
        }
    }

    pub fn can_be_deleted(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    #[cfg(test)]
    pub fn unroll(&self) -> Vec<u32> {
        let mut file = self.clone();
        let mut file_numbers = Vec::new();
        loop {
            file_numbers.push(file.file_number());
            if let Some(next_file) = file.next() {
                file = next_file;
            } else {
                return file_numbers;
            }
        }
    }

    pub fn filename(&self) -> String {
        format!("wal-{:020}", self.inner.file_number)
    }

    #[cfg(test)]
    pub fn file_number(&self) -> u32 {
        self.inner.file_number
    }

    #[cfg(test)]
    pub fn for_test(file_number: u32) -> Self {
        FileNumber::new(file_number)
    }

    pub fn next(&self) -> Option<FileNumber> {
        self.inner.next_file_number.lock().unwrap().clone()
    }

    /// Increment the position and returns the previous value.
    pub fn inc(&self) -> FileNumber {
        let mut lock = self.inner.next_file_number.lock().unwrap();
        if let Some(file) = lock.as_ref() {
            return file.clone();
        }
        let new_file_number = FileNumber::new(self.inner.file_number + 1u32);
        *lock = Some(new_file_number.clone());
        new_file_number
    }

    pub fn from_file_numbers(mut file_numbers: Vec<u32>) -> Option<FileNumber> {
        if file_numbers.is_empty() {
            return None;
        }
        file_numbers.sort_by_key(|val| Reverse(*val));
        let mut first_file_number = FileNumber::new(file_numbers[0]);
        for &file_number in &file_numbers[1..] {
            first_file_number = FileNumber {
                inner: Arc::new(Inner {
                    file_number,
                    next_file_number: Arc::new(Mutex::new(Some(first_file_number.clone()))),
                }),
            };
        }
        Some(first_file_number)
    }
}

#[cfg(test)]
impl From<u32> for FileNumber {
    fn from(file_number: u32) -> Self {
        FileNumber::for_test(file_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_number_starts_at_0() {
        let file = FileNumber::default();
        assert_eq!(file.file_number(), 0u32);
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
