use std::io;

use crate::error::HeaderError;

const MAGIC_NUMBER: u32 = 1_778_463_742_u32;
const HEADER_SIZE: usize = 4 + // magic number
                           4 + // version
                           4 + // num_pages
                           4; // CRC
const VERSION: u32 = 1;

fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn to_header_bytes(num_pages: u32) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());
    buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
    buf[8..12].copy_from_slice(&num_pages.to_le_bytes());
    let checksum = crc32(&buf[..12]);
    buf[12..16].copy_from_slice(&checksum.to_le_bytes());
    buf
}

fn from_header_bytes(header_bytes: [u8; HEADER_SIZE]) -> Result<u32, HeaderError> {
    let magic_number = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());
    let num_pages = u32::from_le_bytes(header_bytes[8..12].try_into().unwrap());
    let checksum = u32::from_le_bytes(header_bytes[12..16].try_into().unwrap());
    let computed_checksum = crc32(&header_bytes[..12]);
    if checksum != computed_checksum {
        return Err(HeaderError::InvalidChecksum);
    }
    if magic_number != MAGIC_NUMBER {
        return Err(HeaderError::InvalidMagicNumber { magic_number });
    }
    if version != VERSION {
        return Err(HeaderError::UnsupportedVersion { version });
    }
    Ok(num_pages)
}

pub fn serialize_header(num_pages: u32, wrt: &mut dyn io::Write) -> io::Result<HeaderInfo> {
    let header_bytes = to_header_bytes(num_pages);
    wrt.write_all(&header_bytes[..])?;
    Ok(HeaderInfo {
        header_len: HEADER_SIZE,
        num_pages,
    })
}

pub fn deserialize_header(read: &mut dyn io::Read) -> io::Result<HeaderInfo> {
    let mut header_bytes = [0u8; HEADER_SIZE];
    read.read_exact(&mut header_bytes)?;
    let num_pages = from_header_bytes(header_bytes)
        .map_err(|header_err| io::Error::new(io::ErrorKind::InvalidData, header_err))?;
    Ok(HeaderInfo {
        header_len: HEADER_SIZE,
        num_pages,
    })
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct HeaderInfo {
    pub header_len: usize,
    pub num_pages: u32,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_simple_serialize_deserialize_header() {
        let mut buf = Vec::new();
        let num_pages = 10;
        let expected_header_info = HeaderInfo {
            header_len: HEADER_SIZE,
            num_pages,
        };
        assert_eq!(
            serialize_header(num_pages, &mut buf).unwrap(),
            expected_header_info
        );
        assert_eq!(
            deserialize_header(&mut buf.as_slice()).unwrap(),
            expected_header_info
        );
    }

    #[test]
    fn test_serialize_header() {
        let mut buf = Vec::new();
        let num_pages = 10;
        let header_info = serialize_header(num_pages, &mut buf).unwrap();
        assert_eq!(header_info.header_len, buf.len());
        assert_eq!(header_info.num_pages, num_pages);
    }
}
