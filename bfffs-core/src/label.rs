// vim: tw=80

use byteorder::{BigEndian, ByteOrder};
use crate::{
    types::*,
    util::*
};
use divbuf::{DivBuf, DivBufShared};
use metrohash::MetroHash64;
use serde::{de::DeserializeOwned, Serialize};
use std::{hash::{Hash, Hasher}, io::{self, Seek, SeekFrom}};

/*
 * On-disk Label Format:
 *
 * Magic:       16 bytes
 * Checksum:    8 bytes     MetroHash64.  Covers all of Length and Contents.
 * Length:      8 bytes     Length of Contents in bytes
 * VdevFile:    variable    bincode-encoded VdevFile::Label
 * VdevRaid:    variable    bincode-encoded VdevRaid::Label
 * Pool:        variable    bincode-encoded Pool::Label
 * IDML:        variable    bincode-encoded IDML::Label
 * Database:    variable    bincode-encoded Database::Label
 * Pad:         variable    0-padding fills the remainder, up to 4 LBAs
 *
 * On-disk Reserved Region Format:
 *
 * Label 0      4 LBAs
 * Label 1      4 LBAs
 * Spacemap0    variable    bincode-encoded spacemap.  Size is determined at
 *                          format-time.
 * Spacemap1    variable
 */
/// The file magic is "BFFFS Vdev\0\0\0\0\0\0"
const MAGIC: &[u8; MAGIC_LEN] = b"BFFFS Vdev\0\0\0\0\0\0";
const MAGIC_LEN: usize = 16;
const CHECKSUM_LEN: usize = 8;
const LENGTH_LEN: usize = 8;
pub const LABEL_COUNT: LbaT = 2;
// Actual label size is about 17 bytes for each RAID member plus 17 bytes for
// each Cluster, plus a couple hundred bytes more.
pub const LABEL_LBAS: LbaT = 4;
pub const LABEL_SIZE: usize = LABEL_LBAS as usize * BYTES_PER_LBA;
/// Space allocated for storing the spacemap.  This the number of zones whose
/// information can be recorded in one LBA of storage.
pub const SPACEMAP_ZONES_PER_LBA: usize = 255;

/// How many LBAs should be reserved for each spacemap?
pub fn spacemap_space(nzones: u64) -> LbaT {
    nzones.div_ceil(SPACEMAP_ZONES_PER_LBA as u64)
}

/// Used to read successive structs out of the label
pub struct LabelReader {
    cursor: io::Cursor<Vec<u8>>
}

impl LabelReader {
    /// Attempt to read a `T` out of the label
    pub fn deserialize<T>(&mut self) -> bincode::Result<T>
        where T: DeserializeOwned
    {
        bincode::deserialize_from(&mut self.cursor)
    }

    /// Construct a `LabelReader` using the raw buffer read from disk
    pub fn new(buffer: Vec<u8>) -> Result<Self> {
        if buffer.len() < MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN {
            return Err(Error::EINVAL);
        }
        if MAGIC[..] != buffer[0..MAGIC_LEN] {
            return Err(Error::EINVAL);
        }

        let checksum = BigEndian::read_u64(
            &buffer[MAGIC_LEN..MAGIC_LEN + CHECKSUM_LEN]);
        let length_start = MAGIC_LEN + CHECKSUM_LEN;
        let contents_start = length_start + LENGTH_LEN;
        let contents_len = BigEndian::read_u64(
            &buffer[length_start .. contents_start]);
        let mut hasher = MetroHash64::new();
        {
            let contents = &buffer[contents_start ..
                               contents_start + contents_len as usize];
            contents_len.to_be().hash(&mut hasher);
            hasher.write(contents);
        }
        if checksum != hasher.finish() {
            return Err(Error::EINTEGRITY);
        }

        let mut cursor = io::Cursor::new(buffer);
        // Seek past header
        cursor.seek(SeekFrom::Start(contents_start as u64))
            .expect("IoVec too short");
        Ok(LabelReader { cursor })
    }

    /// Get the offset of the `label`th label.
    pub fn lba(label: u32) -> LbaT {
        assert!(LbaT::from(label) < LABEL_COUNT);
        LbaT::from(label) * LABEL_LBAS
    }
}

/// Successively writes serialized structs into the label
#[derive(Clone, Debug)]
pub struct LabelWriter {
    buffers: SGList,
    label: u32,
}

impl LabelWriter {
    /// Return the LBA at which to write this label
    pub fn lba(&self) -> LbaT {
        LbaT::from(self.label) * LABEL_LBAS
    }

    /// Create a new label in the `label`th position.
    pub fn new(label: u32) -> Self {
        assert!(LbaT::from(label) < LABEL_COUNT);
        LabelWriter{buffers: SGList::default(), label}
    }

    /// Write a `T` into the label.
    ///
    /// Multiple calls to `serialize` take effect in LIFO order.  That is, the
    /// last `serialize` call's data will be encoded into the lowest position in
    /// the label.
    pub fn serialize<T: Serialize>(&mut self, t: &T) -> bincode::Result<()> {
        bincode::serialize(t).map(|v| {
            let dbs = DivBufShared::from(v);
            self.buffers.push(dbs.try_const().unwrap());
        })
    }

    /// Consume the `LabelWriter` and return an `SGList` suitable for writing to
    /// the first sector of a disk.
    pub fn into_sglist(self) -> SGList {
        let mut sglist: SGList = Vec::with_capacity(self.buffers.len() + 2);
        let header_len = MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN;
        let header_dbs = DivBufShared::with_capacity(header_len);
        let mut header = header_dbs.try_mut().unwrap();
        header.extend(&MAGIC[..]);
        let contents = self.buffers.into_iter().rev().collect::<Vec<_>>();
        let contents_len: usize = contents.iter().map(DivBuf::len).sum();
        let mut hasher = MetroHash64::new();
        (contents_len as u64).to_be().hash(&mut hasher);
        checksum_sglist(&contents, &mut hasher);
        header.try_resize(MAGIC_LEN + CHECKSUM_LEN, 0).unwrap();
        BigEndian::write_u64(&mut header[MAGIC_LEN..], hasher.finish());
        header.try_resize(MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN, 0).unwrap();
        let length_start = MAGIC_LEN + CHECKSUM_LEN;
        BigEndian::write_u64(&mut header[length_start..], contents_len as u64);
        sglist.push(header.freeze());
        sglist.extend(contents);
        sglist
    }
}
