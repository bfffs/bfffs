// vim: tw=80

use byteorder::{BigEndian, ByteOrder};
use common::*;
use divbuf::DivBufShared;
use metrohash::MetroHash64;
use nix::{Error, errno};
use serde::{Deserialize, Serialize};
use serde_cbor;
use std::{hash::{Hash, Hasher}, io::{self, Seek, SeekFrom}};

/*
 * On-disk Label Format:
 * Magic:       16 bytes
 * Checksum:    8 bytes     MetroHash64.  Covers all of Length and Contents.
 * Length:      8 bytes     Length of Contents in bytes
 * Contents:    variable    3 CBOR-encoded structs
 * Pad:         variable    0-padding fills the remainder, up to 4 LBAs
 */
/// The file magic is "ArkFS Vdev\0\0\0\0\0\0"
const MAGIC: [u8; MAGIC_LEN] = [0x41, 0x72, 0x6b, 0x46, 0x53, 0x20, 0x56, 0x64,
                                0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
const MAGIC_LEN: usize = 16;
const CHECKSUM_LEN: usize = 8;
const LENGTH_LEN: usize = 8;
// Actual label size is about 17 bytes for each RAID member plus 17 bytes for
// each Cluster, plus a couple hundred bytes more.
pub const LABEL_LBAS: LbaT = 4;
pub const LABEL_SIZE: usize = LABEL_LBAS as usize * BYTES_PER_LBA;

/// Used to read successive structs out of the label
pub struct LabelReader {
    deserializer: serde_cbor::Deserializer<
        serde_cbor::de::IoRead<io::Cursor<IoVec>>>,
    /// Owns the data referenced by `deserializer`
    _dbs: DivBufShared,
}

impl<'de> LabelReader {
    /// Attempt to read a `T` out of the label
    pub fn deserialize<T>(&mut self) -> serde_cbor::error::Result<T>
        where T: Deserialize<'de> {
        T::deserialize(&mut self.deserializer)
    }

    /// Construct a `LabelReader` using the raw buffer read from disk
    pub fn from_dbs(buffer: DivBufShared) -> Result<Self, Error> {
        let db = buffer.try().unwrap();
        if db.len() < MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN {
            return Err(Error::Sys(errno::Errno::EINVAL));
        }
        if &MAGIC[..] != &db[0..MAGIC_LEN] {
            return Err(Error::Sys(errno::Errno::EINVAL));
        }

        let checksum = BigEndian::read_u64(
            &db[MAGIC_LEN..MAGIC_LEN + CHECKSUM_LEN]);
        let length_start = MAGIC_LEN + CHECKSUM_LEN;
        let contents_start = length_start + LENGTH_LEN;
        let contents_len = BigEndian::read_u64(
            &db[length_start .. contents_start]);
        let mut hasher = MetroHash64::new();
        {
            let contents = &db[contents_start ..
                               contents_start + contents_len as usize];
            contents_len.to_be().hash(&mut hasher);
            hasher.write(contents);
        }
        if checksum != hasher.finish() {
            return Err(Error::Sys(errno::Errno::EINVAL));
        }

        let mut cursor = io::Cursor::new(db);
        // Seek past header
        cursor.seek(SeekFrom::Start(contents_start as u64))
            .expect("IoVec too short");
        let deserializer = serde_cbor::Deserializer::from_reader(cursor);
        Ok(LabelReader { _dbs: buffer, deserializer })
    }
}

/// Successively writes serialized structs into the label
#[derive(Clone, Debug)]
pub struct LabelWriter {
    buffers: SGList,
}

impl LabelWriter {
    /// Construct an empty `LabelWriter`
    pub fn new() -> Self {
        LabelWriter { buffers: Vec::new() }
    }

    /// Write a `T` into the label.
    ///
    /// Multiple calls to `serialize` take effect in LIFO order.  That is, the
    /// last `serialize` call's data will be encoded into the lowest position in
    /// the label.
    pub fn serialize<T: Serialize>(&mut self, t: T)
        -> serde_cbor::error::Result<(DivBufShared)> {

        serde_cbor::ser::to_vec(&t).map(|v| {
            let dbs = DivBufShared::from(v);
            self.buffers.push(dbs.try().unwrap());
            dbs
        })
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Consume the `LabelWriter` and return an `SGList` suitable for writing to
    /// the first sector of a disk.
    ///
    /// The second return argument owns the data referenced by the `SGList` and
    /// must outlive it.
    pub fn into_sglist(self) -> (SGList, Vec<DivBufShared>) {
        let mut sglist: SGList = Vec::with_capacity(self.buffers.len() + 2);
        let header_len = MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN;
        let header_dbs = DivBufShared::with_capacity(header_len);
        let mut header = header_dbs.try_mut().unwrap();
        header.extend(&MAGIC[..]);
        let contents = self.buffers.into_iter().rev().collect::<Vec<_>>();
        let contents_len: usize = contents.iter().map(|x| x.len()).sum();
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
        let len = MAGIC_LEN + CHECKSUM_LEN + LENGTH_LEN + contents_len;
        let padlen = LABEL_SIZE - len;
        sglist.append(&mut zero_sglist(padlen));
        (sglist, vec![header_dbs])
    }
}
