// vim: tw=80

use common::*;
use divbuf::DivBufShared;
use nix::{Error, errno};
use serde::{Deserialize, Serialize};
use serde_cbor;
use std::io;
use std::io::{Seek, SeekFrom};

const CHECKSUM_LEN: usize = 8;
const MAGIC_LEN: usize = 16;
/// The file magic is "ArkFS Vdev\0\0\0\0\0\0"
const MAGIC: [u8; MAGIC_LEN] = [0x41, 0x72, 0x6b, 0x46, 0x53, 0x20, 0x56, 0x64,
                                0x65, 0x76, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
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
        // TODO: verify checksum
        let db = buffer.try().unwrap();
        if &MAGIC[..] == &db[0..MAGIC_LEN] {
            let mut cursor = io::Cursor::new(db);
            // Seek past magic and checksum
            let seek_len = (MAGIC_LEN + CHECKSUM_LEN) as u64;
            cursor.seek(SeekFrom::Start(seek_len)).expect("IoVec too short");
            let deserializer = serde_cbor::Deserializer::from_reader(cursor);
            Ok(LabelReader { _dbs: buffer, deserializer })
        } else {
            Err(Error::Sys(errno::Errno::EINVAL))
        }
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
    }

    /// Consume the `LabelWriter` and return an `SGList` suitable for writing to
    /// the first sector of a disk.
    ///
    /// The second return argument owns the data referenced by the `SGList` and
    /// must outlive it.
    pub fn into_sglist(self) -> (SGList, Vec<DivBufShared>) {
        let mut sglist: SGList = Vec::with_capacity(self.buffers.len() + 2);
        let magic = DivBufShared::from(Vec::from(&MAGIC[..]));
        sglist.push(magic.try().unwrap());
        let checksum = DivBufShared::from(vec![0u8; CHECKSUM_LEN]);
        // TODO: calculate checksum correctly
        sglist.push(checksum.try().unwrap());
        sglist.extend(self.buffers.into_iter().rev());
        let len = sglist.iter().fold(0, |acc, b| acc + b.len());
        let padlen = LABEL_SIZE - len;
        // TODO: use a global zero region for the pad
        let pad = DivBufShared::from(vec![0u8; padlen]);
        sglist.push(pad.try().unwrap().slice_to(padlen));
        (sglist, vec![magic, checksum, pad])
    }
}
