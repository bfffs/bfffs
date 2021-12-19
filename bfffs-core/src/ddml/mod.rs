// vim: tw=80
///! Direct Data Management Layer
///
/// Interface for working with Direct Records.  Unifies cache, compression,
/// disk, and hash operations.  A Direct Record is a record that can never be
/// duplicated, either through snapshots, clones, or deduplication.

use crate::{
    types::*,
    util::*,
};
#[cfg(test)] use rand::{self, Rng};

pub use crate::dml::{Compression, DML};
pub use crate::pool::ClosedZone;

use mockall_double::*;
use serde_derive::{Deserialize, Serialize};

mod ddml;

#[double]
pub use self::ddml::DDML;

/// Direct Record Pointer.  A persistable pointer to a record on disk.
///
/// A Record is a local unit of data on disk.  It may be larger or smaller than
/// a Block, but Records are always read/written in their entirety.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DRP {
    /// Physical Block Address.  The record's location on disk.
    // Must come first so PartialOrd can be derived
    pba: PBA,
    /// Is the record compressed?
    compressed: bool,
    /// Logical size.  Uncompressed size of the record
    lsize: u32,
    /// Compressed size.
    csize: u32,
    /// Checksum of the compressed record.
    checksum: u64
}

impl DRP {
    /// Return a new DRP that refers to the same record as though it were
    /// uncompressed.
    #[must_use]
    pub fn as_uncompressed(&self) -> DRP {
        DRP {
            pba: self.pba,
            compressed: false,
            lsize: self.csize,
            csize: self.csize,
            checksum: self.checksum
        }
    }

    /// Return the storage space actually allocated for this record
    fn asize(&self) -> LbaT {
        div_roundup(self.csize as usize, BYTES_PER_LBA) as LbaT
    }

    /// Transform this DRP into one that has the same compression function as
    /// `old_compressed`.  This is basically the opposite of
    /// [`as_uncompressed`](#method.as_uncompressed)
    #[must_use]
    pub fn into_compressed(mut self, old_compressed: &DRP) -> DRP {
        self.compressed = old_compressed.compressed;
        self.lsize = old_compressed.lsize;
        self
    }

    /// Was this record written in compressed form?
    pub fn is_compressed(&self) -> bool {
        self.compressed
    }

    // LCOV_EXCL_START
    /// Explicitly construct a `DRP`, for testing.  Production code should never
    /// use this method, because `DRP`s should be opaque to the upper layers.
    #[doc(hidden)]
    pub fn new(pba: PBA, compression: Compression, lsize: u32, csize: u32,
               checksum: u64) -> Self {
        let compressed = compression.is_compressed();
        DRP{pba, compressed, lsize, csize, checksum}
    }

    /// Get the Physical Block Address of the record's start
    pub fn pba(&self) -> PBA {
        self.pba
    }

    /// Get an otherwise random DRP with a specific lsize and compression.
    /// Useful for testing purposes.
    #[cfg(test)]
    pub fn random(compression: Compression, lsize: usize) -> DRP {

        let mut rng = rand::thread_rng();
        let csize = if compression == Compression::None {
            lsize as u32
        } else {
            rng.gen_range(0..lsize as u32)
        };
        DRP {
            pba: PBA {
                cluster: rng.gen(),
                lba: rng.gen()
            },
            compressed: compression.is_compressed(),
            lsize: lsize as u32,
            csize,
            checksum: rng.gen()
        }
    }
    // LCOV_EXCL_STOP
}

impl TypicalSize for DRP {
    const TYPICAL_SIZE: usize = 27;
}


