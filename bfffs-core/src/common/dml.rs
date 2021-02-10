// vim: tw=80

use crate::common::*;
use futures::Future;
#[cfg(test)] use mockall::automock;
use std::{
    num::NonZeroU8,
    pin::Pin
};

pub use crate::common::cache::{Cacheable, CacheRef};

/// Compression mode in use
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Compression {
    None,
    /// LZ4 is very fast with decent compression.  From experiment, it's the
    /// best algorithm for metadata
    LZ4(Option<NonZeroU8>),
    /// ZStandard usually gives a very good compression ratio with moderate
    /// speed.  `typesize` is the size of each individual element.  Use
    /// `typesize=None` for an unstructured buffer.
    Zstd(Option<NonZeroU8>),
}

impl Compression {
    pub fn compress(self, input: IoVec) -> (IoVec, Compression) {
        let usize_from_typesize = |ts: NonZeroU8| usize::from(ts.get());
        let lsize = input.len();
        if self == Compression::None || lsize <= BYTES_PER_LBA {
            (input, Compression::None)
        } else {
            let ctx0 = blosc::Context::new()
                .shuffle(blosc::ShuffleMode::Byte);
            let ctx = match self {
                Compression::None  => {
                    unreachable!()
                },
                Compression::LZ4(typesize) => {
                    ctx0.typesize(typesize.map(usize_from_typesize))
                        .compressor(blosc::Compressor::LZ4).unwrap()
                },
                Compression::Zstd(typesize) => {
                    ctx0.typesize(typesize.map(usize_from_typesize))
                        .compressor(blosc::Compressor::Zstd).unwrap()
                }
            };
            let buffer = ctx.compress(&input[..]);
            let v: Vec<u8> = buffer.into();
            let dbs = DivBufShared::from(v);
            let compressed_lbas = div_roundup(dbs.len(), BYTES_PER_LBA);
            let uncompressed_lbas = div_roundup(lsize, BYTES_PER_LBA);
            if compressed_lbas < uncompressed_lbas {
                (dbs.try_const().unwrap(), self)
            } else {
                (input, Compression::None)
            }
        }
    }

    pub fn decompress(input: &IoVec) -> DivBufShared {
        let v = unsafe {
            // Sadly, decompressing with Blosc is unsafe until
            // https://github.com/Blosc/c-blosc/issues/229 gets fixed
            blosc::decompress_bytes(input)
        }.unwrap();
        DivBufShared::from(v)
    }

    /// Does this compression algorithm compress the data at all?
    pub fn is_compressed(self) -> bool {
        self != Compression::None
    }

    /// Get the shuffle setting
    pub fn shuffle(self) -> Option<NonZeroU8> {
        match self {
            Compression::None => None,
            Compression::LZ4(s) | Compression::Zstd(s) => s
        }
    }
}

impl Default for Compression {
    fn default() -> Compression {
        Compression::None
    }
}

/// DML: Data Management Layer
///
/// A DML handles reading and writing records with cacheing.  It also handles
/// compression and checksumming.
#[cfg_attr(test, automock(type Addr=u32;))]
pub trait DML: Send + Sync {
    type Addr;

    /// Delete the record from the cache, and free its storage space.
    fn delete(&self, addr: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>;

    /// If the given record is present in the cache, evict it.
    fn evict(&self, addr: &Self::Addr);

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, addr: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>, Error>> + Send>>;

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, rid: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>, Error>> + Send>>;

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<<Self as DML>::Addr, Error>> + Send>>;

    /// Sync all records written so far to stable storage.
    fn sync_all(&self, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<(), Error>> + Send>>;
}

#[cfg(test)]
mod t {
    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use super::*;

    /// Compressible data should not be compressed, if doing so would save < 1
    /// LBA of space.
    #[test]
    fn compress_barely_compressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; lsize];
        rng.fill_bytes(&mut v[0..lsize - 1024]);
        let dbs = DivBufShared::from(v);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    /// Compressible data should be compressed
    #[test]
    fn compress_compressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert!(zdb.len() < lsize);
        assert_eq!(compression, Compression::Zstd(None));
    }

    /// Compression should not be attempted when it is disabled.
    #[test]
    fn compress_compression_disabled() {
        let lsize = 2 * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::None.compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    /// Compressible data won't be compressed if it's already <= 1 LBA.
    #[test]
    fn compress_compressible_but_short() {
        let lsize = BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![42u8; lsize]);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }

    /// Incompressible data should not be compressed, even when compression is
    /// enabled.
    #[test]
    fn compress_incompressible() {
        let lsize = 2 * BYTES_PER_LBA;
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; lsize];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let db = dbs.try_const().unwrap();
        let (zdb, compression) = Compression::Zstd(None).compress(db);
        assert_eq!(zdb.len(), lsize);
        assert_eq!(compression, Compression::None);
    }
}
