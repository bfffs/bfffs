// vim: tw=80

use blosc;
use common::*;
use futures::Future;
use nix::Error;

pub use common::cache::{Cacheable, CacheRef};

/// Compression mode in use
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum Compression {
    None = 0,
    /// Maximum Compression ratio for unstructured buffers
    ZstdL9NoShuffle = 1,
}

impl Compression {
    pub fn compress(self, input: &IoVec) -> Option<DivBufShared> {
        match self {
            Compression::None  => {
                None
            },
            Compression::ZstdL9NoShuffle => {
                let ctx = blosc::Context::new()
                    .clevel(blosc::Clevel::L9)
                    .compressor(blosc::Compressor::Zstd).unwrap();
                let buffer = ctx.compress(&input[..]);
                let v: Vec<u8> = buffer.into();
                Some(DivBufShared::from(v))
            }
        }
    }

    pub fn decompress(self, input: &IoVec) -> Option<DivBufShared> {
        match self {
            Compression::None  => {
                None
            },
            Compression::ZstdL9NoShuffle => {
                let v = unsafe {
                    // Sadly, decompressing with Blosc is unsafe until
                    // https://github.com/Blosc/c-blosc/issues/229 gets fixed
                    blosc::decompress_bytes(input)
                }.unwrap();
                Some(DivBufShared::from(v))
            }
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
pub trait DML: Send + Sync {
    type Addr;

    /// Delete the record from the cache, and free its storage space.
    fn delete(&self, addr: &Self::Addr, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send>;

    /// If the given record is present in the cache, evict it.
    fn evict(&self, addr: &Self::Addr);

    /// Read a record and return a shared reference
    fn get<T: Cacheable, R: CacheRef>(&self, addr: &Self::Addr)
        -> Box<Future<Item=Box<R>, Error=Error> + Send>;

    /// Read a record and return ownership of it.
    fn pop<T: Cacheable, R: CacheRef>(&self, rid: &Self::Addr, txg: TxgT)
        -> Box<Future<Item=Box<T>, Error=Error> + Send>;

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Box<Future<Item=Self::Addr, Error=Error> + Send>;

    /// Sync all records written so far to stable storage.
    fn sync_all<'a>(&'a self, txg: TxgT)
        -> Box<Future<Item=(), Error=Error> + Send + 'a>;
}
