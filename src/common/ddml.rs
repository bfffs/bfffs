// vim: tw=80
///! Direct Data Management Layer
///
/// Interface for working with Direct Records.  Unifies cache, compression,
/// disk, and hash operations.  A Direct Record is a record that can never be
/// duplicated, either through snapshots, clones, or deduplication.

use common::*;
use common::cache::*;
use common::pool::*;
use futures::{Future, future};
use metrohash::MetroHash64;
use nix::Error;
use std::sync::Mutex;

/// Default cache size.
const CACHE_SIZE: usize = 1_000_000_000;

/// Compression mode in use
#[derive(Clone, Copy, Debug)]
pub enum Compression {
    None,
    // TODO: add more types
}

/// Direct Record Pointer.  A persistable pointer to a record on disk.
///
/// A Record is a local unit of data on disk.  It may be larger or smaller than
/// a Block, but Records are always read/written in their entirety.
#[derive(Clone, Copy, Debug)]
pub struct DRP {
    /// Physical Block Address.  The record's location on disk.
    pba: PBA,
    /// Compression algorithm in use
    _compression: Compression,
    /// Logical size.  Uncompressed size of the record
    _lsize: u32,
    /// Compressed size.
    csize: u32,
    /// Checksum of the compressed record.
    _checksum: u64
}

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Mutex<Cache>,
    pool: Pool,
}

impl<'a> DDML {
    /// Delete the record from the cache, and free its storage space.
    pub fn delete(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
        self.pool.free(drp.pba, drp.csize as LbaT);
    }

    /// If the given record is present in the cache, evict it.
    pub fn evict(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
    }

    /// Initialze the `DDML`.
    pub fn new(pool: Pool) -> Self {
        DDML{pool: pool, cache: Mutex::new(Cache::with_capacity(CACHE_SIZE))}
    }

    /// Read a record and return a shared reference
    pub fn get(&'a self, drp: &DRP)
        -> Box<Future<Item=DivBuf, Error=Error> + 'a> {

        let pba = drp.pba;
        self.cache.lock().unwrap().get(&Key::PBA(pba)).map(|db| {
            let r : Box<Future<Item=DivBuf, Error=Error>> =
            Box::new(future::ok::<DivBuf, Error>(db));
            r
        }).unwrap_or_else(|| {
            let dbs = DivBufShared::from(vec![0u8; drp.csize as usize]);
            Box::new(
                self.pool.read(dbs.try_mut().unwrap(), pba).map(move |_| {
                    let db = dbs.try().unwrap();
                    self.cache.lock().unwrap().insert(Key::PBA(pba), dbs);
                    db
                })
            )
        })
    }

    /// Read a record and return ownership of it.
    pub fn pop(&'a self, drp: &DRP)
        -> Box<Future<Item=DivBufShared, Error=Error> + 'a> {

        let pba = drp.pba;
        let csize = drp.csize;
        self.cache.lock().unwrap().remove(&Key::PBA(pba)).map(|dbs| {
            let r : Box<Future<Item=DivBufShared, Error=Error>> =
            Box::new(future::ok::<DivBufShared, Error>(dbs));
            r
        }).unwrap_or_else(|| {
            let dbs = DivBufShared::from(vec![0u8; csize as usize]);
            Box::new(
                self.pool.read(dbs.try_mut().unwrap(), pba).map(move |_| {
                    self.pool.free(pba, csize as LbaT);
                    dbs
                })
            )
        })
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer
    pub fn put(&'a self, buf: DivBufShared, _compression: Compression)
        -> (DRP, Box<Future<Item=(), Error=Error> + 'a>) {
        // Outline:
        // 1) Compress
        // 2) Hash
        // 3) Cache
        // 4) Write

        let db = buf.try().unwrap();
        assert!(db.len() < u32::max_value() as usize,
            "Record exceeds maximum allowable length");
        let _lsize = db.len() as u32;
        // TODO: compress buffer
        let csize = _lsize;
        let mut hasher = MetroHash64::new();
        hasher.write(&db[..]);
        let _checksum = hasher.finish();
        let (pba, fut) = self.pool.write(db).unwrap();
        self.cache.lock().unwrap().insert(Key::PBA(pba), buf);
        let drp = DRP {
            pba,
            _compression,
            _lsize,
            csize,
            _checksum
        };
        (drp, fut)
    }
}
