// vim: tw=80
///! Direct Data Management Layer
///
/// Interface for working with Direct Records.  Unifies cache, compression,
/// disk, and hash operations.  A Direct Record is a record that can never be
/// duplicated, either through snapshots, clones, or deduplication.

use blosc;
use common::{*, cache::*, pool::*};
use futures::{Future, future};
use metrohash::MetroHash64;
use nix::{Error, errno};
#[cfg(test)] use rand::{self, Rng};
use std::{hash::Hasher, sync::Mutex};
#[cfg(test)] use simulacrum::*;
#[cfg(test)] use uuid::Uuid;

pub use common::cache::{Cacheable, CacheRef};

// LCOV_EXCL_START
#[cfg(test)]
pub struct CacheMock {
    e: Expectations,
}
#[cfg(test)]
impl CacheMock {
    pub fn new() -> Self {
        Self {
            e: Expectations::new()
        }
    }

    pub fn get<T: CacheRef>(&mut self, key: &Key) -> Option<Box<T>> {
        self.e.was_called_returning::<*const Key, Option<Box<T>>>
            ("get", key as *const Key)
    }

    pub fn expect_get<T: CacheRef>(&mut self) -> Method<*const Key, Option<Box<T>>> {
        self.e.expect::<*const Key, Option<Box<T>>>("get")
    }

    pub fn insert(&mut self, key: Key, buf: Box<Cacheable>) {
        self.e.was_called_returning::<(Key, Box<Cacheable>), ()>
            ("insert", (key, buf))
    }

    pub fn expect_insert(&mut self) -> Method<(Key, Box<Cacheable>), ()> {
        self.e.expect::<(Key, Box<Cacheable>), ()>("insert")
    }

    pub fn remove(&mut self, key: &Key) -> Option<Box<Cacheable>> {
        self.e.was_called_returning::<*const Key, Option<Box<Cacheable>>>
            ("remove", key as *const Key)
    }

    pub fn expect_remove(&mut self)
        -> Method<*const Key, Option<Box<Cacheable>>>
    {
        self.e.expect::<*const Key, Option<Box<Cacheable>>>("remove")
    }

    pub fn size(&self) -> usize {
        self.e.was_called_returning::<(), usize>("size", ())
    }
}

#[cfg(test)]
pub type CacheLike = CacheMock;
#[cfg(not(test))]
#[doc(hidden)]
pub type CacheLike = Cache;

#[cfg(test)]
/// Only exists so mockers can replace Pool
pub trait PoolTrait {
    fn free(&self, pba: PBA, length: LbaT);
    fn name(&self) -> &str;
    fn read(&self, buf: IoVecMut, pba: PBA) -> Box<PoolFut>;
    fn sync_all(&self) -> Box<PoolFut>;
    fn uuid(&self) -> Uuid;
    fn write(&self, buf: IoVec) -> Result<(PBA, Box<PoolFut>), Error>;
    fn write_label(&self) -> Box<PoolFut>;
}
#[cfg(test)]
pub type PoolLike = Box<PoolTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type PoolLike = Pool;
// LCOV_EXCL_STOP

/// Default cache size.
#[cfg(not(test))]
const CACHE_SIZE: usize = 1_000_000_000;

/// Compression mode in use
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum Compression {
    None = 0,
    /// Maximum Compression ratio for unstructured buffers
    ZstdL9NoShuffle = 1,
}

impl Compression {
    fn compress(&self, input: &IoVec) -> Option<DivBufShared> {
        match *self {
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

    fn decompress(&self, input: &IoVec) -> Option<DivBufShared> {
        match *self {
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

/// Direct Record Pointer.  A persistable pointer to a record on disk.
///
/// A Record is a local unit of data on disk.  It may be larger or smaller than
/// a Block, but Records are always read/written in their entirety.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DRP {
    /// Physical Block Address.  The record's location on disk.
    pba: PBA,
    /// Compression algorithm in use
    compression: Compression,
    /// Logical size.  Uncompressed size of the record
    lsize: u32,
    /// Compressed size.
    csize: u32,
    /// Checksum of the compressed record.
    checksum: u64
}

impl DRP {
    /// Return the storage space actually allocated for this record
    fn asize(&self) -> LbaT {
        div_roundup(self.csize as usize, BYTES_PER_LBA) as LbaT
    }

    // LCOV_EXCL_START
    /// Explicitly construct a `DRP`, for testing.  Production code should never
    /// use this method, because `DRP`s should be opaque to the upper layers.
    #[cfg(test)]
    pub fn new(pba: PBA, compression: Compression, lsize: u32, csize: u32,
               checksum: u64) -> Self {
        DRP{pba, compression, lsize, csize, checksum}
    }

    /// Get an otherwise random DRP with a specific lsize and compression.
    /// Useful for testing purposes.
    #[cfg(test)]
    pub fn random(compression: Compression, lsize: usize) -> DRP {
        let mut rng = rand::thread_rng();
        DRP {
            pba: PBA {
                cluster: rng.gen(),
                lba: rng.gen()
            },
            compression,
            lsize: lsize as u32,
            csize: rng.gen_range(0, lsize as u32),
            checksum: rng.gen()
        }
    }
    // LCOV_EXCL_STOP
}

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Mutex<CacheLike>,
    pool: PoolLike,
}

impl<'a> DDML {
    /// Initialze the `DDML`.
    #[cfg(not(test))]
    pub fn create(pool: PoolLike) -> Self {
        DDML::new(pool, Cache::with_capacity(CACHE_SIZE))
    }

    /// Delete the record from the cache, and free its storage space.
    pub fn delete(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
        self.pool.free(drp.pba, drp.asize());
    }

    /// If the given record is present in the cache, evict it.
    pub fn evict(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
    }

    #[cfg(any(not(test), feature = "mocks"))]
    fn new(pool: PoolLike, cache: CacheLike) -> Self {
        DDML{pool: pool, cache: Mutex::new(cache)}
    }

    /// Read a record and return a shared reference
    pub fn get<T: CacheRef>(&'a self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a> {

        // Outline:
        // 1) Fetch from cache, or
        // 2) Read from disk, then insert into cache
        let pba = drp.pba;
        self.cache.lock().unwrap().get::<T>(&Key::PBA(pba)).map(|t| {
            let r : Box<Future<Item=Box<T>, Error=Error>> =
            Box::new(future::ok::<Box<T>, Error>(t));
            r
        }).unwrap_or_else(|| {
            Box::new(
                self.read(*drp).map(move |dbs| {
                    let cacheable = T::deserialize(dbs);
                    let r = cacheable.make_ref();
                    self.cache.lock().unwrap().insert(Key::PBA(pba), cacheable);
                    r.downcast::<T>().unwrap()
                })
            )
        })
    }

    /// Read a record and return ownership of it.
    pub fn pop<T: Cacheable>(&'a self, drp: &DRP)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a> {

        let lbas = drp.asize();
        let pba = drp.pba;
        self.cache.lock().unwrap().remove(&Key::PBA(pba)).map(|cacheable| {
            self.pool.free(pba, lbas);
            let t = cacheable.downcast::<T>().unwrap();
            let r : Box<Future<Item=Box<T>, Error=Error>> =
            Box::new(future::ok::<Box<T>, Error>(t));
            r
        }).unwrap_or_else(|| {
            Box::new(
                self.read(*drp).map(move |dbs| {
                    self.pool.free(pba, lbas);
                    Box::new(T::deserialize(dbs))
                })
            )
        })
    }

    /// Write a record to disk and cache.  Return its Direct Record Pointer.
    pub fn put<T: Cacheable>(&'a self, cacheable: T, compression: Compression)
        -> (DRP, Box<Future<Item=(), Error=Error> + 'a>) {
        // Outline:
        // 1) Serialize
        // 2) Compress
        // 3) Checksum
        // 4) Pad
        // 5) Write
        // 6) Cache

        // Serialize
        let (serialized, keeper) = cacheable.serialize();
        assert!(serialized.len() < u32::max_value() as usize,
            "Record exceeds maximum allowable length");
        let lsize = serialized.len() as u32;

        // Compress
        let compressed_dbs = compression.compress(&serialized);
        let compressed_db = match &compressed_dbs {
            &Some(ref dbs) => {
                dbs.try().unwrap()
            },
            &None => {
                serialized
            }
        };
        let csize = compressed_db.len() as u32;

        // Checksum
        let mut hasher = MetroHash64::new();
        checksum_iovec(&compressed_db, &mut hasher);
        let checksum = hasher.finish();

        // Pad
        let asize = div_roundup(csize as usize, BYTES_PER_LBA);
        let compressed_db = if asize * BYTES_PER_LBA != csize as usize {
            let mut dbm = compressed_db.try_mut().unwrap();
            dbm.try_resize(asize * BYTES_PER_LBA, 0).unwrap();
            dbm.freeze()
        } else {
            compressed_db
        };

        // Write
        let (pba, wfut) = self.pool.write(compressed_db).unwrap();
        let fut = Box::new(wfut.map(move |r| {
            if compression == Compression::None {
                // Truncate uncompressed DivBufShareds.  We padded them in the
                // previous step
                cacheable.truncate(csize as usize);
            } else {
                let _ = compressed_dbs;
            }
            let _ = keeper;
            //Cache
            self.cache.lock().unwrap().insert(Key::PBA(pba),
                                              Box::new(cacheable));
            r
        }));
        let drp = DRP { pba, compression, lsize, csize, checksum };
        (drp, fut)
    }

    /// Read a record from disk
    fn read(&'a self, drp: DRP)
        -> Box<Future<Item=DivBufShared, Error=Error> + 'a> {

        // Outline
        // 1) Read
        // 2) Truncate
        // 3) Verify checksum
        // 4) Decompress
        let len = drp.asize() as usize * BYTES_PER_LBA;
        let dbs = DivBufShared::from(vec![0u8; len]);
        Box::new(
            // Read
            self.pool.read(dbs.try_mut().unwrap(), drp.pba).and_then(move |_| {
                //Truncate
                let mut dbm = dbs.try_mut().unwrap();
                dbm.try_truncate(drp.csize as usize).unwrap();
                let db = dbm.freeze();

                // Verify checksum
                let mut hasher = MetroHash64::new();
                checksum_iovec(&db, &mut hasher);
                let checksum = hasher.finish();
                if checksum == drp.checksum {
                    // Decompress
                    let db = dbs.try().unwrap();
                    Ok(match drp.compression.decompress(&db) {
                        Some(decompressed) => decompressed,
                        None => dbs
                    })
                } else {
                    // TODO: create a dedicated ECKSUM error type
                    Err(Error::Sys(errno::Errno::EIO))
                }
            })
        )
    }

    /// Sync all records written so far to stable storage.
    pub fn sync_all(&'a self) -> Box<Future<Item=(), Error=Error> + 'a> {
        Box::new(self.pool.sync_all())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {

    use super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use mockers::matchers::ANY;
    use mockers::{Scenario, Sequence};
    use mockers_derive::mock;
    use simulacrum::validators::trivial::any;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::executor::current_thread;

    mock!{
        MockPool,
        self,
        trait PoolTrait {
            fn free(&self, pba: PBA, length: LbaT);
            fn name(&self) -> &str;
            fn read(&self, buf: IoVecMut, pba: PBA) -> Box<PoolFut<'static>>;
            fn sync_all(&self) -> Box<PoolFut<'static>>;
            fn uuid(&self) -> Uuid;
            fn write(&self, buf: IoVec)
                -> Result<(PBA, Box<PoolFut<'static>>), Error>;
            fn write_label(&self) -> Box<PoolFut>;
        }
    }

    #[test]
    fn delete_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 4096, checksum: 0};
        let pba2 = pba.clone();
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        // Ideally, we'd expect that Cache::remove gets called before
        // Pool::free.  But Simulacrum lacks that ability.
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(move |_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.free_call(pba, 1).and_return(()));

        let ddml = DDML::new(Box::new(pool), cache);
        ddml.delete(&drp);
    }

    #[test]
    fn evict() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 4096, checksum: 0};
        let pba2 = pba.clone();
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| None);
        let pool = s.create_mock::<MockPool>();

        let ddml = DDML::new(Box::new(pool), cache);
        ddml.evict(&drp);
    }

    #[test]
    fn get_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 4096, checksum: 0};
        let pba2 = pba.clone();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });

        let ddml = DDML::new(Box::new(pool), cache);
        ddml.get::<DivBuf>(&drp);
    }

    #[test]
    fn get_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 1, checksum: 0xe7f15966a3d61f8};
        let pba2 = pba.clone();
        let owned_by_cache = Rc::new(RefCell::new(Vec::<Box<Cacheable>>::new()));
        let owned_by_cache2 = owned_by_cache.clone();
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        // Ideally we'd assert that Pool::read gets called in between Cache::get
        // and Cache::insert.  But Simulacrum can't do that.
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| None);
        s.expect(pool.read_call(check!(|dbm: &DivBufMut| {
            dbm.len() == 4096
        }), pba).and_return(Box::new(future::ok::<(), Error>(()))));
        cache.expect_insert()
            .called_once()
            .with(passes(move |args: &(Key, _)| {
                args.0 == Key::PBA(pba2)
            })).returning(move |(_, dbs)| {;
                owned_by_cache2.borrow_mut().push(dbs);
            });

        let ddml = DDML::new(Box::new(pool), cache);
        current_thread::block_on_all(future::lazy(|| {
            ddml.get::<DivBuf>(&drp)
        })).unwrap();
    }

    #[test]
    fn get_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 1, checksum: 0xdeadbeefdeadbeef};
        let pba2 = pba.clone();
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| None);
        s.expect(pool.read_call(check!(|dbm: &DivBufMut| {
            dbm.len() == 4096
        }), pba).and_return(Box::new(future::ok::<(), Error>(()))));

        let ddml = DDML::new(Box::new(pool), cache);
        let err = current_thread::block_on_all(future::lazy(|| {
            ddml.get::<DivBuf>(&drp)
        })).unwrap_err();
        assert_eq!(err, Error::Sys(errno::Errno::EIO));
    }

    #[test]
    fn pop_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 4096, checksum: 0};
        let pba2 = pba.clone();
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        s.expect(pool.free_call(pba, 1).and_return(()));

        let ddml = DDML::new(Box::new(pool), cache);
        ddml.pop::<DivBufShared>(&drp);
    }

    #[test]
    fn pop_cold() {
        let pba = PBA::default();
        let pba2 = pba.clone();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 1, checksum: 0xe7f15966a3d61f8};
        let s = Scenario::new();
        let mut seq = Sequence::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| None);
        seq.expect(pool.read_call(ANY, pba)
                   .and_return(Box::new(future::ok::<(), Error>(()))));
        seq.expect(pool.free_call(pba, 1).and_return(()));
        s.expect(seq);

        let ddml = DDML::new(Box::new(pool), cache);
        current_thread::block_on_all(future::lazy(|| {
            ddml.pop::<DivBufShared>(&drp)
        })).unwrap();
    }

    #[test]
    fn pop_ecksum() {
        let pba = PBA::default();
        let pba2 = pba.clone();
        let drp = DRP{pba, compression: Compression::None, lsize: 4096,
                      csize: 1, checksum: 0xdeadbeefdeadbeef};
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba2)}
            })).returning(|_| None);
        s.expect(pool.read_call(ANY, pba)
                   .and_return(Box::new(future::ok::<(), Error>(()))));

        let ddml = DDML::new(Box::new(pool), cache);
        let err = current_thread::block_on_all(future::lazy(|| {
            ddml.pop::<DivBufShared>(&drp)
        })).unwrap_err();
        assert_eq!(err, Error::Sys(errno::Errno::EIO));
    }

    #[test]
    fn put() {
        let s = Scenario::new();
        let mut cache = CacheMock::new();
        let pba = PBA::default();
        cache.expect_insert()
            .called_once()
            .with(params!(Key::PBA(pba), any()))
            .returning(|_| ());
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.write_call(ANY)
            .and_return(Ok((pba, Box::new(future::ok::<(), Error>(())))))
        );

        let ddml = DDML::new(Box::new(pool), cache);
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let (drp, fut) = ddml.put(dbs, Compression::None);
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
        current_thread::block_on_all(fut).unwrap();
    }

    #[test]
    fn sync_all() {
        let s = Scenario::new();
        let cache = CacheMock::new();
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.sync_all_call()
            .and_return(Box::new(future::ok::<(), Error>(())))
        );

        let ddml = DDML::new(Box::new(pool), cache);
        assert!(current_thread::block_on_all(ddml.sync_all()).is_ok());
    }
}
// LCOV_EXCL_STOP
