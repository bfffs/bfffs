// vim: tw=80
///! Direct Data Management Layer
///
/// Interface for working with Direct Records.  Unifies cache, compression,
/// disk, and hash operations.  A Direct Record is a record that can never be
/// duplicated, either through snapshots, clones, or deduplication.

use crate::{
    boxfut,
    common::{
        *,
        cache::{Cacheable, CacheRef, Key},
        label::*,
    }
};
#[cfg(not(test))] use crate::common::pool::*;
use futures::{Future, Stream, future, stream};
use metrohash::MetroHash64;
#[cfg(test)] use rand::{self, Rng};
use std::{
    borrow,
    hash::Hasher,
    sync::{Arc, Mutex}
};
#[cfg(all(test, feature = "mocks"))] use simulacrum::*;
#[cfg(test)] use uuid::Uuid;

pub use crate::common::dml::{Compression, DML};
pub use crate::common::pool::ClosedZone;

#[cfg(not(test))]
use crate::common::cache::Cache;
// LCOV_EXCL_START
#[cfg(test)]
use crate::common::cache_mock::CacheMock as Cache;

#[cfg(test)]
/// Only exists so mockers can replace Pool
pub trait PoolTrait {
    fn allocated(&self) -> LbaT;
    fn assert_clean_zone(&self, clust: ClusterT, zid: ZoneT, txg: TxgT);
    fn find_closed_zone(&self, clust: ClusterT, zid: ZoneT)
        -> Box<dyn Future<Item=(Option<ClosedZone>, Option<(ClusterT, ZoneT)>),
                      Error=Error>>;
    fn flush(&self, idx: u32) -> Box<dyn Future<Item=(), Error=Error> + Send>;
    fn free(&self, pba: PBA, length: LbaT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>;
    fn name(&self) -> &str;
    fn read(&self, buf: IoVecMut, pba: PBA)
        -> Box<dyn Future<Item=(), Error=Error> + Send>;
    fn shutdown(&self);
    fn size(&self) -> LbaT;
    fn sync_all(&self) -> Box<dyn Future<Item=(), Error=Error> + Send>;
    fn uuid(&self) -> Uuid;
    fn write(&self, buf: IoVec, txg: TxgT)
        -> Box<dyn Future<Item=PBA, Error=Error> + Send>;
    fn write_label(&self, labeller: LabelWriter)
        -> Box<dyn Future<Item=(), Error=Error> + Send>;
}

/// Part of an ugly hack for mocking a Send trait
#[cfg(test)]
pub struct MockPoolWrapper(Box<dyn PoolTrait>);

#[cfg(test)]
impl PoolTrait for MockPoolWrapper {
    fn allocated(&self) -> LbaT {
        self.0.allocated()
    }
    fn assert_clean_zone(&self, clust: ClusterT, zid: ZoneT, txg: TxgT) {
        self.0.assert_clean_zone(clust, zid, txg)
    }
    fn find_closed_zone(&self, clust: ClusterT, zid: ZoneT)
        -> Box<dyn Future<Item=(Option<ClosedZone>, Option<(ClusterT, ZoneT)>),
                      Error=Error>>
    {
        self.0.find_closed_zone(clust, zid)
    }
    fn flush(&self, idx: u32) -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.0.flush(idx)
    }
    fn free(&self, pba: PBA, length: LbaT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.0.free(pba, length)
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn read(&self, buf: IoVecMut, pba: PBA)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.0.read(buf, pba)
    }
    fn shutdown(&self) {
        self.0.shutdown()
    }
    fn size(&self) -> LbaT {
        self.0.size()
    }
    fn sync_all(&self)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.0.sync_all()
    }
    fn uuid(&self) -> Uuid {
        self.0.uuid()
    }
    fn write(&self, buf: IoVec, txg: TxgT)
        -> Box<dyn Future<Item=PBA, Error=Error> + Send>
    {
        self.0.write(buf, txg)
    }
    fn write_label(&self, labeller: LabelWriter)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.0.write_label(labeller)
    }
}

// XXX totally unsafe!  But Mockers doesn't support mocking Send traits.  So
// we have to cheat.  This works as long as MockPoolWrapper is only used in
// single-threaded unit tests.
#[cfg(test)]
unsafe impl Send for MockPoolWrapper {}
#[cfg(test)]
unsafe impl Sync for MockPoolWrapper {}

#[cfg(test)]
pub type PoolLike = MockPoolWrapper;
#[cfg(not(test))]
#[doc(hidden)]
pub type PoolLike = Pool;
// LCOV_EXCL_STOP

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
            rng.gen_range(0, lsize as u32)
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

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Arc<Mutex<Cache>>,
    pool: Arc<PoolLike>,
}

impl DDML {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.pool.allocated()
    }

    /// Assert that the given zone was clean as of the given transaction
    #[cfg(debug_assertions)]
    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) {
        self.pool.assert_clean_zone(cluster, zone, txg)
    }

    /// Free a record's storage, ignoring the Cache
    pub fn delete_direct(&self, drp: &DRP, _txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
    {
        self.pool.free(drp.pba, drp.asize())
    }

    pub fn flush(&self, idx: u32) -> Box<dyn Future<Item=(), Error=Error> + Send> {
        Box::new(self.pool.flush(idx))
    }

    pub fn new(pool: PoolLike, cache: Arc<Mutex<Cache>>) -> Self {
        DDML{pool: Arc::new(pool), cache}
    }

    /// Get directly from disk, bypassing cache
    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Item=Box<T>, Error=Error>
    {
        self.read(*drp).map(move |dbs| {
            Box::new(T::deserialize(dbs))
        })
    }

    /// List all closed zones in the `DDML` in no particular order
    pub fn list_closed_zones(&self)
        -> impl Stream<Item=ClosedZone, Error=Error>
    {
        struct State {
            pool: Arc<PoolLike>,
            cluster: ClusterT,
            zid: ZoneT
        };

        let initial = Some(State{pool: self.pool.clone(), cluster: 0, zid: 0});
        stream::unfold(initial, |state| {
            if let Some(s) = state {
                let fut = s.pool.find_closed_zone(s.cluster, s.zid)
                .map(|r| {
                    match r {
                        (Some(pclz), Some((c, z))) => {
                            let next = State{pool: s.pool, cluster: c, zid: z};
                            (Some(pclz), Some(next))
                        },
                        (Some(_), None) => unreachable!(),  // LCOV_EXCL_LINE
                        (None, Some((c, z))) => {
                            let next = State{pool: s.pool, cluster: c, zid: z};
                            (None, Some(next))
                        },
                        (None, None) => (None, None)
                    }
                }); // LCOV_EXCL_LINE   kcov false negative
                Some(fut)
            } else {
                None
            }
        }).filter_map(|opt_zone| opt_zone)
    }

    /// Read a record from disk
    fn read(&self, drp: DRP)
        -> impl Future<Item=DivBufShared, Error=Error> + Send
    {
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
                    let db = dbs.try_const().unwrap();
                    if drp.is_compressed() {
                        Ok(Compression::decompress(&db))
                    } else {
                        Ok(dbs)
                    }
                } else {
                    Err(Error::ECKSUM)
                }
            })
        )
    }

    /// Open an existing `DDML` from its underlying `Pool`.
    ///
    /// # Parameters
    ///
    /// * `cache`:      An already constructed `Cache`
    /// * `pool`:       An already constructed `Pool`
    pub fn open(pool: PoolLike, cache: Arc<Mutex<Cache>>) -> Self {
        DDML{pool: Arc::new(pool), cache}
    }

    /// Read a record and return ownership of it, bypassing Cache
    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Item=Box<T>, Error=Error>
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        let pool2 = self.pool.clone();
        self.read(*drp)
            .and_then(move |dbs|
                pool2.free(pba, lbas)
                .map(move |_| Box::new(T::deserialize(dbs)))
            )
    }

    /// Does most of the work of DDML::put
    fn put_common<T>(&self, cacheref: &T, compression: Compression,
                     txg: TxgT)
        -> impl Future<Item=DRP, Error=Error>
        where T: borrow::Borrow<CacheRef>
    {
        // Outline:
        // 1) Serialize
        // 2) Compress
        // 3) Checksum
        // 4) Write
        // 5) Cache

        // Serialize
        let serialized = cacheref.borrow().serialize();
        assert!(serialized.len() < u32::max_value() as usize,
            "Record exceeds maximum allowable length");
        let lsize = serialized.len();

        // Compress
        let (compressed_db, compression) = compression.compress(serialized);
        let compressed = compression.is_compressed();
        let csize = compressed_db.len() as u32;

        // Checksum
        let mut hasher = MetroHash64::new();
        checksum_iovec(&compressed_db, &mut hasher);
        let checksum = hasher.finish();

        // Write
        self.pool.write(compressed_db, txg)
        .map(move |pba| {
            DRP { pba, compressed, lsize: lsize as u32, csize, checksum }
        })
    }

    /// Write a buffer bypassing cache.  Return the same buffer
    pub fn put_direct<T>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
        -> impl Future<Item=DRP, Error=Error>
        where T: borrow::Borrow<CacheRef>
    {
        self.put_common(cacheref, compression, txg)
    }

    /// Shutdown all background tasks.
    pub fn shutdown(&self) {
        self.pool.shutdown()
    }

    /// Return approximately the usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.pool.size()
    }

    pub fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error>
    {
        self.pool.write_label(labeller)
    }
}

impl DML for DDML {
    type Addr = DRP;

    fn delete(&self, drp: &DRP, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
        Box::new(self.pool.free(drp.pba, drp.asize()))
    }

    fn evict(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
    }

    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Box<dyn Future<Item=Box<R>, Error=Error> + Send>
    {
        // Outline:
        // 1) Fetch from cache, or
        // 2) Read from disk, then insert into cache
        let pba = drp.pba;
        self.cache.lock().unwrap().get::<R>(&Key::PBA(pba)).map(|t| {
            boxfut!(future::ok::<Box<R>, Error>(t))
        }).unwrap_or_else(|| {
            let cache2 = self.cache.clone();
            Box::new(
                self.get_direct(drp).map(move |cacheable: Box<T>| {
                    let r = cacheable.make_ref();
                    cache2.lock().unwrap().insert(Key::PBA(pba), cacheable);
                    r.downcast::<R>().unwrap()
                })
            )
        })
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP, _txg: TxgT)
        -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        self.cache.lock().unwrap().remove(&Key::PBA(pba)).map(|cacheable| {
            let t = cacheable.downcast::<T>().unwrap();
            boxfut!(self.pool.free(pba, lbas).map(|_| t))
        }).unwrap_or_else(|| {
            boxfut!( self.pop_direct::<T>(drp))
        })
    }

    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Box<dyn Future<Item=DRP, Error=Error> + Send>
    {
        let cache2 = self.cache.clone();
        let db = cacheable.make_ref();
        let fut = self.put_common(&db, compression, txg)
            .map(move |drp|{
                let pba = drp.pba();
                cache2.lock().unwrap()
                    .insert(Key::PBA(pba), Box::new(cacheable));
                drp
            });
        Box::new(fut)
    }

    fn sync_all(&self, _txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        Box::new(self.pool.sync_all())
    }
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {
mod drp {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn as_uncompressed() {
        let drp0 = DRP::random(Compression::ZstdL9NoShuffle, 5000);
        let drp0_nc = drp0.as_uncompressed();
        assert!(!drp0_nc.is_compressed());
        assert_eq!(drp0_nc.lsize, drp0_nc.csize);
        assert_eq!(drp0_nc.csize, drp0.csize);
        assert_eq!(drp0_nc.pba, drp0.pba);

        //drp1 is what DDML::put_direct will return after writing drp0_nc's
        //contents as uncompressed
        let mut drp1 = DRP::random(Compression::None, drp0.csize as usize);
        drp1.checksum = drp0_nc.checksum;

        let drp1_c = drp1.into_compressed(&drp0);
        assert!(drp1_c.is_compressed());
        assert_eq!(drp1_c.lsize, drp0.lsize);
        assert_eq!(drp1_c.csize, drp0.csize);
        assert_eq!(drp1_c.pba, drp1.pba);
        assert_eq!(drp1_c.checksum, drp0.checksum);
    }

    #[test]
    fn typical_size() {
        let drp = DRP::random(Compression::ZstdL9NoShuffle, 5000);
        let size = bincode::serialized_size(&drp).unwrap() as usize;
        assert_eq!(DRP::TYPICAL_SIZE, size);
    }
}

mod ddml {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::{IntoFuture, future};
    use mockers::matchers::ANY;
    use mockers::{Scenario, Sequence, check};
    use mockers_derive::mock;
    use pretty_assertions::assert_eq;
    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use simulacrum::validators::trivial::any;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::runtime::current_thread;

    mock!{
        MockPool,
        self,
        trait PoolTrait {
            fn allocated(&self) -> LbaT;
            fn assert_clean_zone(&self, clust: ClusterT, zid: ZoneT, txg: TxgT);
            fn find_closed_zone(&self, clust: ClusterT, zid: ZoneT)
                -> Box<Future<Item=(Option<ClosedZone>,
                                    Option<(ClusterT, ZoneT)>),
                              Error=Error>>;
            fn flush(&self, idx: u32)
                -> Box<Future<Item=(), Error=Error> + Send>;
            fn free(&self, pba: PBA, length: LbaT)
                -> Box<Future<Item=(), Error=Error> + Send>;
            fn name(&self) -> &str;
            fn read(&self, buf: IoVecMut, pba: PBA)
                -> Box<Future<Item=(), Error=Error> + Send>;
            fn shutdown(&self);
            fn size(&self) -> LbaT;
            fn sync_all(&self)
                -> Box<Future<Item=(), Error=Error> + Send>;
            fn uuid(&self) -> Uuid;
            fn write(&self, buf: IoVec, txg: TxgT)
                -> Box<Future<Item=PBA, Error=Error> + Send>;
            fn write_label(&self, mut labeller: LabelWriter)
                -> Box<Future<Item=(), Error=Error> + Send>;
        }
    }

    #[test]
    fn delete_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let s = Scenario::new();
        let mut cache = Cache::default();
        // Ideally, we'd expect that Cache::remove gets called before
        // Pool::free.  But Simulacrum lacks that ability.
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(move |_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.free_call(pba, 1)
            .and_return(Box::new(Ok(()).into_future())));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.delete(&drp, TxgT::from(0))
        })).unwrap();
    }

    #[test]
    fn evict() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let s = Scenario::new();
        let mut cache = Cache::default();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| None);
        let pool = s.create_mock::<MockPool>();
        let pool_wrapper = MockPoolWrapper(Box::new(pool));

        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        ddml.evict(&drp);
    }

    #[test]
    fn get_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let s = Scenario::new();
        let cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.read_call(check!(|dbm: &DivBufMut| {
            dbm.len() == 4096
        }), pba).and_return(Box::new(future::ok::<(), Error>(()))));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.get_direct::<DivBufShared>(&drp)
        })).unwrap();
    }

    #[test]
    fn get_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(move |_| {
                Some(Box::new(dbs.try_const().unwrap()))
            });

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        ddml.get::<DivBufShared, DivBuf>(&drp);
    }

    #[test]
    fn get_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let owned_by_cache = Rc::new(RefCell::new(Vec::<Box<dyn Cacheable>>::new()));
        let owned_by_cache2 = owned_by_cache.clone();
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        // Ideally we'd assert that Pool::read gets called in between Cache::get
        // and Cache::insert.  But Simulacrum can't do that.
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| None);
        s.expect(pool.read_call(check!(|dbm: &DivBufMut| {
            dbm.len() == 4096
        }), pba).and_return(Box::new(future::ok::<(), Error>(()))));
        cache.expect_insert()
            .called_once()
            .with(passes(move |args: &(Key, _)| {
                args.0 == Key::PBA(pba)
            })).returning(move |(_, dbs)| {;
                owned_by_cache2.borrow_mut().push(dbs);
            });

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.get::<DivBufShared, DivBuf>(&drp)
        })).unwrap();
    }

    #[test]
    fn get_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| None);
        s.expect(pool.read_call(check!(|dbm: &DivBufMut| {
            dbm.len() == 4096
        }), pba).and_return(Box::new(future::ok::<(), Error>(()))));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        let err = rt.block_on(future::lazy(|| {
            ddml.get::<DivBufShared, DivBuf>(&drp)
        })).unwrap_err();
        assert_eq!(err, Error::ECKSUM);
    }

    #[test]
    fn list_closed_zones() {
        let s = Scenario::new();
        let cache = Cache::default();
        let pool = s.create_mock::<MockPool>();

        // The first cluster has two closed zones
        let clz0 = ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5, zid: 0,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        s.expect(pool.find_closed_zone_call(0, 0).and_return(
            Box::new(Ok((Some(clz0.clone()), Some((0, 11)))).into_future())));

        let clz1 = ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6, zid: 1,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3)};
        s.expect(pool.find_closed_zone_call(0, 11).and_return(
            Box::new(Ok((Some(clz1.clone()), Some((0, 31)))).into_future())));

        s.expect(pool.find_closed_zone_call(0, 31).and_return(
            Box::new(Ok((None, Some((1, 0)))).into_future())));

        // The second cluster has no closed zones
        s.expect(pool.find_closed_zone_call(1, 0).and_return(
            Box::new(Ok((None, Some((2, 0)))).into_future())));

        // The third cluster has one closed zone
        let clz2 = ClosedZone{pba: PBA::new(2, 10), freed_blocks: 5, zid: 2,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        s.expect(pool.find_closed_zone_call(2, 0).and_return(
            Box::new(Ok((Some(clz2.clone()), Some((2, 11)))).into_future())));

        s.expect(pool.find_closed_zone_call(2, 11).and_return(
            Box::new(Ok((None, None)).into_future())));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        let closed_zones = rt.block_on(
            ddml.list_closed_zones().collect()
        ).unwrap();
        let expected = vec![clz0, clz1, clz2];
        assert_eq!(closed_zones, expected);
    }

    #[test]
    fn pop_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        s.expect(pool.free_call(pba, 1)
            .and_return(Box::new(Ok(()).into_future())));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0));
    }

    #[test]
    fn pop_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let s = Scenario::new();
        let mut seq = Sequence::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| None);
        seq.expect(pool.read_call(ANY, pba)
                   .and_return(Box::new(future::ok::<(), Error>(()))));
        seq.expect(pool.free_call(pba, 1)
            .and_return(Box::new(Ok(()).into_future())));
        s.expect(seq);

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
        })).unwrap();
    }

    #[test]
    fn pop_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::PBA(pba)}
            })).returning(|_| None);
        s.expect(pool.read_call(ANY, pba)
                   .and_return(Box::new(future::ok::<(), Error>(()))));

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        let err = rt.block_on(future::lazy(|| {
            ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
        })).unwrap_err();
        assert_eq!(err, Error::ECKSUM);
    }

    #[test]
    fn pop_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let s = Scenario::new();
        let mut seq = Sequence::new();
        let cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        seq.expect(pool.read_call(ANY, pba)
                   .and_return(Box::new(future::ok::<(), Error>(()))));
        seq.expect(pool.free_call(pba, 1)
            .and_return(Box::new(Ok(()).into_future())));
        s.expect(seq);

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.pop_direct::<DivBufShared>(&drp)
        })).unwrap();
    }

    #[test]
    fn put() {
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .called_once()
            .with(params!(Key::PBA(pba), any()))
            .returning(drop);
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.write_call(ANY, TxgT::from(42))
            .and_return(Box::new(future::ok::<PBA, Error>(pba)))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::None, TxgT::from(42))
        ).unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
        assert_eq!(drp.pba, pba);
    }

    /// With compression enabled, compressible data should be compressed
    #[test]
    fn put_compressible() {
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .called_once()
            .with(params!(Key::PBA(pba), any()))
            .returning(drop);
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.write_call(ANY, TxgT::from(42))
            .and_return(Box::new(future::ok::<PBA, Error>(pba)))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::ZstdL9NoShuffle, TxgT::from(42))
        ).unwrap();
        assert!(drp.is_compressed());
        assert!(drp.csize < 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
    }

    /// Incompressible data should not be compressed, even when compression is
    /// enabled.
    #[test]
    fn put_incompressible() {
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .called_once()
            .with(params!(Key::PBA(pba), any()))
            .returning(drop);
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.write_call(ANY, TxgT::from(42))
            .and_return(Box::new(future::ok::<PBA, Error>(pba)))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; 8192];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::ZstdL9NoShuffle, TxgT::from(42))
        ).unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
    }

    #[test]
    fn put_partial_lba() {
        let s = Scenario::new();
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .called_once()
            .with(params!(Key::PBA(pba), any()))
            .returning(drop);
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.write_call(ANY, TxgT::from(42))
            .and_return(Box::new(future::ok::<PBA, Error>(pba)))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::None, TxgT::from(42))
        ).unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 1024);
        assert_eq!(drp.lsize, 1024);
    }

    #[test]
    fn put_direct() {
        let s = Scenario::new();
        let cache = Cache::default();
        let pba = PBA::default();
        let pool = s.create_mock::<MockPool>();
        let txg = TxgT::from(42);
        s.expect(pool.write_call(ANY, txg)
            .and_return(Box::new(future::ok::<PBA, Error>(pba)))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let db = Box::new(dbs.try_const().unwrap()) as Box<dyn CacheRef>;
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put_direct(&db, Compression::None, txg)
        ).unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
    }

    #[test]
    fn sync_all() {
        let s = Scenario::new();
        let cache = Cache::default();
        let pool = s.create_mock::<MockPool>();
        s.expect(pool.sync_all_call()
            .and_return(Box::new(future::ok::<(), Error>(())))
        );

        let pool_wrapper = MockPoolWrapper(Box::new(pool));
        let ddml = DDML::new(pool_wrapper, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        assert!(rt.block_on(ddml.sync_all(TxgT::from(0))).is_ok());
    }
}
}
// LCOV_EXCL_STOP
