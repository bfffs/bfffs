// vim: tw=80
use crate::{
    boxfut,
    common::{
        *,
        cache::{Cache, Cacheable, CacheRef, Key},
        label::*,
    }
};
use futures::{Future, Stream, future, stream};
use metrohash::MetroHash64;
#[cfg(test)] use mockall::mock;
use std::{
    borrow,
    convert::identity,
    hash::Hasher,
    sync::{Arc, Mutex}
};
use super::*;

#[cfg(not(test))] use crate::common::pool::Pool;
#[cfg(test)] use crate::common::pool::MockPool as Pool;

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Arc<Mutex<Cache>>,
    pool: Arc<Pool>,
}

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
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

    pub fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        DDML{pool: Arc::new(pool), cache}
    }

    /// Get directly from disk, bypassing cache
    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Item=Box<T>, Error=Error> + Send
    {
        self.read(*drp).map(move |dbs| {
            Box::new(T::deserialize(dbs))
        })
    }

    /// List all closed zones in the `DDML` in no particular order
    pub fn list_closed_zones(&self)
        -> impl Stream<Item=ClosedZone, Error=Error> + Send
    {
        struct State {
            pool: Arc<Pool>,
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
        }).filter_map(identity)
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
        let dbs = DivBufShared::uninitialized(len);
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
    pub fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        DDML{pool: Arc::new(pool), cache}
    }

    /// Read a record and return ownership of it, bypassing Cache
    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Item=Box<T>, Error=Error> + Send
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
        -> impl Future<Item=DRP, Error=Error> + Send
        where T: borrow::Borrow<dyn CacheRef>
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
        -> impl Future<Item=DRP, Error=Error> + Send
        where T: borrow::Borrow<dyn CacheRef>
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
        -> impl Future<Item=(), Error=Error> + Send
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
mock! {
    pub DDML {
        fn allocated(&self) -> LbaT;
        fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT);
        fn delete_direct(&self, drp: &DRP, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn flush(&self, idx: u32)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        fn get_direct<T: Cacheable>(&self, drp: &DRP)
            -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>;
        fn list_closed_zones(&self)
            -> Box<dyn Stream<Item=ClosedZone, Error=Error> + Send>;
        fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        fn pop_direct<T: Cacheable>(&self, drp: &DRP)
            -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>;
        fn put_direct<T: 'static>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
            -> Box<dyn Future<Item=DRP, Error=Error> + Send>
            where T: borrow::Borrow<dyn CacheRef>;
        fn shutdown(&self);
        fn size(&self) -> LbaT;
        fn write_label(&self, labeller: LabelWriter)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
    }
    trait DML {
        type Addr = DRP;

        fn delete(&self, addr: &DRP, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
        fn evict(&self, addr: &DRP);
        fn get<T: Cacheable, R: CacheRef>(&self, addr: &DRP)
            -> Box<dyn Future<Item=Box<R>, Error=Error> + Send>;
        fn pop<T: Cacheable, R: CacheRef>(&self, rid: &DRP, txg: TxgT)
            -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>;
        fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                                 txg: TxgT)
            -> Box<dyn Future<Item=DRP, Error=Error> + Send>;
        fn sync_all(&self, txg: TxgT)
            -> Box<dyn Future<Item=(), Error=Error> + Send>;
    }
}

#[cfg(test)]
mod t {
mod drp {
    use pretty_assertions::assert_eq;
    use super::super::*;

    #[test]
    fn as_uncompressed() {
        let drp0 = DRP::random(Compression::Zstd(None), 5000);
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
        let drp = DRP::random(Compression::Zstd(None), 5000);
        let size = bincode::serialized_size(&drp).unwrap() as usize;
        assert_eq!(DRP::TYPICAL_SIZE, size);
    }
}

mod ddml {
    use super::super::*;
    use divbuf::DivBufShared;
    use futures::{IntoFuture, future};
    use mockall::{
        self,
        Sequence,
        predicate::*
    };
    use pretty_assertions::assert_eq;
    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use tokio::runtime::current_thread;

    #[test]
    fn delete_hot() {
        let mut seq = Sequence::new();
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::default();
        cache.expect_remove()
            .once()
            .in_sequence(&mut seq)
            .with(eq(Key::PBA(pba)))
            .return_once(
                |_| Some(Box::new(DivBufShared::from(vec![0u8;4096])))
            );
        let mut pool = Pool::default();
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.delete(&drp, TxgT::from(0))
        })).unwrap();
    }

    #[test]
    fn evict() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::default();
        cache.expect_remove()
            .once()
            .with(eq(Key::PBA(pba)))
            .return_once(|_| None);
        let pool = Pool::default();

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.evict(&drp);
    }

    #[test]
    fn get_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let cache = Cache::default();
        let mut pool = Pool::default();
        pool.expect_read()
            .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::new(future::ok::<(), Error>(()))
            });

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let mut cache = Cache::default();
        let pool = Pool::default();
        cache.expect_get()
            .once()
            .with(eq(Key::PBA(pba)))
            .returning(move |_| {
                Some(Box::new(dbs.try_const().unwrap()))
            });

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.get::<DivBufShared, DivBuf>(&drp);
    }

    #[test]
    fn get_cold() {
        let mut seq = Sequence::new();
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let owned_by_cache = Arc::new(
            Mutex::new(Vec::<Box<dyn Cacheable>>::new())
        );
        let owned_by_cache2 = owned_by_cache.clone();
        let mut cache = Cache::default();
        let mut pool = Pool::default();
        cache.expect_get::<DivBuf>()
            .once()
            .in_sequence(&mut seq)
            .with(eq(Key::PBA(pba)))
            .return_const(None);
        pool.expect_read()
            .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::new(future::ok::<(), Error>(()))
            });
        cache.expect_insert()
            .once()
            .in_sequence(&mut seq)
            .with(eq(Key::PBA(pba)), always())
            .return_once(move |_, dbs| {;
                owned_by_cache2.lock().unwrap().push(dbs);
            });

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.get::<DivBufShared, DivBuf>(&drp)
        })).unwrap();
    }

    #[test]
    fn get_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let mut cache = Cache::default();
        let mut pool = Pool::default();
        cache.expect_get::<DivBuf>()
            .once()
            .with(eq(Key::PBA(pba)))
            .return_const(None);
        pool.expect_read()
            .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
            .return_once(|_, _| Box::new(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        let err = rt.block_on(future::lazy(|| {
            ddml.get::<DivBufShared, DivBuf>(&drp)
        })).unwrap_err();
        assert_eq!(err, Error::ECKSUM);
    }

    #[test]
    fn list_closed_zones() {
        let cache = Cache::default();
        let mut pool = Pool::default();

        // The first cluster has two closed zones
        let clz0 = ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5, zid: 0,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz0_1 = clz0.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(0))
            .return_once(move |_, _| {
                let next = Some((0, 11));
                Box::new(Ok((Some(clz0_1), next)).into_future())
            });

        let clz1 = ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6, zid: 1,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3)};
        let clz1_1 = clz1.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(11))
            .return_once(move |_, _| {
                let next = Some((0, 31));
                Box::new(Ok((Some(clz1_1), next)).into_future())
            });

        pool.expect_find_closed_zone()
            .with(eq(0), eq(31))
            .return_once(|_, _| Box::new(Ok((None, Some((1, 0)))).into_future()));

        // The second cluster has no closed zones
        pool.expect_find_closed_zone()
            .with(eq(1), eq(0))
            .return_once(|_, _| Box::new(Ok((None, Some((2, 0)))).into_future()));

        // The third cluster has one closed zone
        let clz2 = ClosedZone{pba: PBA::new(2, 10), freed_blocks: 5, zid: 2,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz2_1 = clz2.clone();
        pool.expect_find_closed_zone()
            .with(eq(2), eq(0))
            .return_once(move |_, _|
            Box::new(Ok((Some(clz2_1), Some((2, 11)))).into_future()));

        pool.expect_find_closed_zone()
            .with(eq(2), eq(11))
            .return_once(|_, _| Box::new(Ok((None, None)).into_future()));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let mut cache = Cache::default();
        let mut pool = Pool::default();
        cache.expect_remove()
            .once()
            .with(eq(Key::PBA(pba)))
            .returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0));
    }

    #[test]
    fn pop_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let mut seq = Sequence::new();
        let mut cache = Cache::default();
        let mut pool = Pool::default();
        cache.expect_remove()
            .once()
            .with(eq(Key::PBA(pba)))
            .return_once(|_| None);
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::new(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
        })).unwrap();
    }

    #[test]
    fn pop_ecksum() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let mut cache = Cache::default();
        let mut pool = Pool::default();
        cache.expect_remove()
            .once()
            .with(eq(Key::PBA(pba)))
            .return_once(|_| None);
        pool.expect_read()
            .with(always(), eq(pba))
            .return_once(|_, _| Box::new(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let mut seq = Sequence::new();
        let cache = Cache::default();
        let mut pool = Pool::default();
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::new(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::new(Ok(()).into_future()));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            ddml.pop_direct::<DivBufShared>(&drp)
        })).unwrap();
    }

    #[test]
    fn put() {
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .once()
            .with(eq(Key::PBA(pba)), always())
            .return_const(());
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::new(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .once()
            .with(eq(Key::PBA(pba)), always())
            .return_const(());
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::new(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
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
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .once()
            .with(eq(Key::PBA(pba)), always())
            .return_const(());
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::new(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; 8192];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let mut rt = current_thread::Runtime::new().unwrap();
        let drp = rt.block_on(
            ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
        ).unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
    }

    #[test]
    fn put_partial_lba() {
        let mut cache = Cache::default();
        let pba = PBA::default();
        cache.expect_insert()
            .once()
            .with(eq(Key::PBA(pba)), always())
            .return_const(());
        let mut pool = Pool::default();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::new(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let cache = Cache::default();
        let pba = PBA::default();
        let mut pool = Pool::default();
        let txg = TxgT::from(42);
        pool.expect_write()
            .with(always(), eq(txg))
            .return_once(move |_, _| Box::new(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
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
        let cache = Cache::default();
        let mut pool = Pool::default();
        pool.expect_sync_all()
            .return_once(|| Box::new(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        assert!(rt.block_on(ddml.sync_all(TxgT::from(0))).is_ok());
    }
}
}
// LCOV_EXCL_STOP
