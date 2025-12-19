// vim: tw=80
use crate::{
    cache::{self, Cache, Key},
    dml::*,
    label::*,
    pool::ClosedZone,
    types::*,
    util::*,
    writeback::Credit
};
#[cfg(test)]
use crate::vdev::BoxVdevFut;

use divbuf::DivBufShared;
use futures::{Future, FutureExt, TryFutureExt, future};
use futures_locks::{RwLock, RwLockReadGuard};
use metrohash::MetroHash64;
#[cfg(test)] use mockall::mock;
use std::{
    borrow,
    hash::Hasher,
    mem,
    pin::Pin,
    sync::{Arc, Mutex}
};
use super::{DRP, Status};
use tracing::instrument;
use tracing_futures::Instrument;


#[cfg(not(test))] use crate::pool::Pool;
#[cfg(test)] use crate::pool::MockPool as Pool;

/// Return type of [`DDML::list_closed_zones`]
pub struct ListClosedZones {
    pg: RwLockReadGuard<Pool>,
    next_cluster: ClusterT,
    next_zone: ZoneT
}

impl Iterator for ListClosedZones {
    type Item = ClosedZone;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.pg.find_closed_zone(self.next_cluster, self.next_zone) {
                (Some(pclz), Some((c, z))) => {
                    self.next_cluster = c;
                    self.next_zone = z;
                    break Some(pclz);
                },
                (Some(_), None) => unreachable!(),  // LCOV_EXCL_LINE
                (None, Some((c, z))) => {
                    self.next_cluster = c;
                    self.next_zone = z;
                    continue;
                },
                (None, None) => {break None;}
            }
        }
    }
}

/// Direct Data Management Layer for a single `Pool`
pub struct DDML {
    // Sadly, the Cache needs to be Mutex-protected because updating the LRU
    // list requires exclusive access.  It can be a normal Mutex instead of a
    // futures_lock::Mutex, because we will never need to block while holding
    // this lock.
    cache: Arc<Mutex<Cache>>,
    pool: RwLock<Pool>,
    pool_name: String
}

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
impl DDML {
    /// Cleanup stuff from the previous transaction.
    pub fn advance_transaction(&self, txg: TxgT)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        self.pool.read()
        .then(move |pool| pool.advance_transaction(txg))
    }

    /// Assert that the given zone was clean as of the given transaction
    #[cfg(debug_assertions)]
    pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT)
        -> impl Future<Output=()> + Send + Sync
    {
        self.pool.read()
            .map(move |pool| pool.assert_clean_zone(cluster, zone, txg))
    }

    /// Free a record's storage, ignoring the Cache
    pub fn delete_direct(&self, drp: &DRP, _txg: TxgT)
        -> impl Future<Output=()> + Send + Sync
    {
        let pba = drp.pba;
        let asize = drp.asize();
        self.pool.read()
            .map(move |pool| pool.free(pba, asize))
    }

    pub async fn dump_fsm(&self) -> Vec<String> {
        self.pool.read().await.dump_fsm()
    }

    /// Fault the given disk or mirror
    pub async fn fault(&self, uuid: Uuid) -> Result<()> {
        self.pool.write().await.fault(uuid).await
    }

    pub fn flush(&self, idx: u32)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        self.pool.read()
            .then(move |pool| pool.flush(idx))
    }

    pub fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        let pool_name = pool.name().to_owned();
        DDML{pool: RwLock::new(pool), cache, pool_name}
    }

    /// Get directly from disk, bypassing cache
    #[instrument(skip(self, drp))]
    pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Output=Result<Box<T>>> + Send
    {
        let drp = *drp;
        self.pool.read()
        .then(move |pg| Self::read(pg, drp))
        .map_ok(move |(_, dbs)| Box::new(T::deserialize(dbs)))
    }

    /// List all closed zones in the `DDML` in no particular order
    pub fn list_closed_zones(&self)
        -> impl Future<Output=ListClosedZones> + Send
    {
        let next_cluster = 0;
        let next_zone = 0;
        self.pool.read()
        .map(move |pg| ListClosedZones{pg, next_cluster, next_zone})
    }

    /// Read a record from disk
    #[instrument(skip(pg))]
    fn read(pg: RwLockReadGuard<Pool>, drp: DRP)
        -> impl Future<Output=Result<(RwLockReadGuard<Pool>, DivBufShared)>> + Send
    {
        // Outline
        // 1) Read
        // 2) Truncate
        // 3) Verify checksum
        // 4) Decompress
        let len = drp.asize() as usize * BYTES_PER_LBA;
        let dbs = DivBufShared::uninitialized(len);
        let dbm = dbs.try_mut().unwrap();
        // Read
        pg.read(dbm, drp.pba)
        .and_then(move |_| {
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
                    let r = Compression::decompress(&db, drp.lsize)
                        .map(|unx| (pg, unx));
                    future::ready(r)
                } else {
                    future::ok((pg, dbs))
                }.boxed()
            } else {
                tracing::warn!("Checksum mismatch");
                pg.read_long(drp.asize(), drp.pba)
                .map(move |r| {
                    match r {
                        Ok(reconstructions) => {
                            for rdbs in reconstructions {
                                let rdb = rdbs.try_const().unwrap();
                                let mut hasher = MetroHash64::new();
                                checksum_iovec(&rdb, &mut hasher);
                                let checksum = hasher.finish();
                                if checksum == drp.checksum {
                                    // Somehow fault the stuff in bad
                                    let dbs = if drp.is_compressed() {
                                        let r = Compression::decompress(&rdb,
                                            drp.lsize);
                                        match r {
                                            Ok(dbs) => dbs,
                                            Err(_) => { continue; }
                                        }
                                    } else {
                                        let mut dbm = dbs.try_mut().unwrap();
                                        dbm[..].copy_from_slice(&rdb[..]);
                                        dbs
                                    };
                                    return Ok((pg, dbs));
                                }
                            }
                            Err(Error::EINTEGRITY)
                        },
                        Err(e) => Err(e)
                    }
                }).boxed()
            }
        })
    }

    /// Open an existing `DDML` from its underlying `Pool`.
    ///
    /// # Parameters
    ///
    /// * `cache`:      An already constructed `Cache`
    /// * `pool`:       An already constructed `Pool`
    pub fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self {
        DDML::new(pool, cache)
    }

    /// Read a record and return ownership of it, bypassing Cache
    #[instrument(skip(self, drp))]
    pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
        -> impl Future<Output=Result<Box<T>>> + Send
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        let drp2 = *drp;
        self.pool.read()
        .then(move |pg| {
            Self::read(pg, drp2)
            .map_ok(move |(pg, dbs)| {
                pg.free(pba, lbas);
                Box::new(T::deserialize(dbs))
            })
        })
    }

    pub fn pool_name(&self) -> &str {
        self.pool_name.as_str()
    }

    /// Does most of the work of DDML::put
    fn put_common<T>(&self, cacheref: &T, compression: Compression,
                     txg: TxgT)
        -> impl Future<Output=Result<DRP>> + Send
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
        assert!(serialized.len() < u32::MAX as usize,
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
        self.pool.read()
        .then(move |pool| pool.write(compressed_db, txg))
        .map_ok(move |pba| {
            DRP { pba, compressed, lsize: lsize as u32, csize, checksum }
        })
    }

    /// Write a buffer bypassing cache.  Return the same buffer
    pub fn put_direct<T>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
        -> impl Future<Output=Result<DRP>> + Send
        where T: borrow::Borrow<dyn CacheRef>
    {
        self.put_common(cacheref, compression, txg)
    }

    /// Return approximately the usable storage space in LBAs.
    // NB: consider combining into one method with DDML::used, because they're
    // frequently used together, to reduce lock activity.
    pub fn size(&self) -> impl Future<Output=LbaT> + Send {
        self.pool.read().map(|p| p.size())
    }

    pub fn status(&self) -> impl Future<Output=Status> + Send {
        self.pool.read().map(|p| p.status())
    }

    /// How many blocks are currently used?
    pub fn used(&self) -> impl Future<Output=LbaT> + Send {
        self.pool.read().map(|p| p.used())
    }

    pub fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Output=Result<()>> + Send
    {
        self.pool.read()
            .then(move |pool| pool.write_label(labeller))
    }
}

impl DML for DDML {
    type Addr = DRP;

    fn delete(&self, drp: &DRP, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        let pba = drp.pba;
        let asize = drp.asize();
        self.cache.lock().unwrap().remove(&Key::PBA(pba));
        let fut = self.pool.read()
            .map(move |pool| {pool.free(pba, asize); Ok(())});
        Box::pin(fut)
    }

    fn evict(&self, drp: &DRP) {
        self.cache.lock().unwrap().remove(&Key::PBA(drp.pba));
    }

    #[instrument(skip(self))]
    fn get<T: Cacheable, R: CacheRef>(&self, drp: &DRP)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        // Outline:
        // 1) Fetch from cache, or
        // 2) Wait on any pending cache insertions, or
        // 3) Read from disk, then insert into cache, then notify waiters
        let pba = drp.pba;
        let key = Key::PBA(pba);

        cache::get_or_insert!(T, R, &self.cache, key,
            self.get_direct(drp)
        )
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, drp: &DRP, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        let lbas = drp.asize();
        let pba = drp.pba;
        self.cache.lock().unwrap().remove(&Key::PBA(pba)).map(|cacheable| {
            let t = cacheable.downcast::<T>().unwrap();
            self.pool.read()
                .map(move |pg| {pg.free(pba, lbas); Ok(t)})
                .boxed()
        }).unwrap_or_else(|| {
            Box::pin( self.pop_direct::<T>(drp)) as Pin<Box<_>>
        })
    }

    #[instrument(skip(self))]
    fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                             txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<<Self as DML>::Addr>> + Send>>
    {
        let cache2 = self.cache.clone();
        let db = cacheable.make_ref();
        let fut = self.put_common(&db, compression, txg)
            .map_ok(move |drp|{
                let pba = drp.pba();
                cache2.lock().unwrap()
                    .insert(Key::PBA(pba), Box::new(cacheable));
                drp
            }).in_current_span();
        Box::pin(fut)
    }

    fn repay(&self, credit: Credit) {
        // Writes to the DDML should never attempt to borrow credit.  That could
        // lead to deadlocks.
        debug_assert!(credit.is_null());
        mem::forget(credit);
    }

    fn sync_all(&self, _txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        let fut = self.pool.read()
            .then(|p| p.sync_all());
        Box::pin(fut)
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub DDML {
        pub fn advance_transaction(&self, txg: TxgT) -> BoxVdevFut;
        pub fn assert_clean_zone(&self, cluster: ClusterT, zone: ZoneT, txg: TxgT) -> Pin<Box<dyn Future<Output=()> + Send + Sync>>;
        pub fn delete_direct(&self, drp: &DRP, txg: TxgT) -> Pin<Box<dyn Future<Output=()> + Send>>;
        pub async fn dump_fsm(&self) -> Vec<String>;
        pub async fn fault(&self, uuid: Uuid) -> Result<()>;
        pub fn flush(&self, idx: u32) -> BoxVdevFut;
        pub fn new(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        pub fn get_direct<T: Cacheable>(&self, drp: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        pub fn list_closed_zones(&self)
            -> Pin<Box<dyn Future<Output=Box<dyn Iterator<Item=ClosedZone> + Send>> + Send>>;
        pub fn open(pool: Pool, cache: Arc<Mutex<Cache>>) -> Self;
        pub fn pool_name(&self) -> &str;
        pub fn pop_direct<T: Cacheable>(&self, drp: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        pub fn put_direct<T>(&self, cacheref: &T, compression: Compression,
                         txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<DRP>> + Send>>
            where T: borrow::Borrow<dyn CacheRef> + 'static;
        pub fn size(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
        pub fn status(&self) -> Pin<Box<dyn Future<Output=Status> + Send>>;
        pub fn used(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
        pub fn write_label(&self, labeller: LabelWriter)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
    }
    impl DML for DDML {
        type Addr = DRP;

        fn delete(&self, addr: &DRP, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        fn evict(&self, addr: &DRP);
        fn get<T: Cacheable, R: CacheRef>(&self, addr: &DRP)
            -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>;
        fn pop<T: Cacheable, R: CacheRef>(&self, rid: &DRP, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                                 txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<DRP>> + Send>>;
        fn repay(&self, credit: Credit);
        fn sync_all(&self, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
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
    use divbuf::{DivBuf, DivBufShared};
    use futures::{
        future,
        channel::oneshot
    };
    use mockall::{
        Sequence,
        predicate::*
    };
    use pretty_assertions::assert_eq;
    use rand::{RngCore, SeedableRng};
    use rand_xorshift::XorShiftRng;

    fn mock_pool() -> Pool {
        let mut pool = Pool::default();
        pool.expect_name()
            .return_const("Foo".to_string());
        pool
    }

    #[test]
    fn delete_hot() {
        let mut seq = Sequence::new();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let mut pool = mock_pool();
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| ());

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.delete(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn evict() {
        let pba = PBA::default();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let key = Key::PBA(pba);
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let pool = mock_pool();

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        ddml.evict(&drp);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
    }

    #[test]
    fn get_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();
        pool.expect_read()
            .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.get_direct::<DivBufShared>(&drp)
            .now_or_never().unwrap()
            .unwrap();
    }

    mod get {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Near-simultaneous get requests should not result in multiple reads
        /// from disk.
        #[tokio::test]
        async fn duplicate() {
            let pba = PBA::default();
            let key = Key::PBA(pba);
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
            let (tx, rx) = oneshot::channel::<()>();
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .once()
                .return_once(move |mut dbm, _pba| {
                    for x in dbm.iter_mut() {
                        *x = 0;
                    }
                    Box::pin(rx.map_err(Error::unhandled_error))
                });

            let amcache = Arc::new(Mutex::new(cache));
            let ddml = DDML::new(pool, amcache.clone());
            let fut1 = ddml.get::<DivBufShared, DivBuf>(&drp);
            let fut2 = ddml.get::<DivBufShared, DivBuf>(&drp);
            tx.send(()).unwrap();
            future::try_join(fut1, fut2).await.unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }

        #[test]
        fn hot() {
            let pba = PBA::default();
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 4096, checksum: 0};
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(Key::PBA(pba), Box::new(dbs));
            let pool = mock_pool();

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
        }

        #[test]
        fn cold() {
            let pba = PBA::default();
            let key = Key::PBA(pba);
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .once()
                .returning(|mut dbm, _pba| {
                    for x in dbm.iter_mut() {
                        *x = 0;
                    }
                    Box::pin(future::ok::<(), Error>(()))
                });

            let amcache = Arc::new(Mutex::new(cache));
            let ddml = DDML::new(pool, amcache.clone());
            ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }

        #[test]
        fn nonredundant_eintegrity() {
            let pba = PBA::default();
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xdead_beef_dead_beef};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            pool.expect_read_long()
                .withf(|len, pba| *len == 1 && *pba == PBA::default())
                .times(1)
                .return_once(|_, _| future::err(Error::EINTEGRITY).boxed());

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            let err = ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap_err();
            assert_eq!(err, Error::EINTEGRITY);
        }

        /// A read yields a checksum error.  A subsequent read_long finds the
        /// correct data.
        #[test]
        fn redundant_eintegrity() {
            let pba = PBA::default();
            let csize = 1usize;
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: csize as u32, checksum: 0xe7f_1596_6a3d_61f8};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .returning(|mut dbm, _| {
                    for x in dbm.iter_mut() {
                        *x = b'X';
                    }
                    Box::pin(future::ok::<(), Error>(()))
                });
            pool.expect_read_long()
                .withf(|len, pba| *len == 1 && *pba == PBA::default())
                .times(1)
                .return_once(move |_, _| {
                    let dbs0 = DivBufShared::from(vec![1u8; csize]);
                    let dbs1 = DivBufShared::from(vec![0u8; csize]);
                    let reconstructions = Box::new(vec![dbs0, dbs1].into_iter())
                        as Box<dyn Iterator<Item=DivBufShared> + Send>;
                    Box::new(future::ok(reconstructions)).boxed()
                });

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            let db = ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(&db[..], &vec![0u8; csize]);
        }

        /// A read yields a checksum error.  A subsequent read_long yields other
        /// possibilities, but they're all wrong.
        #[test]
        fn redundant_eintegrity_all_wrong() {
            let pba = PBA::default();
            let drp = DRP{pba, compressed: false, lsize: 4096,
                          csize: 1, checksum: 0xdead_beef_dead_beef};
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(|dbm, pba| dbm.len() == 4096 && *pba == PBA::default())
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            pool.expect_read_long()
                .withf(|len, pba| *len == 1 && *pba == PBA::default())
                .times(1)
                .return_once(|_, _| {
                    let dbs0 = DivBufShared::from(vec![0u8; BYTES_PER_LBA]);
                    let dbs1 = DivBufShared::from(vec![1u8; BYTES_PER_LBA]);
                    let reconstructions = Box::new(vec![dbs0, dbs1].into_iter())
                        as Box<dyn Iterator<Item=DivBufShared> + Send>;
                    Box::new(future::ok(reconstructions)).boxed()
                });

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            let err = ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap_err();
            assert_eq!(err, Error::EINTEGRITY);
        }

        /// A read yields a checksum error.  A subsequent read_long finds the
        /// correct data.
        #[test]
        fn redundant_eintegrity_compressed() {
            let lsize = 2 * BYTES_PER_LBA;
            let raw = DivBufShared::from(vec![0u8; lsize]);
            let db = raw.try_const().unwrap();
            let (compressed, compression) = Compression::LZ4(None).compress(db);
            assert!(compression != Compression::None);
            let mut hasher = MetroHash64::new();
            checksum_iovec(&compressed, &mut hasher);
            let checksum = hasher.finish();

            let pba = PBA::default();
            let csize = compressed.len();
            let drp = DRP{pba, compressed: true, lsize: lsize as u32,
                          csize: csize as u32, checksum};
            let asize = drp.asize() as usize * BYTES_PER_LBA;
            let cache = Cache::with_capacity(1_048_576);
            let mut pool = mock_pool();
            pool.expect_read()
                .withf(move |dbm, pba| dbm.len() == asize && *pba == PBA::default())
                .return_once(|mut dbm, _| {
                    for x in dbm.iter_mut() {
                        *x = 0;
                    }
                    Box::pin(future::ok::<(), Error>(()))
                });
            pool.expect_read_long()
                .withf(|len, pba| *len == 1 && *pba == PBA::default())
                .times(1)
                .return_once(move |_, _| {
                    let dbs0 = DivBufShared::from(vec![1u8; csize]);
                    let dbs1 = DivBufShared::from(Vec::from(&compressed[..]));
                    let reconstructions = Box::new(vec![dbs0, dbs1].into_iter())
                        as Box<dyn Iterator<Item=DivBufShared> + Send>;
                    Box::new(future::ok(reconstructions)).boxed()
                });

            let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
            let db = ddml.get::<DivBufShared, DivBuf>(&drp)
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(db.len(), raw.len());
            assert_eq!(&db[..], &raw.try_const().unwrap()[..]);
        }
    }

    #[tokio::test]
    async fn list_closed_zones() {
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();

        // The first cluster has two closed zones
        let clz0 = ClosedZone{pba: PBA::new(0, 10), freed_blocks: 5, zid: 0,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz0_1 = clz0.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(0))
            .return_once(move |_, _| (Some(clz0_1), Some((0, 11))));

        let clz1 = ClosedZone{pba: PBA::new(0, 30), freed_blocks: 6, zid: 1,
            total_blocks: 10, txgs: TxgT::from(2)..TxgT::from(3)};
        let clz1_1 = clz1.clone();
        pool.expect_find_closed_zone()
            .with(eq(0), eq(11))
            .return_once(move |_, _| (Some(clz1_1), Some((0, 31))));

        pool.expect_find_closed_zone()
            .with(eq(0), eq(31))
            .return_once(|_, _| (None, Some((1, 0))));

        // The second cluster has no closed zones
        pool.expect_find_closed_zone()
            .with(eq(1), eq(0))
            .return_once(|_, _| (None, Some((2, 0))));

        // The third cluster has one closed zone
        let clz2 = ClosedZone{pba: PBA::new(2, 10), freed_blocks: 5, zid: 2,
            total_blocks: 10, txgs: TxgT::from(0)..TxgT::from(1)};
        let clz2_1 = clz2.clone();
        pool.expect_find_closed_zone()
            .with(eq(2), eq(0))
            .return_once(move |_, _| (Some(clz2_1), Some((2, 11))));

        pool.expect_find_closed_zone()
            .with(eq(2), eq(11))
            .return_once(|_, _| (None, None));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));

        let closed_zones: Vec<ClosedZone> = ddml.list_closed_zones()
            .await
            .collect();
        let expected = vec![clz0, clz1, clz2];
        assert_eq!(closed_zones, expected);
    }

    #[test]
    fn pop_hot() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 4096, checksum: 0};
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let key = Key::PBA(pba);
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(Key::PBA(pba), Box::new(dbs));
        let mut pool = mock_pool();
        pool.expect_free()
            .with(eq(pba), eq(1))
            .return_once(|_, _| ());

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());

    }

    #[test]
    fn pop_cold() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let mut seq = Sequence::new();
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| ());

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn pop_eintegrity() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xdead_beef_dead_beef};
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();
        pool.expect_read()
            .with(always(), eq(pba))
            .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
        pool.expect_read_long()
            .withf(|len, pba| *len == 1 && *pba == PBA::default())
            .times(1)
            .return_once(|_, _| future::err(Error::EINTEGRITY).boxed());

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let err = ddml.pop::<DivBufShared, DivBuf>(&drp, TxgT::from(0))
            .now_or_never().unwrap()
            .unwrap_err();
        assert_eq!(err, Error::EINTEGRITY);
    }

    #[test]
    fn pop_direct() {
        let pba = PBA::default();
        let drp = DRP{pba, compressed: false, lsize: 4096,
                      csize: 1, checksum: 0xe7f_1596_6a3d_61f8};
        let mut seq = Sequence::new();
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();
        pool.expect_read()
            .with(always(), eq(pba))
            .once()
            .in_sequence(&mut seq)
            .returning(|mut dbm, _pba| {
                for x in dbm.iter_mut() {
                    *x = 0;
                }
                Box::pin(future::ok::<(), Error>(()))
            });
        pool.expect_free()
            .with(eq(pba), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| ());

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        ddml.pop_direct::<DivBufShared>(&drp)
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn put() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = mock_pool();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let drp = ddml.put(dbs, Compression::None, TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    /// With compression enabled, compressible data should be compressed
    #[test]
    fn put_compressible() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = mock_pool();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 8192]);
        let drp = ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(drp.is_compressed());
        assert!(drp.csize < 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    /// Incompressible data should not be compressed, even when compression is
    /// enabled.
    #[test]
    fn put_incompressible() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = mock_pool();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let mut rng = XorShiftRng::seed_from_u64(12345);
        let mut v = vec![0u8; 8192];
        rng.fill_bytes(&mut v[..]);
        let dbs = DivBufShared::from(v);
        let drp = ddml.put(dbs, Compression::Zstd(None), TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert!(!drp.is_compressed());
        assert_eq!(drp.csize, 8192);
        assert_eq!(drp.lsize, 8192);
        assert_eq!(drp.pba, pba);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    #[test]
    fn put_partial_lba() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let key = Key::PBA(pba);
        let mut pool = mock_pool();
        pool.expect_write()
            .with(always(), eq(TxgT::from(42)))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let amcache = Arc::new(Mutex::new(cache));
        let ddml = DDML::new(pool, amcache.clone());
        let dbs = DivBufShared::from(vec![42u8; 1024]);
        let drp = ddml.put(dbs, Compression::None, TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 1024);
        assert_eq!(drp.lsize, 1024);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    #[test]
    fn put_direct() {
        let cache = Cache::with_capacity(1_048_576);
        let pba = PBA::default();
        let mut pool = mock_pool();
        let txg = TxgT::from(42);
        pool.expect_write()
            .with(always(), eq(txg))
            .return_once(move |_, _| Box::pin(future::ok::<PBA, Error>(pba)));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let db = Box::new(dbs.try_const().unwrap()) as Box<dyn CacheRef>;
        let drp = ddml.put_direct(&db, Compression::None, txg)
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(drp.pba, pba);
        assert_eq!(drp.csize, 4096);
        assert_eq!(drp.lsize, 4096);
    }

    #[test]
    fn sync_all() {
        let cache = Cache::with_capacity(1_048_576);
        let mut pool = mock_pool();
        pool.expect_sync_all()
            .return_once(|| Box::pin(future::ok::<(), Error>(())));

        let ddml = DDML::new(pool, Arc::new(Mutex::new(cache)));
        assert!(ddml.sync_all(TxgT::from(0))
                .now_or_never().unwrap()
                .is_ok());
    }
}
}
// LCOV_EXCL_STOP
