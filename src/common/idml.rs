// vim: tw=80
///! Indirect Data Management Layer
///
/// Interface for working with indirect records.  An indirect record is a record
/// that is referenced by an immutable Record ID, rather than a disk address.
/// Unlike a direct record, it may be duplicated, by through snapshots, clones,
/// or deduplication.

// use the atomic crate since libstd's AtomicU64 type is still unstable
// https://github.com/rust-lang/rust/issues/32976
use atomic::{Atomic, Ordering};
use common::{
    *,
    dml::*,
    ddml::DRP,
    cache::{Cacheable, CacheRef, Key},
    tree::*
};
use futures::{Future, IntoFuture, Stream, future};
use nix::{Error, errno::Errno};
use std::sync::{Arc, Mutex};

pub use common::ddml::ClosedZone;

#[cfg(not(test))]
use common::cache::Cache;
#[cfg(test)]
use common::cache_mock::CacheMock as Cache;
#[cfg(not(test))]
use common::ddml::DDML;
#[cfg(test)]
use common::ddml_mock::DDMLMock as DDML;

/// a Record that can only have a single reference
pub struct DirectRecord(DRP);

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct RidtEntry {
    drp: DRP,
    refcount: u64
}

impl RidtEntry {
    fn new(drp: DRP) -> Self {
        RidtEntry{drp, refcount: 1}
    }
}

pub type DTree<K, V> = Tree<DRP, DDML, K, V>;

/// Indirect Data Management Layer for a single `Pool`
pub struct IDML {
    cache: Arc<Mutex<Cache>>,

    ddml: Arc<DDML>,

    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    // TODO: consider a lazy delete strategy to reduce the amount of tree
    // activity on pop/delete by deferring alloct removals to the cleaner.
    alloct: DTree<PBA, RID>,

    /// Holds the next RID to allocate.  They are never reused.
    next_rid: Atomic<u64>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    ridt: DTree<RID, RidtEntry>
}

impl<'a> IDML {
    pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self {
        let alloct = DTree::<PBA, RID>::create(ddml.clone());
        let next_rid = Atomic::new(0);
        let ridt = DTree::<RID, RidtEntry>::create(ddml.clone());
        IDML{alloct, cache, ddml, next_rid, ridt}
    }

    pub fn list_closed_zones(&'a self) -> Box<Iterator<Item=ClosedZone> + 'a> {
        unimplemented!()
    }

    /// Return a list of all active (not delete) Records that have been written
    /// to the IDML in the given Zone.
    ///
    /// This list should be persistent across reboots.
    pub fn list_records(&self, _zone: &ClosedZone)
        -> Box<Stream<Item=DirectRecord, Error=Error>>
    {
        unimplemented!()
    }

    pub fn move_record(&self, _record: DirectRecord)
        -> Box<Future<Item=(), Error=Error>>
    {
        unimplemented!()
    }
}

impl DML for IDML {
    type Addr = RID;

    fn delete<'a>(&'a self, rid: &Self::Addr)
        -> Box<Future<Item=(), Error=Error> + 'a>
    {
        let rid2 = rid.clone();
        let fut = self.ridt.get(rid.clone())
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    self.cache.lock().unwrap().remove(&Key::Rid(rid2));
                    // TODO: usd ddml.delete_direct
                    let ddml_fut = self.ddml.delete(&entry.drp);
                    let alloct_fut = self.alloct.remove(entry.drp.pba());
                    let ridt_fut = self.ridt.remove(rid2);
                    Box::new(
                        ddml_fut.join3(alloct_fut, ridt_fut)
                             .map(|(_, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                             })
                     ) as Box<Future<Item=(), Error=Error>>
                } else {
                    let ridt_fut = self.ridt.insert(rid2, entry)
                        .map(|_| ());
                    Box::new(ridt_fut)
                    as Box<Future<Item=(), Error=Error>>
                }
            });
        Box::new(fut)
    }

    fn evict(&self, rid: &Self::Addr) {
        self.cache.lock().unwrap().remove(&Key::Rid(*rid));
    }

    fn get<'a, T: Cacheable, R: CacheRef>(&'a self, rid: &Self::Addr)
        -> Box<Future<Item=Box<R>, Error=Error> + 'a>
    {
        self.cache.lock().unwrap().get::<R>(&Key::Rid(*rid)).map(|t| {
            let r : Box<Future<Item=Box<R>, Error=Error>> =
            Box::new(future::ok::<Box<R>, Error>(t));
            r
        }).unwrap_or_else(|| {
            let rid2 = rid.clone();
            let fut = self.ridt.get(*rid)
                .and_then(|r| {
                    match r {
                        None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                        Some(entry) => Ok(entry).into_future()
                    }
                }).and_then(move |entry| {
                    self.ddml.get_direct(&entry.drp)
                }).map(move |cacheable: Box<T>| {
                    let r = cacheable.make_ref();
                    let key = Key::Rid(rid2);
                    self.cache.lock().unwrap().insert(key, cacheable);
                    r.downcast::<R>().unwrap()
                });
            Box::new(fut)
        })
    }

    fn pop<'a, T: Cacheable, R: CacheRef>(&'a self, rid: &Self::Addr)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a>
    {
        let rid2 = rid.clone();
        let fut = self.ridt.get(rid.clone())
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(Errno::ENOENT)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    let cacheval = self.cache.lock().unwrap()
                        .remove(&Key::Rid(rid2));
                    let bfut: Box<Future<Item=Box<T>, Error=Error>> = cacheval
                        .map(|cacheable| {
                            let t = cacheable.downcast::<T>().unwrap();
                            Box::new(self.ddml.delete(&entry.drp)
                                              .map(move |_| t)
                            ) as Box<Future<Item=Box<T>, Error=Error>>
                        }).unwrap_or_else(||{
                            Box::new(self.ddml.pop_direct::<T>(&entry.drp))
                        });
                    let alloct_fut = self.alloct.remove(entry.drp.pba());
                    let ridt_fut = self.ridt.remove(rid2);
                    Box::new(
                        bfut.join3(alloct_fut, ridt_fut)
                             .map(|(cacheable, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                                 cacheable
                             })
                     ) as Box<Future<Item=Box<T>, Error=Error>>
                } else {
                    let cacheval = self.cache.lock().unwrap()
                        .get::<R>(&Key::Rid(rid2));
                    let bfut = cacheval.map(|cacheref: Box<R>|{
                        let t = cacheref.to_owned().downcast::<T>().unwrap();
                        Box::new(future::ok(t))
                            as Box<Future<Item=Box<T>, Error=Error>>
                    }).unwrap_or_else(|| {
                        Box::new(self.ddml.get_direct::<T>(&entry.drp))
                    });
                    let ridt_fut = self.ridt.insert(rid2, entry);
                    Box::new(
                        bfut.join(ridt_fut)
                            .map(|(cacheable, _)| {
                                cacheable
                            })
                    ) as Box<Future<Item=Box<T>, Error=Error>>
                }
            });
        Box::new(fut)
    }

    fn put<'a, T: Cacheable>(&'a self, cacheable: T, compression: Compression)
        -> (Self::Addr, Box<Future<Item=(), Error=Error> + 'a>)
    {
        // Outline:
        // 1) Write to the DDML
        // 2) Cache
        // 3) Add entry to the RIDT
        // 4) Add reverse entry to the AllocT
        let (drp, ddml_fut) = self.ddml.put_direct(cacheable, compression);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct_fut = self.alloct.insert(drp.pba(), rid.clone());
        let rid_entry = RidtEntry::new(drp);
        let ridt_fut = self.ridt.insert(rid.clone(), rid_entry);
        let fut = Box::new(
            ddml_fut.join3(ridt_fut, alloct_fut)
                .map(move |(cacheable, old_rid_entry, old_alloc_entry)| {
                    assert!(old_rid_entry.is_none(), "RID was not unique");
                    assert!(old_alloc_entry.is_none(), concat!(
                        "Double allocate without free.  ",
                        "DDML allocator leak detected!"));
                    self.cache.lock().unwrap().insert(Key::Rid(rid),
                        Box::new(cacheable));
                })
        );
        (rid, fut)
    }

    fn sync_all<'a>(&'a self) -> Box<Future<Item=(), Error=Error> + 'a>
    {
        Box::new(
            self.ridt.flush()
                .join(self.alloct.flush())
                .and_then(move |(_, _)| self.ddml.sync_all())
        )
    }
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {

    use super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use simulacrum::*;
    use simulacrum::validators::trivial::any;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::Mutex
    };
    use tokio::runtime::current_thread;

    #[test]
    fn delete_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::new();
        ddml.expect_delete()
            .called_once()
            .with(passes(move |key: &*const DRP| unsafe {**key == drp}))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT and AllocT
        rt.block_on(idml.ridt.insert(rid.clone(), RidtEntry::new(drp2)))
            .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())).unwrap();

        rt.block_on(idml.delete(&rid)).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn delete_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let cache = Cache::new();
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        // Inject the address into the RIDT and AllocT
        let entry = RidtEntry{drp: drp2, refcount: 2};
        rt.block_on(idml.ridt.insert(rid.clone(), entry)) .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())) .unwrap();

        rt.block_on(idml.delete(&rid)).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp2);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn evict() {
        let rid = RID(42);
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        idml.evict(&rid);
    }

    #[test]
    fn get_hot() {
        let rid = RID(42);
        let mut cache = Cache::new();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn get_cold() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        let owned_by_cache = Rc::new(
            RefCell::new(Vec::<Box<Cacheable>>::new())
        );
        let owned_by_cache2 = owned_by_cache.clone();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| None);
        cache.then().expect_insert()
            .called_once()
            .with(passes(move |args: &(Key, _)| {
                args.0 == Key::Rid(RID(42))
            })).returning(move |(_, dbs)| {;
                owned_by_cache2.borrow_mut().push(dbs);
            });
        let mut ddml = DDML::new();
        ddml.expect_get_direct::<DivBufShared>()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {**key == drp}
            })).returning(move |_| {
                let dbs = Box::new(DivBufShared::from(vec![0u8; 4096]));
                Box::new(future::ok::<Box<DivBufShared>, Error>(dbs))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT
        rt.block_on(idml.ridt.insert(rid.clone(), RidtEntry::new(drp2)))
            .unwrap();

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn pop_hot_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::new();
        ddml.expect_delete()
            .called_once()
            .with(passes(move |key: &*const DRP| unsafe {**key == drp}))
            .returning(|_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT and AllocT
        rt.block_on(idml.ridt.insert(rid.clone(), RidtEntry::new(drp2)))
            .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())).unwrap();

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid);
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn pop_hot_notlast() {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try().unwrap()))
            });
        let ddml = DDML::new();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT and AllocT
        let entry = RidtEntry{drp: drp2, refcount: 2};
        rt.block_on(idml.ridt.insert(rid.clone(), entry)) .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())).unwrap();

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid);
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp2);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn pop_cold_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::new();
        ddml.expect_pop_direct::<DivBufShared>()
            .called_once()
            .with(passes(move |key: &*const DRP| unsafe {**key == drp}))
            .returning(|_| {
                let dbs = DivBufShared::from(vec![42u8; 4096]);
                Box::new(future::ok::<Box<DivBufShared>, Error>(Box::new(dbs)))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT and AllocT
        rt.block_on(idml.ridt.insert(rid.clone(), RidtEntry::new(drp2)))
            .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())) .unwrap();

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid);
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.ridt.get(rid)).unwrap().is_none());
        assert!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().is_none());
    }

    #[test]
    fn pop_cold_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = drp.clone();
        let mut cache = Cache::new();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::new();
        ddml.expect_get_direct()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {**key == drp}
            })).returning(move |_| {
                let dbs = Box::new(DivBufShared::from(vec![42u8; 4096]));
                Box::new(future::ok::<Box<DivBufShared>, Error>(dbs))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject the address into the RIDT and AllocT
        let entry = RidtEntry{drp: drp2, refcount: 2};
        rt.block_on(idml.ridt.insert(rid.clone(), entry)) .unwrap();
        rt.block_on(idml.alloct.insert(drp2.pba(), rid.clone())) .unwrap();

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid);
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp2);
        assert_eq!(entry2.refcount, 1);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
            rid);
    }

    #[test]
    fn put() {
        let mut cache = Cache::new();
        let mut ddml = DDML::new();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdeadbeef);
        let rid = RID(0);
        cache.expect_insert()
            .called_once()
            .with(params!(Key::Rid(rid), any()))
            .returning(|_| ());
        ddml.expect_put_direct::<DivBufShared>()
            .called_once()
            .with(params!(any(), any()))
            .returning(move |(buf, _)|
                       (drp, Box::new(future::ok::<DivBufShared, Error>(buf)))
            );
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let (actual_rid, fut) = idml.put(dbs, Compression::None);
        assert_eq!(rid, actual_rid);
        rt.block_on(fut).unwrap();

        // Now verify the contents of the RIDT and AllocT
        let entry = rt.block_on(idml.ridt.get(actual_rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp);
        assert_eq!(rt.block_on(idml.alloct.get(drp.pba())).unwrap().unwrap(),
                   actual_rid);
    }

    #[ignore = "Simulacrum can't mock a single generic method with different type parameters more than once in the same test"]
    #[test]
    fn sync_all() {
        let rid = RID(42);
        let cache = Cache::new();
        let mut ddml = DDML::new();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdeadbeef);
        ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            .called_any()
            .with(params!(any(), any()))
            .returning(move |(_, _)|
                (DRP::random(Compression::None, 4096),
                 Box::new(future::ok::<(), Error>(())))
            );
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .called_any()
            .with(params!(any(), any()))
            .returning(move |(_, _)|
                (DRP::random(Compression::None, 4096),
                 Box::new(future::ok::<(), Error>(())))
            );
        ddml.expect_sync_all()
            .called_once()
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject some data into the RIDT and AllocT
        let entry = RidtEntry{drp: drp, refcount: 2};
        rt.block_on(idml.ridt.insert(rid.clone(), entry)) .unwrap();
        rt.block_on(idml.alloct.insert(drp.pba(), rid.clone())) .unwrap();

        rt.block_on(idml.sync_all()).unwrap();
    }
}
