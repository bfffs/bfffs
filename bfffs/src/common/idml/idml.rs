// vim: tw=80

// use the atomic crate since libstd's AtomicU64 type is still unstable
// https://github.com/rust-lang/rust/issues/32976
use atomic::{Atomic, Ordering};
use crate::{
    boxfut,
    common::{
        *,
        dml::*,
        ddml::*,
        cache::{Cache, Cacheable, CacheRef, Key},
        label::*,
        tree::*
    }
};
use futures::{Future, IntoFuture, Stream, future};
use futures_locks::{RwLock, RwLockReadFut};
use std::{
    io,
    sync::{Arc, Mutex},
};
use super::*;

/// Container for the IDML's private trees
struct Trees {
    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    // TODO: consider a lazy delete strategy to reduce the amount of tree
    // activity on pop/delete by deferring alloct removals to the cleaner.
    alloct: DTree<PBA, RID>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    ridt: DTree<RID, RidtEntry>,
}

/// Indirect Data Management Layer for a single `Pool`
pub struct IDML {
    cache: Arc<Mutex<Cache>>,

    ddml: Arc<DDML>,

    /// Holds the next RID to allocate.  They are never reused.
    next_rid: Atomic<u64>,

    /// Current transaction group
    transaction: RwLock<TxgT>,

    // Even though it has a single owner, the tree must be Arc so IDML methods
    // can be 'static
    trees: Arc<Trees>
}

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
impl<'a> IDML {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.ddml.allocated()
    }

    /// Foreground RIDT/AllocT consistency check.
    ///
    /// Checks that the RIDT and AllocT are exact inverses of each other.
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    fn check_ridt(&self) -> impl Future<Item=bool, Error=Error> {
        let trees2 = self.trees.clone();
        let trees3 = self.trees.clone();
        // Grab the TXG lock exclusively, just so other users can't modify the
        // RIDT or AllocT while we're checking them.  NB: it might be
        // preferable to use a dedicated lock for this instead.
        self.transaction.write()
        .map_err(|_| unreachable!())
        .and_then(move |txg_guard| {
            let alloct_fut = trees2.alloct.range(..)
            .fold(true, move |passes, (pba, rid)| {
                trees2.ridt.get(rid)
                .map(move |v| {
                    passes & match v {
                        Some(ridt_entry) => {
                            if ridt_entry.drp.pba() != pba {
                                eprintln!(concat!("Indirect block {} has ",
                                    "address {:?} but another address {:?} ",
                                    "also maps to same indirect block"), rid,
                                    ridt_entry.drp.pba(), pba);
                                false
                            } else {
                                true
                            }
                        }, None => {
                            eprintln!(concat!("Extraneous entry {:?} => {} in ",
                                "the Allocation Table"), pba, rid);
                            false
                        }
                    }
                })
            });
            let ridt_fut = trees3.ridt.range(..)
            .fold(true, move |passes, (rid, entry)| {
                trees3.alloct.get(entry.drp.pba())
                .map(move |v| {
                    passes & match v {
                        Some(_) => true,
                        None => {
                            eprintln!(concat!("Indirect block {} has no ",
                                "reverse mapping in the allocation table.  ",
                                "Entry={:?}"),
                                rid, entry);
                            false
                        }
                    }
                })
            });
            alloct_fut.join(ridt_fut)
            .map(move |(x, y)| {
                drop(txg_guard);
                x & y
            })
        })
    }

    /// Foreground Tree consistency check.
    ///
    /// Checks that all DTrees are consistent and satisfy their invariants.
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    pub fn check(&self) -> impl Future<Item=bool, Error=Error> {
        self.trees.alloct.check()
        .join3(self.trees.ridt.check(),
               self.check_ridt())
        .map(|(x, y, z)| x && y && z)
    }

    /// Clean `zone` by moving all of its records to other zones.
    pub fn clean_zone(&self, zone: ClosedZone, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
    {
        // Outline:
        // 1) Lookup the Zone's PBA range in the Allocation Table.  Rewrite each
        //    record, modifying the RIDT and AllocT for each record
        // 2) Clean the Allocation table and RIDT themselves.  This must happen
        //    second, because the first step will reduce the amount of work to
        //    do in the second.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        let cache2 = self.cache.clone();
        let trees2 = self.trees.clone();
        let trees3 = self.trees.clone();
        let ddml2 = self.ddml.clone();
        #[cfg(debug_assertions)]
        let ddml3 = self.ddml.clone();
        #[cfg(debug_assertions)]
        let zid = zone.zid;
        let pba = zone.pba;
        let total_blocks = zone.total_blocks;
        self.list_indirect_records(&zone).for_each(move |record| {
            IDML::move_record(&cache2, &trees2, &ddml2, record, txg)
            .map(move |drp| {
                // We shouldn't have moved the record into the same zone
                debug_assert!(drp.pba().cluster != pba.cluster ||
                              drp.pba().lba < pba.lba ||
                              drp.pba().lba >= pba.lba + total_blocks);
            })
        }).and_then(move |_| {
            let txgs2 = zone.txgs.clone();
            let pba_range = pba..end;
            let czfut = trees3.ridt.clean_zone(pba_range.clone(), txgs2, txg);
            // Finish alloct.range_delete before alloct.clean_zone, because the
            // range delete is likely to eliminate most of not all nodes that
            // need to be moved by clean_zone
            let atfut = trees3.alloct.range_delete(pba_range.clone(), txg)
                .and_then(move |_| {
                    trees3.alloct.clean_zone(pba_range, zone.txgs, txg)
                });
            czfut.join(atfut).map(drop)
        }).map(move |_| {
            #[cfg(debug_assertions)]
            ddml3.assert_clean_zone(pba.cluster, zid, txg)
        })  // LCOV_EXCL_LINE   kcov false negative
    }

    pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self {
        let alloct = DTree::<PBA, RID>::create(ddml.clone(), true, 16.5, 2.809);
        let next_rid = Atomic::new(0);
        let ridt = DTree::<RID, RidtEntry>::create(ddml.clone(), true, 4.22,
            3.73);
        let transaction = RwLock::new(TxgT::from(0));
        let trees = Arc::new(Trees{alloct, ridt});
        IDML{cache, ddml, next_rid, transaction, trees}
    }

    pub fn dump_trees(&self, f: &mut io::Write) -> Result<(), Error>
    {
        self.trees.ridt.dump(f)?;
        self.trees.alloct.dump(f)
    }

    pub fn flush(&self, idx: u32, txg: TxgT)
        -> impl Future<Item=(), Error=Error> + Send
    {
        let ddml2 = self.ddml.clone();
        self.trees.alloct.flush(txg)
        .join(self.trees.ridt.flush(txg))
        .and_then(move |_| ddml2.flush(idx))
    }

    pub fn list_closed_zones(&self)
        -> impl Stream<Item=ClosedZone, Error=Error> + Send
    {
        self.ddml.list_closed_zones()
    }

    /// Return a list of all active (not deleted) indirect Records that have
    /// been written to the IDML in the given Zone.
    ///
    /// This list should be persistent across reboots.
    fn list_indirect_records(&self, zone: &ClosedZone)
        -> impl Stream<Item=RID, Error=Error> + Send
    {
        // Iterate through the AllocT to get indirect records from the target
        // zone.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        self.trees.alloct.range(zone.pba..end)
            .map(|(_pba, rid)| rid)
    }

    /// Open an existing `IDML`
    ///
    /// # Parameters
    ///
    /// * `ddml`:           An already-opened `DDML`
    /// * `cache`:          An already-constrcuted `Cache`
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>,
                 mut label_reader: LabelReader) -> (Self, LabelReader)
    {
        let l: Label = label_reader.deserialize().unwrap();
        let alloct = Tree::open(ddml.clone(), true, l.alloct);
        let ridt = Tree::open(ddml.clone(), true, l.ridt);
        let transaction = RwLock::new(l.txg);
        let next_rid = Atomic::new(l.next_rid);
        let trees = Arc::new(Trees{alloct, ridt});
        let idml = IDML{cache, ddml, next_rid, transaction, trees};
        (idml, label_reader)
    }

    /// Rewrite the given direct Record and update its metadata.
    fn move_record(cache: &Arc<Mutex<Cache>>, trees: &Arc<Trees>,
                   ddml: &Arc<DDML>, rid: RID, txg: TxgT)
        -> impl Future<Item=DRP, Error=Error> + Send
    {
        type MyFut = Box<dyn Future<Item=DRP, Error=Error> + Send>;

        // Even if the cache contains the target record, we must also do an RIDT
        // lookup because we're going to rewrite the RIDT
        let cache2 = cache.clone();
        let ddml2 = ddml.clone();
        let ddml3 = ddml.clone();
        let trees2 = trees.clone();
        trees.ridt.get(rid)
            .and_then(move |v| {
                let mut entry = v.expect(
                    "Inconsistency in alloct.  Entry not found in RIDT");
                let compressed = entry.drp.is_compressed();

                let cache_miss = || {
                    // Cache miss: get the old record, write the new one, then
                    // erase the old.  Same ordering requirements apply as for
                    // the cache hit case.
                    //
                    // Even if the record is a Tree node, get it as though it
                    // were a DivBufShared.  This skips deserialization and
                    // works perfectly fine with put_direct.
                    //
                    // Read the record as though it were uncompressed, to avoid
                    // the CPU cost of decompression/compression.
                    let drp_uc = entry.drp.as_uncompressed();
                    let ddml4 = ddml2.clone();
                    let fut = ddml2.get_direct::<DivBufShared>(&drp_uc)
                    .and_then(move |dbs| {
                        let db = dbs.try_const().unwrap();
                        ddml4.put_direct(&db, Compression::None, txg)
                        .and_then(move |drp| {
                            ddml4.delete_direct(&entry.drp, txg)
                            .map(move |_| drp.into_compressed(&entry.drp))
                        })
                    });
                    Box::new(fut) as MyFut
                };

                // Bypass the cache for compressed records, since we don't know
                // what compression algorithm to write back with.
                let fut = if !compressed {
                    let guard = cache2.lock().unwrap();
                    if let Some(t) = guard.get_ref(&Key::Rid(rid)) {
                        // Cache hit: Write the new record and delete the old
                        // Must finish writing the new record before deleting
                        // the old so we don't reuse the zone too soon.
                        // NB: if BFFFS ever implements deferred zone erase,
                        // then we can write and delete in parallel.
                        let db = t.serialize();
                        let fut = ddml2.put_direct(&db, Compression::None, txg)
                        .and_then(move |drp| {
                            ddml3.delete_direct(&entry.drp, txg)
                            .map(move |_| drp)
                        });
                        Box::new(fut) as MyFut
                    } else {
                        cache_miss()
                    }
                } else {
                    cache_miss()
                };
                fut.and_then(move |drp: DRP| {
                    entry.drp = drp;
                    let ridt_fut = trees2.ridt.insert(rid, entry, txg);
                    let alloct_fut = trees2.alloct.insert(drp.pba(), rid, txg);
                    ridt_fut.join(alloct_fut)
                    .map(move |_| drp)
                })
            })  // LCOV_EXCL_LINE   kcov false negative
    }

    /// Shutdown all background tasks.
    pub fn shutdown(&self) {
        self.ddml.shutdown()
    }

    /// Return approximately the usable storage space in LBAs.
    pub fn size(&self) -> LbaT {
        self.ddml.size()
    }

    /// Get a reference to the current transaction group.
    ///
    /// The reference will prevent the current transaction group from syncing,
    /// so don't hold it too long.
    pub fn txg(&self) -> RwLockReadFut<TxgT> {
        self.transaction.read()
    }

    /// Finish the current transaction group and start a new one.
    pub fn advance_transaction<B, F>(&self, f: F)
        -> impl Future<Item=(), Error=Error> + 'a
        where F: FnOnce(TxgT) -> B + 'a,
              B: IntoFuture<Item = (), Error = Error> + 'a
    {
        self.transaction.write()
            .map_err(|_| Error::EPIPE)
            .and_then(move |mut txg_guard| {
                let txg = *txg_guard;
                f(txg).into_future()
                .map(move |_| *txg_guard += 1)
            })
    }

    /// Asynchronously write this `IDML`'s label to its `Pool`
    pub fn write_label(&self, mut labeller: LabelWriter, txg: TxgT)
        -> impl Future<Item=(), Error=Error>
    {
        // The txg lock must be held when calling write_label.  Otherwise,
        // next_rid may be out-of-date by the time we serialize the label.
        debug_assert!(self.transaction.try_read().is_err(),
            "IDML::write_label must be called with the txg lock held");
        let next_rid = self.next_rid.load(Ordering::Relaxed);
        let alloct = self.trees.alloct.serialize().unwrap();
        let ridt = self.trees.ridt.serialize().unwrap();
        let label = Label {
            alloct,
            next_rid,
            ridt,
            txg,
        };
        labeller.serialize(&label).unwrap();
        self.ddml.write_label(labeller)
    }
}

/// Private helper function for several IDML methods
fn unwrap_or_enoent(r: Option<RidtEntry>)
    -> impl Future<Item=RidtEntry, Error=Error>
{
    match r {
        None => Err(Error::ENOENT).into_future(),
        Some(entry) => Ok(entry).into_future()
    }
}

impl DML for IDML {
    type Addr = RID;

    fn delete(&self, ridp: &Self::Addr, txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let trees2 = self.trees.clone();
        let rid = *ridp;
        let fut = self.trees.ridt.get(rid)
            .and_then(unwrap_or_enoent)
            .and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    cache2.lock().unwrap().remove(&Key::Rid(rid));
                    let ddml_fut = ddml2.delete_direct(&entry.drp, txg);
                    let alloct_fut = trees2.alloct.remove(entry.drp.pba(), txg);
                    let ridt_fut = trees2.ridt.remove(rid, txg);
                    Box::new(
                        ddml_fut.join3(alloct_fut, ridt_fut)
                             .map(|(_, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                             })
                     )
                } else {
                    let ridt_fut = trees2.ridt.insert(rid, entry, txg)
                        .map(drop);
                    boxfut!(ridt_fut)
                }
            });
        Box::new(fut)
    }

    fn evict(&self, rid: &Self::Addr) {
        self.cache.lock().unwrap().remove(&Key::Rid(*rid));
    }

    fn get<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr)
        -> Box<dyn Future<Item=Box<R>, Error=Error> + Send>
    {
        let rid = *ridp;
        self.cache.lock().unwrap().get::<R>(&Key::Rid(rid)).map(|t| {
            boxfut!(future::ok::<Box<R>, Error>(t))
        }).unwrap_or_else(|| {
            let cache2 = self.cache.clone();
            let ddml2 = self.ddml.clone();
            let fut = self.trees.ridt.get(rid)
                .and_then(unwrap_or_enoent)
                .and_then(move |entry| {
                    ddml2.get_direct(&entry.drp)
                }).map(move |cacheable: Box<T>| {
                    let r = cacheable.make_ref();
                    let key = Key::Rid(rid);
                    cache2.lock().unwrap().insert(key, cacheable);
                    r.downcast::<R>().unwrap()
                });
            Box::new(fut)
        })
    }

    fn pop<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr, txg: TxgT)
        -> Box<dyn Future<Item=Box<T>, Error=Error> + Send>
    {
        let rid = *ridp;
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let ddml3 = self.ddml.clone();
        let trees2 = self.trees.clone();
        let fut = self.trees.ridt.get(rid)
            .and_then(unwrap_or_enoent)
            .and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    let cacheval = cache2.lock().unwrap()
                        .remove(&Key::Rid(rid));
                    let bfut = cacheval
                        .map(move |cacheable| {
                            let t = cacheable.downcast::<T>().unwrap();
                            boxfut!(ddml2.delete(&entry.drp, txg)
                                              .map(move |_| t)
                            )
                        }).unwrap_or_else(||{
                            boxfut!(ddml3.pop_direct::<T>(&entry.drp))
                        });
                    let alloct_fut = trees2.alloct.remove(entry.drp.pba(), txg);
                    let ridt_fut = trees2.ridt.remove(rid, txg);
                    boxfut!(
                        bfut.join3(alloct_fut, ridt_fut)
                             .map(|(cacheable, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                                 cacheable
                             })
                     )
                } else {
                    let cacheval = cache2.lock().unwrap()
                        .get::<R>(&Key::Rid(rid));
                    let bfut = cacheval.map(|cacheref: Box<R>|{
                        let t = cacheref.to_owned().downcast::<T>().unwrap();
                        boxfut!(future::ok(t))
                    }).unwrap_or_else(|| {
                        Box::new(ddml2.get_direct::<T>(&entry.drp))
                    });
                    let ridt_fut = trees2.ridt.insert(rid, entry, txg);
                    boxfut!(
                        bfut.join(ridt_fut)
                            .map(|(cacheable, _)| {
                                cacheable
                            })
                    )
                }
            });
        Box::new(fut)
    }

    fn put<T>(&self, cacheable: T, compression: Compression, txg: TxgT)
        -> Box<dyn Future<Item=Self::Addr, Error=Error> + Send>
        where T: Cacheable
    {
        // Outline:
        // 1) Write to the DDML
        // 2) Cache
        // 3) Add entry to the RIDT
        // 4) Add reverse entry to the AllocT
        let cache2 = self.cache.clone();
        let trees2 = self.trees.clone();
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));

        let fut = self.ddml.put_direct(&cacheable.make_ref(), compression, txg)
        .and_then(move|drp| {
            let alloct_fut = trees2.alloct.insert(drp.pba(), rid, txg);
            let rid_entry = RidtEntry::new(drp);
            let ridt_fut = trees2.ridt.insert(rid, rid_entry, txg);
            ridt_fut.join(alloct_fut)
            .map(move |(old_rid_entry, old_alloc_entry)| {
                assert!(old_rid_entry.is_none(), "RID was not unique");
                assert!(old_alloc_entry.is_none(), concat!(
                    "Double allocate without free.  ",
                    "DDML allocator leak detected!"));
                cache2.lock().unwrap().insert(Key::Rid(rid),
                    Box::new(cacheable));
                rid
            })
        });
        Box::new(fut)
    }

    fn sync_all(&self, txg: TxgT)
        -> Box<dyn Future<Item=(), Error=Error> + Send>
    {
        self.ddml.sync_all(txg)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Label {
    alloct:             TreeOnDisk<DRP>,
    next_rid:           u64,
    ridt:               TreeOnDisk<DRP>,
    /// Last transaction group synced before the label was written
    txg:                TxgT,
}

// LCOV_EXCL_START
#[cfg(test)]
#[cfg(feature = "mocks")]
mod t {

    use super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use pretty_assertions::assert_eq;
    use simulacrum::*;
    use simulacrum::validators::trivial::any;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::Mutex
    };
    use tokio::runtime::current_thread;

    /// Inject a record into the RIDT and AllocT
    fn inject_record(rt: &mut current_thread::Runtime, idml: &IDML, rid: RID,
                     drp: &DRP, refcount: u64)
    {
        let entry = RidtEntry{drp: *drp, refcount};
        let txg = TxgT::from(0);
        rt.block_on(idml.trees.ridt.insert(rid, entry, txg)).unwrap();
        rt.block_on(idml.trees.alloct.insert(drp.pba(), rid, txg)).unwrap();
    }

    // pet kcov
    #[test]
    fn ridtentry_debug() {
        let drp = DRP::random(Compression::None, 4096);
        let ridt_entry = RidtEntry::new(drp);
        format!("{:?}", ridt_entry);

        let label = Label{
            alloct:     TreeOnDisk::default(),
            next_rid:   0,
            ridt:       TreeOnDisk::default(),
            txg:        TxgT(0)
        };
        format!("{:?}", label);
    }

    #[test]
    fn ridtentry_typical_size() {
        let typical = RidtEntry::new(DRP::default());
        assert_eq!(RidtEntry::TYPICAL_SIZE,
                   bincode::serialized_size(&typical).unwrap() as usize);
    }

    #[test]
    fn check_ridt_ok() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 2);

        assert!(rt.block_on(idml.check_ridt()).unwrap());
    }

    #[test]
    fn check_ridt_extraneous_alloct() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject a record into the AllocT but not the RIDT
        let txg = TxgT::from(0);
        rt.block_on(idml.trees.alloct.insert(drp.pba(), rid, txg)).unwrap();

        assert!(!rt.block_on(idml.check_ridt()).unwrap());
    }

    #[test]
    fn check_ridt_extraneous_ridt() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject a record into the RIDT but not the AllocT
        let entry = RidtEntry{drp, refcount: 2};
        let txg = TxgT::from(0);
        rt.block_on(idml.trees.ridt.insert(rid, entry, txg)).unwrap();

        assert!(!rt.block_on(idml.check_ridt()).unwrap());
    }

    #[test]
    fn check_ridt_mismatch() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let drp2 = DRP::random(Compression::None, 4096);
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        // Inject a mismatched pair of records
        let entry = RidtEntry{drp, refcount: 2};
        let txg = TxgT::from(0);
        rt.block_on(idml.trees.ridt.insert(rid, entry, txg)).unwrap();
        rt.block_on(idml.trees.alloct.insert(drp2.pba(), rid, txg)).unwrap();

        assert!(!rt.block_on(idml.check_ridt()).unwrap());
    }

    #[test]
    fn delete_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::default();
        ddml.expect_delete_direct()
            .called_once()
            .with(passes(move |args: &(*const DRP, TxgT)|
                         unsafe {*args.0 == drp} && args.1 == TxgT::from(42))
            ).returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 1);

        rt.block_on(idml.delete(&rid, TxgT::from(42))).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.trees.ridt.get(rid)).unwrap().is_none());
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert!(alloc_rec.is_none());
    }

    #[test]
    fn delete_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 2);

        rt.block_on(idml.delete(&rid, TxgT::from(42))).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    #[test]
    fn evict() {
        let rid = RID(42);
        let mut cache = Cache::default();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        idml.evict(&rid);
    }

    #[test]
    fn get_hot() {
        let rid = RID(42);
        let mut cache = Cache::default();
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try_const().unwrap()))
            });
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn get_cold() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        let owned_by_cache = Rc::new(
            RefCell::new(Vec::<Box<dyn Cacheable>>::new())
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
        let mut ddml = DDML::default();
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
        inject_record(&mut rt, &idml, rid, &drp, 1);

        let fut = idml.get::<DivBufShared, DivBuf>(&rid);
        current_thread::Runtime::new().unwrap().block_on(fut).unwrap();
    }

    #[test]
    fn list_indirect_records() {
        let txgs = TxgT::from(0)..TxgT::from(2);
        let cz = ClosedZone{pba: PBA::new(0, 100), total_blocks: 100, zid: 0,
                            freed_blocks: 50, txgs};
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        // A record just below the target zone
        let rid0 = RID(99);
        let drp0 = DRP::new(PBA::new(0, 99), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, rid0, &drp0, 1);
        // A record at the end of the target zone
        let rid2 = RID(102);
        let drp2 = DRP::new(PBA::new(0, 199), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, rid2, &drp2, 1);
        // A record at the start of the target zone
        let rid1 = RID(92);
        let drp1 = DRP::new(PBA::new(0, 100), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, rid1, &drp1, 1);
        // A record just past the target zone
        let rid3 = RID(101);
        let drp3 = DRP::new(PBA::new(0, 200), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, rid3, &drp3, 1);
        // A record in the same LBA range as but different cluster than the
        // target zone
        let rid4 = RID(105);
        let drp4 = DRP::new(PBA::new(1, 150), Compression::None, 4096, 4096, 0);
        inject_record(&mut rt, &idml, rid4, &drp4, 1);

        let r = rt.block_on(idml.list_indirect_records(&cz).collect());
        assert_eq!(r.unwrap(), vec![rid1, rid2]);
    }

    /// When moving a record not resident in cache, get it from disk
    #[test]
    fn move_indirect_record_cold() {
        let v = vec![42u8; 4096];
        let dbs = DivBufShared::from(v.clone());
        let rid = RID(1);
        let drp0 = DRP::random(Compression::None, 4096);
        let drp1 = DRP::random(Compression::None, 4096);
        let drp1_c = drp1;
        let mut cache = Cache::default();
        let mut ddml = DDML::default();
        cache.expect_get_ref()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(rid)}
            })).returning(move |_| {
                None
            });
        ddml.expect_get_direct()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {
                    (**key).pba() == drp0.pba() && !(**key).is_compressed()
                }
            })).returning(move |_| {
                let r = DivBufShared::from(&dbs.try_const().unwrap()[..]);
                Box::new(future::ok::<Box<DivBufShared>, Error>(Box::new(r)))
            });
        ddml.expect_put_direct::<DivBuf>()
            .called_once()
            .with(passes(move |(_, c, _): &(*const DivBuf, Compression, TxgT)| {
                *c == Compression::None
            })).returning(move |(_, _, _)|
                Box::new(Ok(drp1).into_future())
            );
        ddml.then().expect_delete_direct()
            .called_once()
            .with(passes(move |(key, _txg): &(*const DRP, TxgT)| {
                unsafe {**key == drp0}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp0, 1);

        rt.block_on(IDML::move_record(&idml.cache, &idml.trees,
                                      &idml.ddml, rid, TxgT::from(0))
        ).unwrap();

        // Now verify the RIDT and alloct entries
        let entry = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp1_c);
        let alloc_rec = rt.block_on(
            idml.trees.alloct.get(drp1_c.pba())
        ).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    /// When moving compressed records, the cache should be bypassed
    #[test]
    fn move_indirect_record_compressed() {
        let v = vec![42u8; 4096];
        let dbs = DivBufShared::from(v.clone());
        let rid = RID(1);
        let drp0 = DRP::random(Compression::Zstd(None), 4096);
        let drp1 = DRP::random(Compression::Zstd(None), 4096);
        let drp1_c = drp1;
        let cache = Cache::default();
        let mut ddml = DDML::default();
        ddml.expect_get_direct()
            .called_once()
            .with(passes(move |key: &*const DRP| {
                unsafe {
                    (**key).pba() == drp0.pba() && !(**key).is_compressed()
                }
            })).returning(move |_| {
                let r = DivBufShared::from(&dbs.try_const().unwrap()[..]);
                Box::new(future::ok::<Box<DivBufShared>, Error>(Box::new(r)))
            });
        ddml.expect_put_direct::<DivBuf>()
            .called_once()
            .with(passes(move |(_, c, _): &(*const DivBuf, Compression, TxgT)| {
                *c == Compression::None
            })).returning(move |(_, _, _)|
                Box::new(Ok(drp1).into_future())
            );
        ddml.then().expect_delete_direct()
            .called_once()
            .with(passes(move |(key, _txg): &(*const DRP, TxgT)| {
                unsafe {**key == drp0}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp0, 1);

        rt.block_on(IDML::move_record(&idml.cache, &idml.trees,
                                      &idml.ddml, rid, TxgT::from(0))
        ).unwrap();

        // Now verify the RIDT and alloct entries
        let entry = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp1_c);
        let alloc_rec = rt.block_on(
            idml.trees.alloct.get(drp1_c.pba())
        ).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    /// When moving records, check the cache first.
    #[test]
    fn move_indirect_record_hot() {
        let v = vec![42u8; 4096];
        let dbs = DivBufShared::from(v.clone());
        let rid = RID(1);
        let drp0 = DRP::random(Compression::None, 4096);
        let drp1 = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        let mut ddml = DDML::default();
        cache.expect_get_ref()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(rid)}
            })).returning(move |_| {
                Some(Box::new(dbs.try_const().unwrap()))
            });
        ddml.expect_put_direct::<DivBuf>()
            .called_once()
            .returning(move |(_, _, _)|
                       Box::new(Ok(drp1).into_future())
            );
        ddml.then().expect_delete_direct()
            .called_once()
            .with(passes(move |(key, _txg): &(*const DRP, TxgT)| {
                unsafe {**key == drp0}
            })).returning(move |_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp0, 1);

        rt.block_on(IDML::move_record(&idml.cache, &idml.trees,
                                      &idml.ddml, rid, TxgT::from(0))
        ).unwrap();

        // Now verify the RIDT and alloct entries
        let entry = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp1);
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp1.pba())).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    #[test]
    fn pop_hot_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| {
                Some(Box::new(DivBufShared::from(vec![0u8; 4096])))
            });
        let mut ddml = DDML::default();
        ddml.expect_delete()
            .called_once()
            .with(passes(move |args: &(*const DRP, TxgT)|
                         unsafe {*args.0 == drp} && args.1 == TxgT::from(42))
            ).returning(|_| {
                Box::new(future::ok::<(), Error>(()))
            });
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 1);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(42));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.trees.ridt.get(rid)).unwrap().is_none());
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert!(alloc_rec.is_none());
    }

    #[test]
    fn pop_hot_notlast() {
        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        cache.expect_get()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(move |_| {
                Some(Box::new(dbs.try_const().unwrap()))
            });
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 2);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    #[test]
    fn pop_cold_last() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        cache.expect_remove()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::default();
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
        inject_record(&mut rt, &idml, rid, &drp, 1);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        assert!(rt.block_on(idml.trees.ridt.get(rid)).unwrap().is_none());
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert!(alloc_rec.is_none());
    }

    #[test]
    fn pop_cold_notlast() {
        let rid = RID(42);
        let drp = DRP::random(Compression::None, 4096);
        let mut cache = Cache::default();
        cache.expect_get::<DivBuf>()
            .called_once()
            .with(passes(move |key: &*const Key| {
                unsafe {**key == Key::Rid(RID(42))}
            })).returning(|_| None );
        let mut ddml = DDML::default();
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
        inject_record(&mut rt, &idml, rid, &drp, 2);

        let fut = idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0));
        rt.block_on(fut).unwrap();
        // Now verify the contents of the RIDT and AllocT
        let entry2 = rt.block_on(idml.trees.ridt.get(rid)).unwrap().unwrap();
        assert_eq!(entry2.drp, drp);
        assert_eq!(entry2.refcount, 1);
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert_eq!(alloc_rec.unwrap(), rid);
    }

    #[test]
    fn put() {
        let mut cache = Cache::default();
        let mut ddml = DDML::default();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdead_beef);
        let rid = RID(0);
        cache.expect_insert()
            .called_once()
            .with(params!(Key::Rid(rid), any()))
            .returning(drop);
        ddml.expect_put_direct::<Box<dyn CacheRef>>()
            .called_once()
            .returning(move |(_, _, _)|
                       Box::new(Ok(drp).into_future())
            );
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let actual_rid = rt.block_on(
            idml.put(dbs, Compression::None, TxgT::from(0))
        ).unwrap();
        assert_eq!(rid, actual_rid);

        // Now verify the contents of the RIDT and AllocT
        let ridt_fut = idml.trees.ridt.get(actual_rid);
        let entry = rt.block_on(ridt_fut).unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp);
        let alloc_rec = rt.block_on(idml.trees.alloct.get(drp.pba())).unwrap();
        assert_eq!(alloc_rec.unwrap(), actual_rid);
    }

    #[test]
    fn sync_all() {
        let rid = RID(42);
        let cache = Cache::default();
        let mut ddml = DDML::default();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdead_beef);
        ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            .called_any()
            .with(params!(any(), any(), TxgT::from(42)))
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(Ok(drp).into_future())
            });
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .called_any()
            .with(params!(any(), any(), TxgT::from(42)))
            .returning(move |(_, _, _)| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::new(Ok(drp).into_future())
            });
        ddml.expect_sync_all()
            .called_once()
            .with(TxgT::from(42))
            .returning(|_| Box::new(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();
        inject_record(&mut rt, &idml, rid, &drp, 2);

        rt.block_on(idml.sync_all(TxgT::from(42))).unwrap();
    }

    #[test]
    fn advance_transaction() {
        let cache = Cache::default();
        let ddml = DDML::default();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        let mut rt = current_thread::Runtime::new().unwrap();

        rt.block_on(idml.advance_transaction(|_txg| Ok(()))).unwrap();
        assert_eq!(*idml.transaction.try_read().unwrap(), TxgT::from(1));
    }
}
