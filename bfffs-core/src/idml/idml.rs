// vim: tw=80

use crate::{
    dml::*,
    ddml::*,
    cache::{self, Cache, Cacheable, CacheRef, Key},
    label::*,
    tree::TreeOnDisk,
    types::*,
    writeback::{Credit, WriteBack}
};
use divbuf::DivBufShared;
use futures::{
    Future, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future
};
use futures_locks::{RwLock, RwLockReadFut};
#[cfg(test)] use mockall::mock;
use serde_derive::{Deserialize, Serialize};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex
    },
};
use tracing::instrument;
use tracing_futures::Instrument;
use super::{DTree, RidtEntry, Status};

/// Indirect Data Management Layer for a single `Pool`
pub struct IDML {
    cache: Arc<Mutex<Cache>>,

    ddml: Arc<DDML>,

    /// Holds the next RID to allocate.  They are never reused.
    next_rid: AtomicU64,

    /// Current transaction group
    transaction: RwLock<TxgT>,

    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    // TODO: consider a lazy delete strategy to reduce the amount of tree
    // activity on pop/delete by deferring alloct removals to the cleaner.
    // Even though it has a single owner, the tree must be Arc so IDML methods
    // can be 'static
    alloct: Arc<DTree<PBA, RID>>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    // Even though it has a single owner, the tree must be Arc so IDML methods
    // can be 'static
    ridt: Arc<DTree<RID, RidtEntry>>,

    /// The IDML is the owner of the WriteBack tracker
    writeback: WriteBack
}

// Some of these methods have no unit tests.  Their test coverage is provided
// instead by integration tests.
#[cfg_attr(test, allow(unused))]
impl<'a> IDML {
    pub fn borrow_credit(&self, size: usize)
        -> Pin<Box<dyn Future<Output=Credit> + Send>>
    {
        self.writeback.borrow(size).boxed()
    }

    /// Get the maximum size of bytes in the cache
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().capacity()
    }

    /// Foreground RIDT/AllocT consistency check.
    ///
    /// Checks that the RIDT and AllocT are exact inverses of each other and
    /// that the DDML's used space count is correct.
    ///
    /// # Returns
    ///
    /// `true` on success, `false` on failure
    fn check_ridt(&self) -> impl Future<Output=Result<bool>> {
        let alloct2 = self.alloct.clone();
        let alloct3 = self.alloct.clone();
        let alloct4 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let ridt3 = self.ridt.clone();
        let ddml2 = self.ddml.clone();
        // Grab the TXG lock exclusively, just so other users can't modify the
        // RIDT or AllocT while we're checking them.  NB: it might be
        // preferable to use a dedicated lock for this instead.
        self.transaction.write()
        .then(move |txg_guard| {
            let used_fut = ddml2.used().map(Ok);
            let alloct_fut = alloct2.range(..)
            .try_fold(true, move |passes, (pba, rid)| {
                ridt2.get(rid)
                .map_ok(move |v| {
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

            let ridt_fut = ridt3.range(..)
            .try_fold(true, move |passes, (rid, entry)| {
                alloct3.get(entry.drp.pba())
                .map_ok(move |v| {
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

            let indirect_bfut = ridt3.range(..)
            .try_fold(0, |size, (_rid, entry)| {
                future::ok(size + entry.drp.asize())
            });
            let ridt_bfut = ridt3.addresses(..)
            .fold(0, |size, drp| future::ready(size + drp.asize()))
            .map(Ok);
            let alloct_bfut = alloct4.addresses(..)
            .fold(0, |size, drp| future::ready(size + drp.asize()))
            .map(Ok);
            let ufut = future::try_join4(indirect_bfut, ridt_bfut, alloct_bfut, used_fut)
            .map_ok(move |(iblocks, rblocks, ablocks, used)| {
                if used != iblocks + rblocks + ablocks {
                    eprintln!(concat!("DDML used space inconsistency.  ",
                        "DDML reports {} blocks used, but there are {} ",
                        "indirect blocks, {} blocks used by the RIDT, and {} ",
                        "blocks used by the alloct"),
                        used, iblocks, rblocks, ablocks);
                    false
                } else {
                    true
                }
            });

            future::try_join3(alloct_fut, ridt_fut, ufut)
            .map_ok(move |(x, y, z)| {
                drop(txg_guard);
                x & y & z
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
    #[tracing::instrument(skip(self))]
    pub fn check(&self) -> impl Future<Output=Result<bool>> {
        future::try_join3(self.alloct.clone().check(),
                          self.ridt.clone().check(),
                          self.check_ridt())
        .map_ok(|(x, y, z)| x && y && z)
            .in_current_span()
    }

    /// Clean `zone` by moving all of its records to other zones.
    #[tracing::instrument(skip(self))]
    pub fn clean_zone(&self, zone: ClosedZone, txg: TxgT)
        -> impl Future<Output=Result<()>> + Send
    {
        // Outline:
        // 1) Lookup the Zone's PBA range in the Allocation Table.  Rewrite each
        //    record, modifying the RIDT and AllocT for each record
        // 2) Clean the Allocation table and RIDT themselves.  This must happen
        //    second, because the first step will reduce the amount of work to
        //    do in the second.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        let cache2 = self.cache.clone();
        let alloct2 = self.alloct.clone();
        let alloct3 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let ridt3 = self.ridt.clone();
        let ddml2 = self.ddml.clone();
        #[cfg(debug_assertions)]
        let ddml3 = self.ddml.clone();
        #[cfg(debug_assertions)]
        let zid = zone.zid;
        let pba = zone.pba;
        let total_blocks = zone.total_blocks;
        // Cleaning normally happens in the background, so limit concurrency to
        // 1, so as not to interfere too much with foreground tasks
        self.list_indirect_records(&zone)
        .try_for_each(move |record| {
            IDML::move_record(&cache2, ridt2.clone(), alloct2.clone(), &ddml2,
                record, txg)
            .map_ok(move |drp| {
                // We shouldn't have moved the record into the same zone
                debug_assert!(drp.pba().cluster != pba.cluster ||
                              drp.pba().lba < pba.lba ||
                              drp.pba().lba >= pba.lba + total_blocks);
            })
        }).and_then(move |_| {
            let txgs2 = zone.txgs.clone();
            let pba_range = pba..end;
            let czfut = ridt3.clean_zone(pba_range.clone(), txgs2, txg);
            // Finish alloct.range_delete before alloct.clean_zone, because the
            // range delete is likely to eliminate most if not all nodes that
            // need to be moved by clean_zone
            let atfut = alloct3.clone()
            .range_delete(pba_range.clone(), txg, Credit::null())
            .and_then(move |_| {
                alloct3.clean_zone(pba_range, zone.txgs, txg)
            });
            future::try_join(czfut, atfut).map_ok(drop)
        }).and_then(move |_| {
            #[cfg(debug_assertions)]
            {
                ddml3.assert_clean_zone(pba.cluster, zid, txg)
                    .map(Ok)
            }
            #[cfg(not(debug_assertions))]
            future::ok(())
        })
    }

    pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self {
        let alloct = Arc::new(
            DTree::<PBA, RID>::create(ddml.clone(), true, 16.5, 2.809)
        );
        let next_rid = AtomicU64::new(0);
        let ridt = Arc::new(
            DTree::<RID, RidtEntry>::create(ddml.clone(), true, 4.22, 3.73)
        );
        let transaction = RwLock::new(TxgT::from(0));
        // TODO: apply configurable writeback size
        let writeback = WriteBack::limitless();
        IDML{cache, ddml, next_rid, transaction, alloct, ridt, writeback}
    }

    /// Drop all data from the cache, for testing or benchmarking purposes
    pub fn drop_cache(&self) {
        self.cache.lock().unwrap().drop_cache()
    }

    pub async fn dump_alloct(&self, f: &mut dyn io::Write) -> Result<()>
    {
        self.alloct.dump(f).await
    }

    pub async fn dump_fsm(&self) -> Vec<String> {
        self.ddml.dump_fsm().await
    }

    pub async fn dump_ridt(&self, f: &mut dyn io::Write) -> Result<()>
    {
        self.ridt.dump(f).await
    }

    /// Fault the given disk or mirror
    pub async fn fault(&self, uuid: Uuid) -> Result<()> {
        self.ddml.fault(uuid).await
    }

    /// Flush the IDML's data to disk
    ///
    /// `idx`, if provided, is the index of the label to sync to disk.  If not
    /// provided, no label will be synced.
    #[tracing::instrument(skip(self))]
    pub fn flush(&self, idx: Option<u32>, txg: TxgT)
        -> impl Future<Output=Result<()>> + Send
    {
        let tfut = future::try_join(self.alloct.clone().flush(txg),
                                    self.ridt.clone().flush(txg))
            .map_ok(drop);
        if let Some(idx) = idx {
            let ddml2 = self.ddml.clone();
            tfut.and_then(move |_| ddml2.flush(idx)).boxed()
        } else {
            tfut.boxed()
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn list_closed_zones(&self)
        -> impl Future<Output=impl Iterator<Item=ClosedZone>>
    {
        self.ddml.list_closed_zones()
    }

    /// Return a list of all active (not deleted) indirect Records that have
    /// been written to the IDML in the given Zone.
    ///
    /// This list should be persistent across reboots.
    fn list_indirect_records(&self, zone: &ClosedZone)
        -> impl Stream<Item=Result<RID>> + Send
    {
        // Iterate through the AllocT to get indirect records from the target
        // zone.
        let end = PBA::new(zone.pba.cluster, zone.pba.lba + zone.total_blocks);
        self.alloct.range(zone.pba..end)
            .map_ok(|(_pba, rid)| rid)
    }

    /// Open an existing `IDML`
    ///
    /// # Parameters
    ///
    /// * `ddml`:           An already-opened `DDML`
    /// * `cache`:          An already-constrcuted `Cache`
    /// * `writeback_size`: Soft limit for the amount of cached dirty data in
    ///                     bytes.
    /// * `label_reader`:   A `LabelReader` that has already consumed all labels
    ///                     prior to this layer.
    pub fn open(
        ddml: Arc<DDML>,
        cache: Arc<Mutex<Cache>>,
        writeback_size: usize,
        mut label_reader: LabelReader,
    ) -> (Self, LabelReader)
    {
        let l: Label = label_reader.deserialize().unwrap();
        let alloct = Arc::new(DTree::open(ddml.clone(), true, l.alloct));
        let ridt = Arc::new(DTree::open(ddml.clone(), true, l.ridt));
        let transaction = RwLock::new(l.txg);
        let next_rid = AtomicU64::new(l.next_rid);
        let writeback = WriteBack::with_capacity(writeback_size);
        let idml = IDML{
            cache,
            ddml,
            next_rid,
            transaction,
            alloct,
            ridt,
            writeback
        };
        (idml, label_reader)
    }

    /// Rewrite the given direct Record and update its metadata.
    fn move_record(cache: &Arc<Mutex<Cache>>, ridt: Arc<DTree<RID, RidtEntry>>,
                   alloct: Arc<DTree<PBA, RID>>,
                   ddml: &Arc<DDML>, rid: RID, txg: TxgT)
        -> impl Future<Output=Result<DRP>> + Send
    {
        // Even if the cache contains the target record, we must also do an RIDT
        // lookup because we're going to rewrite the RIDT
        let cache2 = cache.clone();
        let ddml2 = ddml.clone();
        let ddml3 = ddml.clone();
        let ridt2 = ridt.clone();
        ridt.get(rid)
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
                            .map_ok(move |_| drp.into_compressed(&entry.drp))
                        })
                    });
                    fut.boxed()
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
                            .map_ok(move |_| drp)
                        });
                        fut.boxed()
                    } else {
                        cache_miss()
                    }
                } else {
                    cache_miss()
                };
                fut.and_then(move |drp: DRP| {
                    entry.drp = drp;
                    let ridt_fut = ridt2.insert(rid, entry, txg,
                                                Credit::null());
                    let alloct_fut = alloct.insert(drp.pba(), rid, txg,
                                                   Credit::null());
                    future::try_join(ridt_fut, alloct_fut)
                    .map_ok(move |_| drp)
                })
            })
    }

    pub fn pool_name(&self) -> &str {
        self.ddml.pool_name()
    }

    /// Return approximately the usable storage space in LBAs.
    pub fn size(&self) -> impl Future<Output=LbaT> + Send {
        self.ddml.size()
    }

    pub fn status(&self) -> impl Future<Output=Status> + Send {
        self.ddml.status()
    }

    /// Get a reference to the current transaction group.
    ///
    /// The reference will prevent the current transaction group from syncing,
    /// so don't hold it too long.
    pub fn txg(&self) -> RwLockReadFut<TxgT> {
        self.transaction.read()
    }

    /// How many blocks are currently used?
    pub fn used(&self) -> impl Future<Output=LbaT> + Send {
        self.ddml.used()
    }

    /// Finish the current transaction group and start a new one.
    #[tracing::instrument(skip(self, f))]
    pub fn advance_transaction<B, F>(&self, f: F)
        -> impl Future<Output=Result<()>> + Send + 'a
        where F: FnOnce(TxgT) -> B + Send + 'a,
              B: Future<Output=Result<()>> + Send + 'a,
    {
        let ddml = self.ddml.clone();
        self.transaction.write()
        .then(move |mut txg_guard| async move {
            let txg = *txg_guard;
            f(txg).await?;
            ddml.advance_transaction(txg).await?;
            *txg_guard += 1;
            Ok(())
        })
    }

    /// Asynchronously write this `IDML`'s label to its `Pool`
    #[tracing::instrument(skip(self, labeller))]
    pub fn write_label(&self, mut labeller: LabelWriter, txg: TxgT)
        -> impl Future<Output=Result<()>> + Send
    {
        // The txg lock must be held when calling write_label.  Otherwise,
        // next_rid may be out-of-date by the time we serialize the label.
        debug_assert!(self.transaction.try_read().is_err(),
            "IDML::write_label must be called with the txg lock held");
        let next_rid = self.next_rid.load(Ordering::Relaxed);
        let alloct = self.alloct.serialize().unwrap();
        let ridt = self.ridt.serialize().unwrap();
        let label = Label {
            alloct,
            next_rid,
            ridt,
            txg,
        };
        labeller.serialize(&label).unwrap();
        self.ddml.write_label(labeller)
    }

    /// Get the soft limit for the bytes in the writeback cache
    pub fn writeback_size(&self) -> usize {
        self.writeback.capacity()
    }
}

impl DML for IDML {
    type Addr = RID;

    #[tracing::instrument(skip(self))]
    fn delete(&self, ridp: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
    {
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let rid = *ridp;
        let fut = self.ridt.get(rid)
            .and_then(move |oentry| {
                let mut entry = match oentry {
                    Some(e) => e,
                    None => panic!("Double delete detected for {rid:?}.")
                };
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    cache2.lock().unwrap().remove(&Key::Rid(rid));
                    let ddml_fut = ddml2.delete_direct(&entry.drp, txg);
                    let alloct_fut = alloct2.remove(entry.drp.pba(), txg,
                        Credit::null());
                    let ridt_fut = ridt2.remove(rid, txg, Credit::null());
                    Box::pin(
                        future::try_join3(ddml_fut, alloct_fut, ridt_fut)
                         .map_ok(|(_, old_rid, _old_ridt_entry)| {
                             assert!(old_rid.is_some());
                         })
                     )
                } else {
                    let ridt_fut = ridt2.insert(rid, entry, txg, Credit::null())
                        .map_ok(|old| assert!(old.is_some()));
                    ridt_fut.boxed()
                }
            });
        Box::pin(fut)
    }

    #[tracing::instrument(skip(self))]
    fn evict(&self, rid: &Self::Addr) {
        self.cache.lock().unwrap().remove(&Key::Rid(*rid));
    }

    #[instrument(skip(self))]
    fn get<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr)
        -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>
    {
        let rid = *ridp;
        cache::get_or_insert!(T, R, &self.cache, Key::Rid(rid),
            {
                let ddml2 = self.ddml.clone();
                self.ridt.get(rid)
                    .map(|r| match r {
                        Ok(None) => Err(Error::ENOENT),
                        Ok(Some(entry)) => Ok(entry),
                        Err(e) => Err(e)
                    }).and_then(move |entry| {
                        ddml2.get_direct::<T>(&entry.drp)
                    }).in_current_span()
            }
        )
    }

    #[instrument(skip(self))]
    fn pop<T: Cacheable, R: CacheRef>(&self, ridp: &Self::Addr, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>
    {
        let rid = *ridp;
        let cache2 = self.cache.clone();
        let ddml2 = self.ddml.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let efut = self.ridt.get(rid);
        async move {
            let mut entry = efut.await?
                .ok_or(Error::ENOENT)?;
            entry.refcount -= 1;
            if entry.refcount == 0 {
                let cacheval = cache2.lock().unwrap()
                    .remove(&Key::Rid(rid));
                let bfut = if let Some(cacheable) = cacheval {
                    let t = cacheable.downcast::<T>().unwrap();
                    ddml2.delete(&entry.drp, txg)
                    .map_ok(move |_| t)
                    .boxed()
                } else {
                    ddml2.pop_direct::<T>(&entry.drp).boxed()
                };
                let alloct_fut = alloct2.remove(entry.drp.pba(), txg,
                    Credit::null());
                let ridt_fut = ridt2.remove(rid, txg, Credit::null());
                let (cacheable, old_rid, old_ridt_entry) =
                    future::try_join3(bfut, alloct_fut, ridt_fut).await?;
                assert!(old_rid.is_some());
                assert!(old_ridt_entry.is_some());
                Ok(cacheable)
            } else {
                let cacheval = cache2.lock().unwrap()
                    .get::<R>(&Key::Rid(rid));
                let bfut = cacheval.map(|cacheref: Box<R>|{
                    let t = cacheref.into_owned().downcast::<T>().unwrap();
                    future::ok(t).boxed()
                }).unwrap_or_else(|| {
                    ddml2.get_direct::<T>(&entry.drp).boxed()
                });
                let ridt_fut = ridt2.insert(rid, entry, txg, Credit::null());
                let (cacheable, oldr) = future::try_join(bfut, ridt_fut).await?;
                assert!(oldr.is_some());
                Ok(cacheable)
            }
        }.boxed()
    }

    #[instrument(skip(self))]
    fn put<T>(&self, cacheable: T, compression: Compression, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<Self::Addr>> + Send>>
        where T: Cacheable
    {
        // TODO: spawn a separate task, for better parallelism.
        // Outline:
        // 1) Write to the DDML
        // 2) Cache
        // 3) Add entry to the RIDT
        // 4) Add reverse entry to the AllocT
        let cache2 = self.cache.clone();
        let alloct2 = self.alloct.clone();
        let ridt2 = self.ridt.clone();
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));

        let fut = self.ddml.put_direct(&cacheable.make_ref(), compression, txg)
        .and_then(move|drp| {
            let alloct_fut = alloct2.insert(drp.pba(), rid, txg,
                                            Credit::null());
            let rid_entry = RidtEntry::new(drp);
            let ridt_fut = ridt2.insert(rid, rid_entry, txg, Credit::null());
            future::try_join(ridt_fut, alloct_fut)
            .map_ok(move |(old_rid_entry, old_alloc_entry)| {
                assert!(old_rid_entry.is_none(), "RID was not unique");
                assert!(old_alloc_entry.is_none(), concat!(
                    "Double allocate without free.  ",
                    "DDML allocator leak detected!"));
                cache2.lock().unwrap()
                    .insert(Key::Rid(rid), Box::new(cacheable));
                rid
            })
        });
        Box::pin(fut)
    }

    fn repay(&self, credit: Credit) {
        self.writeback.repay(credit)
    }

    fn sync_all(&self, txg: TxgT)
        -> Pin<Box<dyn Future<Output=Result<()>> + Send>>
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
mock!{
    pub IDML {
        pub fn cache_size(&self) -> usize;
        pub fn borrow_credit(&self, size: usize)
            -> Pin<Box<dyn Future<Output=Credit> + Send>>;
        pub fn check(&self) -> Pin<Box<dyn Future<Output=Result<bool>>>>;
        pub fn clean_zone(&self, zone: ClosedZone, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self;
        pub fn drop_cache(&self);
        pub fn dump_alloct(&self, f: &mut dyn io::Write)
            -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
        pub async fn dump_fsm(&self) -> Vec<String>;
        pub fn dump_ridt(&self, f: &mut dyn io::Write)
            -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
        pub async fn fault(&self, uuid: Uuid) -> Result<()>;
        pub fn flush(&self, idx: Option<u32>, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        pub fn list_closed_zones(&self)
            -> Pin<Box<dyn Future<Output=Box<dyn Iterator<Item=ClosedZone> + Send>> + Send>>;
        pub fn open(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>, wbs: usize,
                     mut label_reader: LabelReader) -> (Self, LabelReader);
        pub fn pool_name(&self) -> &str;
        pub fn size(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
        pub fn status(&self) -> Pin<Box<dyn Future<Output=Status> + Send>>;
        // Return a static reference instead of a RwLockReadFut because it makes
        // the expectations easier to write
        pub fn txg(&self)
            -> Pin<Box<dyn Future<Output=&'static TxgT> + Send>>;
        pub fn used(&self) -> Pin<Box<dyn Future<Output=LbaT> + Send>>;
        // advance_transaction is difficult to mock with Mockall, because f's
        // output is typically a chained future that is difficult to name.
        // Instead, we'll use special logic in advance_transaction and only mock
        // the txg used.
        pub fn advance_transaction_inner(&self) -> TxgT;
        pub fn write_label(&self, mut labeller: LabelWriter, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        pub fn writeback_size(&self) -> usize;
    }
    impl DML for IDML {
        type Addr = RID;
        fn delete(&self, addr: &RID, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
        fn evict(&self, addr: &RID);
        fn get<T: Cacheable, R: CacheRef>(&self, addr: &RID)
            -> Pin<Box<dyn Future<Output=Result<Box<R>>> + Send>>;
        fn pop<T: Cacheable, R: CacheRef>(&self, rid: &RID, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<Box<T>>> + Send>>;
        fn put<T: Cacheable>(&self, cacheable: T, compression: Compression,
                                 txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<RID>> + Send>>;
        fn repay(&self, credit: Credit);
        fn sync_all(&self, txg: TxgT)
            -> Pin<Box<dyn Future<Output=Result<()>> + Send>>;
    }
}
#[cfg(test)]
impl<'a> MockIDML {
    pub fn advance_transaction<B, F>(&self, f: F)
        -> impl Future<Output=Result<()>> + Send + 'a
        where F: FnOnce(TxgT) -> B + Send + 'a,
              B: Future<Output=Result<()>> + Send + 'a,
    {
        let txg = self.advance_transaction_inner();
        f(txg)
    }
}

#[cfg(test)]
mod t {

    use super::*;
    use crate::tree;
    use divbuf::{DivBuf, DivBufShared};
    use futures::{channel::oneshot, future};
    use pretty_assertions::assert_eq;
    use mockall::{Sequence, predicate::*};
    use std::sync::Mutex;

    /// Inject a record into the RIDT and AllocT
    fn inject_record(idml: &IDML, rid: RID, drp: &DRP, refcount: u64)
    {
        let entry = RidtEntry{drp: *drp, refcount};
        let txg = TxgT::from(0);
        idml.ridt.clone().insert(rid, entry, txg, Credit::null())
            .now_or_never().unwrap()
            .unwrap();
        idml.alloct.clone().insert(drp.pba(), rid, txg, Credit::null())
            .now_or_never().unwrap()
            .unwrap();
    }

    fn mock_ddml() -> DDML {
        let mut ddml = DDML::default();
        ddml.expect_repay()
            .withf(|credit| *credit == 0usize)
            .return_const(());
        ddml
    }

    // pet kcov
    #[test]
    fn ridtentry_debug() {
        let drp = DRP::random(Compression::None, 4096);
        let ridt_entry = RidtEntry::new(drp);
        format!("{ridt_entry:?}");

        let label = Label{
            alloct:     TreeOnDisk::default(),
            next_rid:   0,
            ridt:       TreeOnDisk::default(),
            txg:        TxgT(0)
        };
        format!("{label:?}");
    }

    #[test]
    fn ridtentry_typical_size() {
        let typical = RidtEntry::new(DRP::default());
        assert_eq!(RidtEntry::TYPICAL_SIZE,
                   bincode::serialized_size(&typical).unwrap() as usize);
    }

    mod check_ridt {
        use super::*;

        #[tokio::test]
        async fn ok() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_used().returning(|| future::ready(1u64).boxed());
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 2);

            assert!(idml.check_ridt().await.unwrap());
        }

        #[tokio::test]
        async fn allocation_mismatch() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_used().returning(|| future::ready(42u64).boxed());
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 2);

            assert!(!idml.check_ridt().await.unwrap());
        }

        #[tokio::test]
        async fn extraneous_alloct() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_used().returning(|| future::ready(1u64).boxed());
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            // Inject a record into the AllocT but not the RIDT
            let txg = TxgT::from(0);
            idml.alloct.clone().insert(drp.pba(), rid, txg, Credit::null())
                .now_or_never().unwrap()
                .unwrap();

            assert!(!idml.check_ridt().await.unwrap());
        }

        #[tokio::test]
        async fn extraneous_ridt() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_used().returning(|| future::ready(1u64).boxed());
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            // Inject a record into the RIDT but not the AllocT
            let entry = RidtEntry{drp, refcount: 2};
            let txg = TxgT::from(0);
            idml.ridt.clone().insert(rid, entry, txg, Credit::null())
                .now_or_never().unwrap()
                .unwrap();

            assert!(!idml.check_ridt().await.unwrap());
        }

        #[tokio::test]
        async fn mismatch() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let drp2 = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_used().returning(|| future::ready(1u64).boxed());
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            // Inject a mismatched pair of records
            let entry = RidtEntry{drp, refcount: 2};
            let txg = TxgT::from(0);
            idml.ridt.clone().insert(rid, entry, txg, Credit::null())
                .now_or_never().unwrap()
                .unwrap();
            idml.alloct.clone().insert(drp2.pba(), rid, txg, Credit::null())
                .now_or_never().unwrap()
                .unwrap();

            assert!(!idml.check_ridt().await.unwrap());
        }
    }

    mod delete {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Delete a record that does not exist.  This typically indicate a
        /// double-free, and it is a fatal error.
        #[test]
        #[should_panic(expected = "Double delete")]
        fn double() {
            let rid = RID(42);
            let cache = Cache::with_capacity(1_048_576);
            let ddml = mock_ddml();
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

            let _r = idml.delete(&rid, TxgT::from(42))
                .now_or_never().unwrap();
        }

        #[test]
        fn last() {
            let rid = RID(42);
            let key = Key::Rid(rid);
            let drp = DRP::random(Compression::None, 4096);
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(key, Box::new(dbs));
            let mut ddml = mock_ddml();
            ddml.expect_delete_direct()
                .once()
                .with(eq(drp), eq(TxgT::from(42)))
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));
            let arc_ddml = Arc::new(ddml);
            let amcache = Arc::new(Mutex::new(cache));
            let idml = IDML::create(arc_ddml, amcache.clone());
            inject_record(&idml, rid, &drp, 1);

            idml.delete(&rid, TxgT::from(42))
                .now_or_never().unwrap()
                .unwrap();
            // Now verify the contents of the RIDT and AllocT
            assert!(idml.ridt.get(rid)
                    .now_or_never().unwrap()
                    .unwrap().is_none());
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert!(alloc_rec.is_none());
            // Finally, the cahce entry should be gone
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
        }

        #[test]
        fn notlast() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let ddml = mock_ddml();
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 2);

            idml.delete(&rid, TxgT::from(42))
                .now_or_never().unwrap().unwrap();
            // Now verify the contents of the RIDT and AllocT
            let entry2 = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(entry2.drp, drp);
            assert_eq!(entry2.refcount, 1);
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);
        }
    }

    #[test]
    fn evict() {
        let rid = RID(42);
        let key = Key::Rid(rid);
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let mut cache = Cache::with_capacity(1_048_576);
        cache.insert(key, Box::new(dbs));
        let ddml = mock_ddml();
        let arc_ddml = Arc::new(ddml);
        let amcache = Arc::new(Mutex::new(cache));
        let idml = IDML::create(arc_ddml, amcache.clone());

        idml.evict(&rid);
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
    }

    mod get {
        use super::*;

        /// Near-simultaneous get requests should not result in multiple reads
        /// from disk.
        #[tokio::test]
        async fn duplicate() {
            let rid = RID(42);
            let key = Key::Rid(rid);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let (tx, rx) = oneshot::channel();
            let mut ddml = mock_ddml();
            ddml.expect_get_direct::<DivBufShared>()
                .once()
                .with(eq(drp))
                .return_once(move |_| {
                    Box::pin(rx.map_err(Error::unhandled_error))
                });
            let arc_ddml = Arc::new(ddml);
            let amcache = Arc::new(Mutex::new(cache));
            let idml = IDML::create(arc_ddml, amcache.clone());
            inject_record(&idml, rid, &drp, 1);

            let fut1 = idml.get::<DivBufShared, DivBuf>(&rid);
            let fut2 = idml.get::<DivBufShared, DivBuf>(&rid);
            tx.send(Box::new(DivBufShared::from(vec![0u8; 4096])))
                .unwrap();
            future::try_join(fut1, fut2).await.unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }

        #[test]
        fn hot() {
            let rid = RID(42);
            let key = Key::Rid(rid);
            let mut cache = Cache::with_capacity(1_048_576);
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            cache.insert(key, Box::new(dbs));
            let ddml = mock_ddml();
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

            idml.get::<DivBufShared, DivBuf>(&rid)
                .now_or_never().unwrap()
                .unwrap();
        }

        #[test]
        fn cold() {
            let rid = RID(42);
            let key = Key::Rid(rid);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_get_direct::<DivBufShared>()
                .once()
                .with(eq(drp))
                .returning(move |_| {
                    let dbs = Box::new(DivBufShared::from(vec![0u8; 4096]));
                    Box::pin(future::ok::<Box<DivBufShared>, Error>(dbs))
                });
            let arc_ddml = Arc::new(ddml);
            let amcache = Arc::new(Mutex::new(cache));
            let idml = IDML::create(arc_ddml, amcache.clone());
            inject_record(&idml, rid, &drp, 1);

            idml.get::<DivBufShared, DivBuf>(&rid)
                .now_or_never().unwrap()
                .unwrap();
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
        }
    }

    #[test]
    fn list_indirect_records() {
        let txgs = TxgT::from(0)..TxgT::from(2);
        let cz = ClosedZone{pba: PBA::new(0, 100), total_blocks: 100, zid: 0,
                            freed_blocks: 50, txgs};
        let cache = Cache::with_capacity(1_048_576);
        let ddml = mock_ddml();
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        // A record just below the target zone
        let rid0 = RID(99);
        let drp0 = DRP::new(PBA::new(0, 99), Compression::None, 4096, 4096, 0);
        inject_record(&idml, rid0, &drp0, 1);
        // A record at the end of the target zone
        let rid2 = RID(102);
        let drp2 = DRP::new(PBA::new(0, 199), Compression::None, 4096, 4096, 0);
        inject_record(&idml, rid2, &drp2, 1);
        // A record at the start of the target zone
        let rid1 = RID(92);
        let drp1 = DRP::new(PBA::new(0, 100), Compression::None, 4096, 4096, 0);
        inject_record(&idml, rid1, &drp1, 1);
        // A record just past the target zone
        let rid3 = RID(101);
        let drp3 = DRP::new(PBA::new(0, 200), Compression::None, 4096, 4096, 0);
        inject_record(&idml, rid3, &drp3, 1);
        // A record in the same LBA range as but different cluster than the
        // target zone
        let rid4 = RID(105);
        let drp4 = DRP::new(PBA::new(1, 150), Compression::None, 4096, 4096, 0);
        inject_record(&idml, rid4, &drp4, 1);

        let r: Vec<RID> = idml.list_indirect_records(&cz)
            .try_collect()
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(r, vec![rid1, rid2]);
    }

    mod move_indirect_record {
        use super::*;
        use pretty_assertions::assert_eq;

        /// When moving a record not resident in cache, get it from disk
        #[test]
        fn cold() {
            let v = vec![42u8; 4096];
            let dbs = DivBufShared::from(v);
            let rid = RID(1);
            let key = Key::Rid(rid);
            let drp0 = DRP::random(Compression::None, 4096);
            let drp1 = DRP::random(Compression::None, 4096);
            let drp1_c = drp1;
            let mut seq = Sequence::new();
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_get_direct()
                .once()
                .in_sequence(&mut seq)
                .withf(move |key| key.pba() == drp0.pba() &&
                       !key.is_compressed())
                .returning(move |_| {
                    let r = DivBufShared::from(&dbs.try_const().unwrap()[..]);
                    Box::pin(future::ok::<Box<DivBufShared>, Error>(Box::new(r)))
                });
            ddml.expect_put_direct::<DivBuf>()
                .once()
                .in_sequence(&mut seq)
                .with(always(), eq(Compression::None), always())
                .returning(move |_, _, _|
                    Box::pin(future::ok(drp1))
                );
            ddml.expect_delete_direct()
                .once()
                .in_sequence(&mut seq)
                .with(eq(drp0), always())
                .returning(move |_, _| {
                    Box::pin(future::ok::<(), Error>(()))
                });
            let arc_ddml = Arc::new(ddml);
            let amcache = Arc::new(Mutex::new(cache));
            let idml = IDML::create(arc_ddml, amcache.clone());
            inject_record(&idml, rid, &drp0, 1);

            IDML::move_record(&idml.cache, idml.ridt.clone(), idml.alloct.clone(),
                &idml.ddml, rid, TxgT::from(0))
            .now_or_never().unwrap().unwrap();

            // Now verify the RIDT and alloct entries
            let entry = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(entry.refcount, 1);
            assert_eq!(entry.drp, drp1_c);
            let alloc_rec = idml.alloct.get(drp1_c.pba())
                .now_or_never().unwrap().unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);

            // Moving a record should not result in a cache insertion
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
        }

        /// When moving compressed records, the cache should be bypassed
        #[test]
        fn compressed() {
            let v = vec![42u8; 4096];
            let dbs = DivBufShared::from(v);
            let rid = RID(1);
            let drp0 = DRP::random(Compression::Zstd(None), 4096);
            let drp1 = DRP::random(Compression::Zstd(None), 4096);
            let drp1_c = drp1;
            let mut seq = Sequence::new();
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_get_direct()
                .once()
                .in_sequence(&mut seq)
                .withf(move |key| key.pba() == drp0.pba() &&
                       !key.is_compressed())
                .returning(move |_| {
                    let r = DivBufShared::from(&dbs.try_const().unwrap()[..]);
                    Box::pin(future::ok(Box::new(r)))
                });
            ddml.expect_put_direct::<DivBuf>()
                .once()
                .in_sequence(&mut seq)
                .with(always(), eq(Compression::None), always())
                .returning(move |_, _, _| Box::pin(future::ok(drp1)));
            ddml.expect_delete_direct()
                .once()
                .in_sequence(&mut seq)
                .with(eq(drp0), always())
                .returning(move |_, _| Box::pin(future::ok::<(), Error>(())));
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp0, 1);

            IDML::move_record(&idml.cache, idml.ridt.clone(), idml.alloct.clone(),
                &idml.ddml, rid, TxgT::from(0))
                .now_or_never().unwrap().unwrap();

            // Now verify the RIDT and alloct entries
            let entry = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(entry.refcount, 1);
            assert_eq!(entry.drp, drp1_c);
            let alloc_rec = idml.alloct.get(drp1_c.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);
        }

        /// When moving records, check the cache first.
        #[test]
        fn hot() {
            let v = vec![42u8; 4096];
            let dbs = DivBufShared::from(v);
            let rid = RID(1);
            let key = Key::Rid(rid);
            let drp0 = DRP::random(Compression::None, 4096);
            let drp1 = DRP::random(Compression::None, 4096);
            let mut seq = Sequence::new();
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(key, Box::new(dbs));
            let mut ddml = mock_ddml();
            ddml.expect_put_direct::<DivBuf>()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_, _, _|
                           Box::pin(future::ok(drp1))
                );
            ddml.expect_delete_direct()
                .once()
                .in_sequence(&mut seq)
                .with(eq(drp0), always())
                .returning(move |_, _| Box::pin(future::ok::<(), Error>(())));
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp0, 1);

            IDML::move_record(&idml.cache, idml.ridt.clone(), idml.alloct.clone(),
                &idml.ddml, rid, TxgT::from(0))
                .now_or_never().unwrap().unwrap();

            // Now verify the RIDT and alloct entries
            let entry = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap().unwrap();
            assert_eq!(entry.refcount, 1);
            assert_eq!(entry.drp, drp1);
            let alloc_rec = idml.alloct.get(drp1.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);
        }
    }

    mod pop {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn hot_last() {
            let rid = RID(42);
            let key = Key::Rid(rid);
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let drp = DRP::random(Compression::None, 4096);
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(key, Box::new(dbs));
            let mut ddml = mock_ddml();
            ddml.expect_delete()
                .once()
                .with(eq(drp), eq(TxgT::from(42)))
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));
            let arc_ddml = Arc::new(ddml);
            let amcache = Arc::new(Mutex::new(cache));
            let idml = IDML::create(arc_ddml, amcache.clone());
            inject_record(&idml, rid, &drp, 1);

            idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(42))
                .now_or_never().unwrap()
                .unwrap();
            // Now verify the contents of the RIDT and AllocT
            assert!(idml.ridt.get(rid)
                    .now_or_never().unwrap()
                    .unwrap().is_none());
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert!(alloc_rec.is_none());
            // It should be gone from the cache, too
            assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_none());
        }

        #[test]
        fn hot_notlast() {
            let dbs = DivBufShared::from(vec![42u8; 4096]);
            let rid = RID(42);
            let key = Key::Rid(rid);
            let drp = DRP::random(Compression::None, 4096);
            let mut cache = Cache::with_capacity(1_048_576);
            cache.insert(key, Box::new(dbs));
            let ddml = mock_ddml();
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 2);

            idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0))
                .now_or_never().unwrap()
                .unwrap();
            // Now verify the contents of the RIDT and AllocT
            let entry2 = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap().unwrap();
            assert_eq!(entry2.drp, drp);
            assert_eq!(entry2.refcount, 1);
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);
        }

        #[test]
        fn cold_last() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_pop_direct::<DivBufShared>()
                .once()
                .with(eq(drp))
                .returning(|_| {
                    let dbs = DivBufShared::from(vec![42u8; 4096]);
                    Box::pin(future::ok::<Box<DivBufShared>, Error>(
                            Box::new(dbs))
                    )
                });
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 1);

            idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0))
                .now_or_never().unwrap()
                .unwrap();
            // Now verify the contents of the RIDT and AllocT
            assert!(idml.ridt.get(rid)
                    .now_or_never().unwrap()
                    .unwrap().is_none());
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert!(alloc_rec.is_none());
        }

        #[test]
        fn cold_notlast() {
            let rid = RID(42);
            let drp = DRP::random(Compression::None, 4096);
            let cache = Cache::with_capacity(1_048_576);
            let mut ddml = mock_ddml();
            ddml.expect_get_direct()
                .once()
                .with(eq(drp))
                .returning(move |_| {
                    let dbs = Box::new(DivBufShared::from(vec![42u8; 4096]));
                    Box::pin(future::ok::<Box<DivBufShared>, Error>(dbs))
                });
            let arc_ddml = Arc::new(ddml);
            let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
            inject_record(&idml, rid, &drp, 2);

            idml.pop::<DivBufShared, DivBuf>(&rid, TxgT::from(0))
                .now_or_never().unwrap()
                .unwrap();
            // Now verify the contents of the RIDT and AllocT
            let entry2 = idml.ridt.get(rid)
                .now_or_never().unwrap()
                .unwrap().unwrap();
            assert_eq!(entry2.drp, drp);
            assert_eq!(entry2.refcount, 1);
            let alloc_rec = idml.alloct.get(drp.pba())
                .now_or_never().unwrap()
                .unwrap();
            assert_eq!(alloc_rec.unwrap(), rid);
        }
    }

    #[test]
    fn put() {
        let cache = Cache::with_capacity(1_048_576);
        let mut ddml = mock_ddml();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdead_beef);
        let rid = RID(0);
        let key = Key::Rid(rid);
        ddml.expect_put_direct::<Box<dyn CacheRef>>()
            .once()
            .returning(move |_, _, _|
                       Box::pin(future::ok(drp))
            );
        let arc_ddml = Arc::new(ddml);
        let amcache = Arc::new(Mutex::new(cache));
        let idml = IDML::create(arc_ddml, amcache.clone());

        let dbs = DivBufShared::from(vec![42u8; 4096]);
        let actual_rid = idml.put(dbs, Compression::None, TxgT::from(0))
            .now_or_never().unwrap().unwrap();
        assert_eq!(rid, actual_rid);

        // Now verify the contents of the RIDT and AllocT
        let ridt_fut = idml.ridt.get(actual_rid);
        let entry = ridt_fut
            .now_or_never().unwrap()
            .unwrap().unwrap();
        assert_eq!(entry.refcount, 1);
        assert_eq!(entry.drp, drp);
        let alloc_rec = idml.alloct.get(drp.pba())
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(alloc_rec.unwrap(), actual_rid);
        // It should be added to the cache, too
        assert!(amcache.lock().unwrap().get::<DivBuf>(&key).is_some());
    }

    #[test]
    fn sync_all() {
        let rid = RID(42);
        let cache = Cache::with_capacity(1_048_576);
        let mut ddml = mock_ddml();
        let drp = DRP::new(PBA::new(0, 0), Compression::None, 40000, 40000,
                           0xdead_beef);
        ddml.expect_put::<Arc<tree::Node<DRP, RID, RidtEntry>>>()
            .with(always(), always(), eq(TxgT::from(42)))
            .returning(move |_, _, _| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::pin(future::ok(drp))
            });
        ddml.expect_put::<Arc<tree::Node<DRP, PBA, RID>>>()
            .with(always(), always(), eq(TxgT::from(42)))
            .returning(move |_, _, _| {
                let drp = DRP::random(Compression::None, 4096);
                 Box::pin(future::ok(drp))
            });
        ddml.expect_sync_all()
            .once()
            .with(eq(TxgT::from(42)))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));
        inject_record(&idml, rid, &drp, 2);

        idml.sync_all(TxgT::from(42))
            .now_or_never().unwrap()
            .unwrap();
    }

    #[test]
    fn advance_transaction() {
        let cache = Cache::with_capacity(1_048_576);
        let txg = TxgT::from(0);
        let mut ddml = mock_ddml();
        ddml.expect_advance_transaction()
            .with(eq(txg))
            .once()
            .return_once(move |_| Box::pin(future::ok(())));
        let arc_ddml = Arc::new(ddml);
        let idml = IDML::create(arc_ddml, Arc::new(Mutex::new(cache)));

        idml.advance_transaction(|_txg| future::ok(()))
            .now_or_never().unwrap()
            .unwrap();
        assert_eq!(*idml.transaction.try_read().unwrap(), txg + 1);
    }
}
