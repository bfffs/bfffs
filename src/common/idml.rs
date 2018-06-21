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
    ddml::*,
    cache::*,
    tree::*
};
use futures::{Future, IntoFuture, Stream};
use nix::{Error, errno};
use std::sync::{Arc, Mutex};

pub use common::ddml::ClosedZone;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord,
         Serialize)]
pub struct RID(u64);

impl MinValue for RID {
    fn min_value() -> Self {
        RID(u64::min_value())
    }
}

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
    _cache: Arc<Mutex<Cache>>,

    ddml: Arc<DDML>,

    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    alloct: DTree<PBA, RID>,

    /// Holds the next RID to allocate.  They are never reused.
    next_rid: Atomic<u64>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    ridt: DTree<RID, RidtEntry>
}

impl<'a> IDML {
    #[cfg(not(test))]
    pub fn create(ddml: Arc<DDML>, cache: Arc<Mutex<Cache>>) -> Self {
        let alloct = DTree::<PBA, RID>::create(ddml.clone());
        let next_rid = Atomic::new(0);
        let ridt = DTree::<RID, RidtEntry>::create(ddml.clone());
        IDML{alloct, _cache: cache, ddml, next_rid, ridt}
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

    fn delete(&self, _rid: &Self::Addr) {
        unimplemented!()
    }

    fn evict(&self, _rid: &Self::Addr) {
        unimplemented!()
    }

    fn get<'a, T: CacheRef>(&'a self, rid: &Self::Addr)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a>
    {
        let fut = self.ridt.get(*rid)
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(errno::Errno::EPIPE)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |entry| {
                self.ddml.get(&entry.drp)
            });
        Box::new(fut)
    }

    fn pop<'a, T: Cacheable, R: CacheRef>(&'a self, rid: &Self::Addr)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a>
    {
        let rid2 = rid.clone();
        let fut = self.ridt.get(rid.clone())
            .and_then(|r| {
                match r {
                    None => Err(Error::Sys(errno::Errno::EPIPE)).into_future(),
                    Some(entry) => Ok(entry).into_future()
                }
            }).and_then(move |mut entry| {
                entry.refcount -= 1;
                if entry.refcount == 0 {
                    let ddml_fut = self.ddml.pop::<T, R>(&entry.drp);
                    let alloct_fut = self.alloct.remove(entry.drp.pba());
                    let ridt_fut = self.ridt.remove(rid2);
                    Box::new(
                        ddml_fut.join3(alloct_fut, ridt_fut)
                             .map(|(cacheable, old_rid, _old_ridt_entry)| {
                                 assert!(old_rid.is_some());
                                 cacheable
                             })
                     ) as Box<Future<Item=Box<T>, Error=Error>>
                } else {
                    let ddml_fut = self.ddml.get::<R>(&entry.drp);
                    let ridt_fut = self.ridt.insert(rid2, entry);
                    Box::new(
                        ddml_fut.join(ridt_fut)
                            .map(|(cacheref, _)| {
                                cacheref.to_owned().downcast::<T>().unwrap()
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
        // 2) Add entry to the RIDT
        // 3) Add reverse entry to the AllocT
        let (drp, ddml_fut) = self.ddml.put(cacheable, compression);
        let rid = RID(self.next_rid.fetch_add(1, Ordering::Relaxed));
        let alloct_fut = self.alloct.insert(drp.pba(), rid.clone());
        let rid_entry = RidtEntry::new(drp);
        let ridt_fut = self.ridt.insert(rid.clone(), rid_entry);
        let fut = Box::new(
            ddml_fut.join3(ridt_fut, alloct_fut)
                .map(move |(_, old_rid_entry, old_alloc_entry)| {
                    assert!(old_rid_entry.is_none(), "RID was not unique");
                    assert!(old_alloc_entry.is_none(), concat!(
                        "Double allocate without free.  ",
                        "DDML allocator leak detected!"));
                })
        );
        (rid, fut)
    }

    fn sync_all<'a>(&'a self) -> Box<Future<Item=(), Error=Error> + 'a>
    {
        unimplemented!()
    }
}
