// vim: tw=80
///! Indirect Data Management Layer
///
/// Interface for working with indirect records.  An indirect record is a record
/// that is referenced by an immutable Record ID, rather than a disk address.
/// Unlike a direct record, it may be duplicated, by through snapshots, clones,
/// or deduplication.

use common::{
    *,
    dml::*,
    ddml::*,
    //cache::*,
    tree::*
};
use futures::{Future, Stream};
use nix::Error;
use std::sync::Arc;

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
    refcount: RID
}

pub type DTree<K, V> = Tree<DRP, DDML, K, V>;

/// Indirect Data Management Layer for a single `Pool`
pub struct IDML {
    _ddml: Arc<DDML>,

    /// Allocation table.  The reverse of `ridt`.
    ///
    /// Maps disk addresses back to record IDs.  Used for operations like
    /// garbage collection and defragmentation.
    _alloct: DTree<PBA, RID>,

    /// Record indirection table.  Maps record IDs to disk addresses.
    _ridt: DTree<RID, RidtEntry>
}

impl<'a> IDML {
    #[cfg(not(test))]
    pub fn create(ddml: Arc<DDML>) -> Self {
        let alloct = DTree::<PBA, RID>::create(ddml.clone());
        let ridt = DTree::<RID, RidtEntry>::create(ddml.clone());
        IDML{_alloct: alloct, _ddml: ddml, _ridt: ridt}
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

    fn get<'a, T: CacheRef>(&'a self, _rid: &Self::Addr)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a>
    {
        unimplemented!()
    }

    fn pop<'a, T: Cacheable>(&'a self, _rid: &Self::Addr)
        -> Box<Future<Item=Box<T>, Error=Error> + 'a>
    {
        unimplemented!()
    }

    fn put<'a, T: Cacheable>(&'a self, _cacheable: T, _compression: Compression)
        -> (Self::Addr, Box<Future<Item=(), Error=Error> + 'a>)
    {
        unimplemented!()
    }

    fn sync_all<'a>(&'a self) -> Box<Future<Item=(), Error=Error> + 'a>
    {
        unimplemented!()
    }
}
