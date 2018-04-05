// vim: tw=80

use common::*;
use common::vdev::{Vdev, VdevFut};
#[cfg(not(test))]
use common::vdev_raid::*;
use futures::{Future, future};
use nix::{Error, errno};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Keys;

pub type ClusterFut<'a> = Future<Item = (), Error = Error> + 'a;

#[cfg(test)]
/// Only exists so mockers can replace VdevRaid
pub trait VdevRaidTrait : Vdev {
    fn erase_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn finish_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn flush_zone(&self, zone: ZoneT) -> (LbaT, Box<VdevFut>);
    fn open_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
    fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> Box<VdevFut>;
}
#[cfg(test)]
pub type VdevRaidLike = Box<VdevRaidTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type VdevRaidLike = VdevRaid;

/// Minimal in-memory representation of a zone.
///
/// A full zone is one which contains data, and may not be written to again
/// until it has been garbage collected.  An open zone is one which may contain
/// data, and may be written to.  An Empty zone contains no data.
#[derive(Clone, Copy, Debug, Default)]
struct Zone {
    /// Number of LBAs that have been freed from this `Zone` since it was
    /// opened.
    pub freed_blocks: u32,
    /// Total number of LBAs in the `Zone`.  It may never change while the
    /// `Zone` is open or full.
    pub total_blocks: u32
}

#[derive(Clone, Copy, Debug)]
struct OpenZone {
    /// First LBA of the `Zone`.  It may never change while the `Zone` is open
    /// or full.
    pub start: LbaT,
    /// Number of LBAs that have been allocated within this `Zone` so far.
    pub allocated_blocks: u32,
}

impl OpenZone {
    /// Returns the next LBA within this `Zone` that should be allocated
    fn write_pointer(&self) -> LbaT {
        self.start + (self.allocated_blocks as LbaT)
    }

    /// Mark some space in this Zone as wasted, usually because `VdevRaid`
    /// zero-filled them.
    fn waste_space(&mut self, space: LbaT) {
        self.allocated_blocks += space as u32;
    }
}

/// In-core representation of the free-space map.  Used for deciding when to
/// open new zones, close old ones, and reclaim full ones.
// Common operations include:
// * Choose an open zone to write X bytes, or open a new one
// * Choose a zone to reclaim
// * Find a zone by Zone ID, to rebuild it
// * Find all zones modified in a certain txg range
struct FreeSpaceMap {
    /// `Vec` of all zones in the Vdev.  Any zones past the end of the `Vec` are
    /// implicitly Empty.  Any zones whose index is present in `empty_zones` are
    /// also Empty.  Any zones whose index is also present in `open_zones` are
    /// implicitly open.  All other zones are Closed.
    zones: Vec<Zone>,

    /// Stores the set of empty zones with id less than zones.len().  All zones
    /// with id greater than or equal to zones.len() are implicitly empty
    empty_zones: BTreeSet<ZoneT>,

    /// Currently open zones
    open_zones: BTreeMap<ZoneT, OpenZone>,

    /// Total number of zones in the vdev
    total_zones: ZoneT,
}

impl FreeSpaceMap {
    /// Return Zone `zone_id` to an Empty state
    fn erase_zone(&mut self, zone_id: ZoneT) {
        let zone_idx = zone_id as usize;
        assert!(!self.open_zones.contains_key(&zone_id),
            "Can't erase an open zone");
        assert!(zone_idx < self.zones.len(),
            "Can't erase an empty zone");
        self.empty_zones.insert(zone_id);
        // If this was the last zone, then remove all trailing Empty zones
        if zone_idx == self.zones.len() - 1 {
            // NB: determining first_empty should be rewritten with
            // Iterator::rfind once that feature is stable
            // https://github.com/rust-lang/rust/issues/39480
            let first_empty: ZoneT;
            {
                let mut iter = self.zones.iter().enumerate();
                loop {
                    let elem = iter.next_back();
                    match elem {
                        Some((i, _)) if self.is_empty(i as ZoneT) => continue,
                        Some((i, _)) => {
                            first_empty = i as ZoneT + 1;
                            break;
                        },
                        None => {
                            first_empty = 0;    // All zones are empty!
                            break;
                        }
                    }
                }
            }
            self.zones.truncate(first_empty as usize);
            let _going_away = self.empty_zones.split_off(&first_empty);
        }
    }

    /// Find the first Empty zone
    fn find_empty(&self) -> Option<ZoneT> {
        self.empty_zones.iter().nth(0).cloned()
            .or_else(|| {
                if (self.zones.len() as ZoneT) < self.total_zones {
                    Some(self.zones.len() as ZoneT)
                } else {
                    None
                }
            })
    }

    /// Mark the Zone as closed
    fn finish_zone(&mut self, zone_id: ZoneT) {
        assert!(self.open_zones.remove(&zone_id).is_some(),
            "Can't finish a Zone that isn't open");
    }

    fn free(&mut self, zone_id: ZoneT, length: LbaT) {
        assert!(!self.is_empty(zone_id), "Can't free from an empty zone");
        let zone = self.zones.get_mut(zone_id as usize).expect(
            "Can't free from an empty zone");
        // NB: the next two lines can be replaced by u32::try_from(length), once
        // that feature is stabilized
        // https://github.com/rust-lang/rust/issues/33417
        assert!(length < u32::max_value() as LbaT);
        zone.freed_blocks += length as u32;
        assert!(zone.freed_blocks <= zone.total_blocks,
                "Double free detected");
        if let Some(oz) = self.open_zones.get(&zone_id) {
            assert!(oz.allocated_blocks >= zone.freed_blocks,
                    "Double free detected in an open zone");
        }
    }

    /// Is the Zone with the given id empty?
    fn is_empty(&self, zone_id: ZoneT) -> bool {
        zone_id >= self.zones.len() as ZoneT ||
            self.empty_zones.contains(&zone_id)
    }

    fn new(total_zones: ZoneT) -> Self {
        FreeSpaceMap{empty_zones: BTreeSet::new(),
                     open_zones: BTreeMap::new(),
                     total_zones,
                     zones: Vec::new()}
    }

    /// Open an Empty zone, and optionally try to allocate from it.
    ///
    /// If there is not enough space for the requested allocation, then don't
    /// allocate anything and don't open the zone.
    ///
    /// # Parameters
    ///
    /// - `id`:     Zone id to open.  The consumer is responsible for opening
    ///             the zone in the underlying storage
    /// - `start`:  First LBA inside of the zone
    /// - `end`:    First LBA beyond the zone
    /// - `lbas`:   If nonzero, immediately allocate this much space
    ///
    /// # Returns
    ///
    /// If `lbas` was nonzero, return the zone id and LBA of the newly allocated
    /// space.  If `lbas` was zero, return `None`.  If `lbas` was nonzero and
    /// the requested zone has insufficient space, return ENOSPC.
    fn open_zone(&mut self, id: ZoneT, start: LbaT, end: LbaT,
                 lbas: LbaT) -> Result<Option<(ZoneT, LbaT)>, Error> {
        let idx = id as usize;
        let space = end - start;
        assert!(self.is_empty(id), "Can only open empty zones");
        if space < lbas {
            return Err(Error::from(errno::Errno::ENOSPC));
        }

        if idx >= self.zones.len() {
            for z in self.zones.len()..idx {
                assert!(self.empty_zones.insert(z as ZoneT));
            }
            // NB: this should use resize_default, once that API is stabilized:
            // https://github.com/rust-lang/rust/issues/41758
            self.zones.resize(idx + 1, Zone::default());
        }
        self.zones[idx].total_blocks = space as u32;
        self.zones[idx].freed_blocks = 0;
        let oz = OpenZone{start, allocated_blocks: lbas as u32};
        self.empty_zones.remove(&id);
        assert!(self.open_zones.insert(id, oz).is_none(),
            "Can only open empty zones");
        if lbas > 0 {
            Ok(Some((id, start)))
        } else {
            Ok(None)
        }
    }

    /// Return an iterator over the zone IDs of all open zones
    pub fn open_zone_ids(&self) -> Keys<ZoneT, OpenZone> {
        self.open_zones.keys()
    }

    /// Try to allocate `space` worth of space in any open zone.  If no open
    /// zones can satisfy the allocation, return `None` instead.
    ///
    /// # Returns
    ///
    /// The Zone and LBA where the allocation happened, and a vector of Zone IDs
    /// of Zones which has too little space.
    fn try_allocate(&mut self, space: LbaT) -> (Option<(ZoneT, LbaT)>, Vec<ZoneT>) {
        let zones = &self.zones;
        let mut nearly_full_zones = Vec::with_capacity(1);
        let result = self.open_zones.iter_mut().find(|&(zone_id, ref oz)| {
            let zone = &zones[*zone_id as usize];
            let avail_lbas = zone.total_blocks - oz.allocated_blocks;
            // NB the next two lines can be replaced by u32::try_from(space),
            // once that feature is stabilized
            // https://github.com/rust-lang/rust/issues/33417
            assert!(space < u32::max_value() as LbaT);
            if avail_lbas < space as u32 {
                nearly_full_zones.push(*zone_id);
                false
            } else {
                true
            }
        }).and_then(|(zone_id, oz)| {
            let lba = oz.write_pointer();
            oz.allocated_blocks += space as u32;
            Some((*zone_id, lba))
        });
        (result, nearly_full_zones)
    }

    /// Mark the next `space` LBAs in zone `zid` as wasted
    pub fn waste_space(&mut self, zid: ZoneT, space: LbaT) {
        let oz = self.open_zones.get_mut(&zid).unwrap();
        oz.waste_space(space);
        self.zones[zid as usize].freed_blocks += space as u32;
        assert!(oz.allocated_blocks <= self.zones[zid as usize].total_blocks);
    }
}

/// A `Cluster` is ArkFS's equivalent of ZFS's top-level Vdev.  It is the
/// highest level `Vdev` that has its own LBA space.
pub struct Cluster {
    fsm: RefCell<FreeSpaceMap>,

    /// Underlying vdev (which may or may not use RAID)
    vdev: VdevRaidLike
}

impl<'a> Cluster {
    /// Finish any zones that are too full for new allocations
    // This method defines the policy of when to close nearly full zones
    fn close_zones(&self, nearly_full_zones: &[ZoneT]) -> Vec<Box<VdevFut>> {
        // Any zone that had too little space for one allocation will probably
        // have too little space for the next allocation, too.  Go ahead and
        // close it
        nearly_full_zones.iter().map(|&zone_id| {
            self.fsm.borrow_mut().finish_zone(zone_id);
            self.vdev.finish_zone(zone_id)
        }).collect::<Vec<_>>()
    }

    /// Delete the underlying storage for a Zone.
    pub fn erase_zone(&mut self, zone: ZoneT) -> Box<ClusterFut<'static>> {
        self.fsm.borrow_mut().erase_zone(zone);
        self.vdev.erase_zone(zone)
    }

    /// Mark `length` LBAs beginning at LBA `lba` as unused, but do not delete
    /// them from the underlying storage.
    ///
    /// Deleting data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, ArkFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Cluster.
    pub fn free(&self, lba: LbaT, length: LbaT) {
        let start_zone = self.vdev.lba2zone(lba).expect(
            "Can't free from inter-zone padding");
        #[cfg(test)]
        {
            let end_zone = self.vdev.lba2zone(lba + length - 1).expect(
                "Can't free from inter-zone padding");
            assert_eq!(start_zone, end_zone,
                "Can't free across multiple zones");
        }
        self.fsm.borrow_mut().free(start_zone, length);
    }

    /// Construct a new `Cluster` from an already constructed
    /// [`VdevRaid`](struct.VdevRaid.html)
    pub fn new(vdev: VdevRaidLike) -> Self {
        Cluster{fsm: RefCell::new(FreeSpaceMap::new(vdev.zones())), vdev}
    }

    /// Returns the "best" number of operations to queue to this `Cluster`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    pub fn optimum_queue_depth(&self) -> u32 {
        self.vdev.optimum_queue_depth()
    }

    /// Asynchronously read from the cluster
    pub fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<ClusterFut<'static>> {
        self.vdev.read_at(buf, lba)
    }

    /// Return approximately the usable space of the Cluster in LBAs.
    pub fn size(&self) -> LbaT {
        self.vdev.size()
    }

    /// Sync the `Cluster`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&'a self) -> Box<ClusterFut<'a>> {
        let mut fsm = self.fsm.borrow_mut();
        let zone_ids = fsm.open_zone_ids().cloned().collect::<Vec<_>>();
        let flush_futs = zone_ids.iter().map(|&zone_id| {
            let (gap, fut) = self.vdev.flush_zone(zone_id);
            fsm.waste_space(zone_id, gap);
            fut
        }).collect::<Vec<_>>();
        let flush_fut = future::join_all(flush_futs);
        Box::new(flush_fut.and_then(move |_| self.vdev.sync_all()))
    }

    /// Write a buffer to the cluster
    ///
    /// # Returns
    ///
    /// The LBA where the data will be written, and a
    /// `Future` for the operation in progress.
    pub fn write(&'a self, buf: IoVec) -> Result<(LbaT, Box<ClusterFut<'a>>), Error> {
        // Outline:
        // 1) Try allocating in an open zone
        // 2) If that doesn't work, try opening a new one, and allocating from
        //    that
        // 3) If that doesn't work, return ENOSPC
        // 4) write to the vdev
        let space = (buf.len() / BYTES_PER_LBA) as LbaT;
        let (alloc_result, nearly_full_zones) = self.fsm.borrow_mut().try_allocate(space);
        let finish_futs = self.close_zones(&nearly_full_zones);
        alloc_result.map(|(zone_id, lba)| {
            let oz_fut: Box<ClusterFut<'static>> = Box::new(future::ok::<(), Error>(()));
            (zone_id, lba, oz_fut)
        }).or_else(|| {
            let empty_zone = self.fsm.borrow().find_empty();
            empty_zone.and_then(|zone_id| {
                let zl = self.vdev.zone_limits(zone_id);
                let e = self.fsm.borrow_mut().open_zone(zone_id, zl.0, zl.1,
                                                        space);
                match e {
                    Ok(Some((zone_id, lba))) => {
                        let oz_fut = self.vdev.open_zone(zone_id);
                        Some((zone_id, lba, oz_fut))
                    },
                    Err(_) => None,
                    Ok(None) => unreachable!("Tried a 0-length write?"),
                }
            })
        }).map(|(zone_id, lba, oz_fut)| {
            let fut : Box<Future<Item = (), Error = Error>+ 'a>;
            let wfut = oz_fut.and_then(move |_|
                self.vdev.write_at(buf, zone_id, lba)
            );
            fut = Box::new(future::join_all(finish_futs).join(wfut).map(|_| ()));
            (lba, fut)
        }).ok_or(Error::Sys(errno::Errno::ENOSPC))
    }
}

#[cfg(test)]
mod t {

#[cfg(feature = "mocks")]
mod cluster {
    use super::super::*;
    use divbuf::DivBufShared;
    use mockers::Scenario;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    mock!{
        MockVdevRaid,
        vdev,
        trait Vdev {
            fn handle(&self) -> Handle;
            fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
            fn optimum_queue_depth(&self) -> u32;
            fn size(&self) -> LbaT;
            fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
            fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
            fn zones(&self) -> ZoneT;
        },
        self,
        trait VdevRaidTrait{
            fn erase_zone(&self, zone: ZoneT) -> Box<VdevFut>;
            fn finish_zone(&self, zone: ZoneT) -> Box<VdevFut>;
            fn flush_zone(&self, zone: ZoneT) -> (LbaT, Box<VdevFut>);
            fn open_zone(&self, zone: ZoneT) -> Box<VdevFut>;
            fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
            fn write_at(&self, buf: IoVec, zone: ZoneT,
                        lba: LbaT) -> Box<VdevFut>;
        }
    }

    #[test]
    #[should_panic(expected = "Can't free across")]
    fn free_crosszone_padding() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.lba2zone_call(900).and_return_clone(Some(0)).times(..));
        s.expect(vr.lba2zone_call(1099).and_return_clone(Some(1)).times(..));
        let cluster = Cluster::new(Box::new(vr));
        cluster.free(900, 200);
    }

    #[test]
    #[should_panic(expected = "Can't free from inter-zone padding")]
    fn free_interzone_padding() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.lba2zone_call(1000).and_return_clone(None).times(..));
        let cluster = Cluster::new(Box::new(vr));
        cluster.free(1000, 10);
    }

    // Cluster.sync_all should flush all open VdevRaid zones, then sync_all the
    // VdevRaid
    #[test]
    fn sync_all() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1000)).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }),
            0,  // Zone
            0   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.flush_zone_call(0)
                 .and_return((5, Box::new(future::ok::<(), Error>(())))));
        s.expect(vr.sync_all_call()
                 .and_return(Box::new(future::ok::<(), Error>(()))));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        current_thread::block_on_all(future::lazy(|| {
            let (_, fut) = cluster.write(db0).expect("write failed early");
            fut.and_then(|_| cluster.sync_all())
        })).unwrap();
        let fsm = cluster.fsm.borrow();
        assert_eq!(fsm.open_zones.get(&0).unwrap().write_pointer(), 6);
        assert_eq!(fsm.zones[0].freed_blocks, 5);
    }

    #[test]
    fn write_zones_too_small() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1)).times(..));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
         let result = current_thread::block_on_all(future::lazy(|| {
           cluster.write(dbs.try().unwrap())
        }));
        assert_eq!(result.err().unwrap(), Error::Sys(errno::Errno::ENOSPC));
    }

    #[test]
    fn write_no_available_zones() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        // What a useless disk ...
        s.expect(vr.zones_call().and_return_clone(0).times(..));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let result = cluster.write(dbs.try().unwrap());
        assert_eq!(result.err().unwrap(), Error::Sys(errno::Errno::ENOSPC));
    }

    #[test]
    fn write_with_no_open_zones() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1000)).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }),
            0,  // Zone
            0   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let result = current_thread::block_on_all(future::lazy(|| {
            let (lba, fut) = cluster.write(db0).expect("write failed early");
            fut.map(move |_| lba)
        }));
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn write_with_open_zones() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1000)).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }),
            0,  // Zone
            0   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == BYTES_PER_LBA
            }),
            0,  // Zone
            1   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let db1 = dbs.try().unwrap();
        current_thread::block_on_all(future::lazy(|| {
            let cluster_ref = &cluster;
            let (_, fut0) = cluster.write(db0).expect("Cluster::write");
            fut0.and_then(move |_| {
                let (lba1, fut1) = cluster_ref.write(db1).expect("Cluster::write");
                assert_eq!(lba1, 1);
                fut1
            })
        })).expect("write failed");
    }

    // When one zone is too full to satisfy an allocation, it should be closed
    // and a new zone opened.
    #[test]
    fn write_zone_full() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 3)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((3, 6)).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == 2 * BYTES_PER_LBA
            }),
            0,  // Zone
            0   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.finish_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.open_zone_call(1)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(check!(move |buf: &IoVec| {
                buf.len() == 2 * BYTES_PER_LBA
            }),
            1,  // Zone
            3   /* Lba */)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        let cluster = Cluster::new(Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs.try().unwrap();
        let db1 = dbs.try().unwrap();
        current_thread::block_on_all(future::lazy(|| {
            let cluster_ref = &cluster;
            let (_, fut0) = cluster.write(db0).expect("Cluster::write");
            fut0.and_then(move |_| {
                let (lba1, fut1) = cluster_ref.write(db1).expect("Cluster::write");
                assert_eq!(lba1, 3);
                fut1
            })
        })).expect("write failed");
    }

}

mod free_space_map {
    use super::super::*;

    #[test]
    fn erase_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0).unwrap();
        fsm.erase_zone(0);
        assert!(fsm.is_empty(0));
        assert!(!fsm.is_empty(1));
    }

    #[test]
    #[should_panic(expected = "Can't erase an empty zone")]
    fn erase_empty_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.erase_zone(0);
    }

    #[test]
    fn erase_last_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0).unwrap();
        fsm.open_zone(1, 1000, 2000, 0).unwrap();
        fsm.finish_zone(1);
        fsm.erase_zone(1);
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.zones.len(), 1);
    }

    #[test]
    fn erase_last_zone_with_empties_behind_it() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0).unwrap();
        fsm.open_zone(2, 2000, 3000, 0).unwrap();
        fsm.finish_zone(2);
        fsm.erase_zone(2);
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.zones.len(), 1);
    }

    #[test]
    fn erase_last_zone_with_all_other_zones_empty() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(2, 2000, 3000, 0).unwrap();
        fsm.finish_zone(2);
        fsm.erase_zone(2);
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Can't erase an open zone")]
    fn erase_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0).unwrap();
        fsm.erase_zone(0);
    }

    #[test]
    fn find_empty_enospc() {
        let mut fsm = FreeSpaceMap::new(2);
        fsm.open_zone(0, 0, 1000, 0).unwrap().is_none();
        fsm.open_zone(1, 1000, 2000, 0).unwrap().is_none();
        fsm.finish_zone(1);
        assert_eq!(fsm.find_empty(), None);
    }

    #[test]
    fn find_empty_explicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0).unwrap().is_none();
        fsm.open_zone(2, 2000, 3000, 0).unwrap().is_none();
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn find_empty_implicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0).unwrap().is_none();
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn finish() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none();
        fsm.finish_zone(zid);
        assert!(!fsm.open_zones.contains_key(&zid));
        assert!(!fsm.is_empty(zid));
    }

    #[should_panic(expected = "Can't finish a Zone that isn't open")]
    #[test]
    fn finish_explicitly_empty() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        // First, open zone 1 so zone 0 will become explicitly empty
        assert!(fsm.open_zone(1, 1000, 2000, 0).unwrap().is_none());
        fsm.finish_zone(zid);
    }

    #[should_panic(expected = "Can't finish a Zone that isn't open")]
    #[test]
    fn finish_implicitly_empty() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.finish_zone(zid);
    }

    #[test]
    #[should_panic(expected = "Double free")]
    fn free_double_free_from_closed_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 1000;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space).unwrap();
        fsm.finish_zone(zid);
        fsm.free(zid, space);
        fsm.free(zid, space);
    }

    #[test]
    #[should_panic(expected = "Double free")]
    fn free_double_free_from_open_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space).unwrap();
        fsm.free(zid, space);
        fsm.free(zid, space);
    }

    #[test]
    fn free_from_closed_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space).unwrap();
        fsm.finish_zone(zid);
        fsm.free(zid, space);
        assert_eq!(fsm.zones[zid as usize].freed_blocks as LbaT, space);
    }

    #[test]
    #[should_panic(expected = "free from an empty zone")]
    fn free_from_explicitly_empty_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        // First, open zone 1 so zone 0 will become explicitly empty
        assert!(fsm.open_zone(1, 1000, 2000, 0).unwrap().is_none());
        fsm.free(zid, space);
    }

    #[test]
    #[should_panic(expected = "free from an empty zone")]
    fn free_from_implicitly_empty_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.free(zid, space);
    }

    #[test]
    fn free_from_open_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space).unwrap();
        fsm.free(zid, space);
        assert_eq!(fsm.zones[zid as usize].freed_blocks as LbaT, space);
    }

    #[test]
    fn open() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 1);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.open_zones[&zid].start, 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 0);
    }

    #[test]
    fn open_and_allocate() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        assert_eq!(fsm.open_zone(zid, 0, 1000, space).unwrap(), Some((zid, 0)));
        assert_eq!(fsm.zones.len(), 1);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.open_zones[&zid].start, 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), space);
    }

    #[test]
    // Try to open and allocate, but with insufficient space
    fn open_and_enospc() {
        let zid: ZoneT = 0;
        let space: LbaT = 2000;
        let mut fsm = FreeSpaceMap::new(32768);
        assert_eq!(fsm.open_zone(zid, 0, 1000, space).unwrap_err(),
            Error::Sys(errno::Errno::ENOSPC));
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    fn open_explicitly_empty() {
        let mut fsm = FreeSpaceMap::new(32768);

        // First, open zone 1 so zone 0 will become explicitly empty
        assert!(fsm.open_zone(1, 1000, 2000, 0).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 2);

        // Now try to open an explicitly empty zone
        let zid: ZoneT = 0;
        assert!(fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 2);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.open_zones[&zid].start, 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 0);
    }

    #[test]
    fn open_implicitly_empty() {
        let zid: ZoneT = 1;
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 1000, 2000, 0).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 2);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.open_zones[&zid].start, 1000);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 1000);
        assert!(fsm.is_empty(0))
    }

    #[test]
    #[should_panic(expected="Can only open empty zones")]
    fn open_already_open() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0).unwrap();
        fsm.open_zone(zid, 0, 1000, 0).unwrap();
    }

    #[test]
    #[should_panic(expected="Can only open empty zones")]
    fn open_closed_zone() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0).unwrap();
        fsm.finish_zone(zid);
        fsm.open_zone(zid, 0, 1000, 0).unwrap();
    }

    #[test]
    fn try_allocate() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none());
        let (res, full_zones) = fsm.try_allocate(64);
        assert_eq!(res, Some((zid, 0)));
        assert!(full_zones.is_empty());
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 64);
    }

    #[test]
    fn try_allocate_enospc() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none());
        assert!(fsm.try_allocate(2000).0.is_none());
    }

    #[test]
    fn try_allocate_from_zone_1() {
        let zid: ZoneT = 1;
        let mut fsm = FreeSpaceMap::new(32768);
        // Pretend that zone 0 is too small for our allocation, but zone 1 isn't
        assert!(fsm.open_zone(0, 0, 10, 0).unwrap().is_none());
        assert!(fsm.open_zone(zid, 10, 1000, 0).unwrap().is_none());
        let (res, full_zones) = fsm.try_allocate(64);
        assert_eq!(res, Some((zid, 10)));
        assert_eq!(full_zones, vec![0]);
        assert_eq!(fsm.open_zones[&0].write_pointer(), 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 74);
    }

    #[test]
    fn try_allocate_only_closed_zones() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0).unwrap().is_none();
        fsm.finish_zone(zid);
        assert!(fsm.try_allocate(64).0.is_none());
    }

    #[test]
    fn try_allocate_only_empty_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.try_allocate(64).0.is_none());
    }


}
}
