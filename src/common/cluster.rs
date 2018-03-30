// vim: tw=80

use common::*;
use common::vdev::Vdev;
use common::vdev_raid::VdevRaid;
use futures::Future;
use nix::Error;
use std::collections::BTreeMap;

pub type ClusterFut = Future<Item = (), Error = Error>;

/// Minimal in-memory representation of a zone.
///
/// A full zone is one which contains data, and may not be written to again
/// until it has been garbage collected.  An open zone is one which may contain
/// data, and may be written to.  An Empty zone contains no data.
struct Zone {
    pub freed_blocks: LbaT,
    pub total_blocks: LbaT
}

impl Zone {
    const EMPTY_SENTINEL: LbaT = LbaT::max_value();

    fn is_empty(&self) -> bool {
        self.freed_blocks == Zone::EMPTY_SENTINEL
    }
}

struct OpenZone {
    pub start: LbaT,
    pub write_pointer: LbaT,
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
    /// implicitly Empty.  Any zones whose value of `freed_blocks` is
    /// `EMPTY_SENTINEL` are also Empty.  Any zones whose index is also present
    /// in `open_zones` are implicitly open.  All other zones are Closed.
    zones: Vec<Zone>,
    open_zones: BTreeMap<ZoneT, OpenZone>
}

impl FreeSpaceMap {
    /// Return Zone `zone_id` to an Empty state
    fn erase_zone(&mut self, zone_id: ZoneT) {
        let zone_idx = zone_id as usize;
        assert!(!self.open_zones.contains_key(&zone_id),
            "Can't erase an Open zone");
        assert!(zone_idx < self.zones.len(),
            "Can't erase an Empty zone");
        self.zones[zone_idx].freed_blocks = Zone::EMPTY_SENTINEL;
        // If this was the last zone, then remove all trailing Empty zones
        if zone_idx == self.zones.len() - 1 {
            // NB: determining first_empty should be rewritten with
            // Iterator::rfind once that feature is stable
            // https://github.com/rust-lang/rust/issues/39480
            let first_empty;
            {
                let mut iter = self.zones.iter().enumerate();
                loop {
                    let elem = iter.next_back();
                    match elem {
                        Some((_, z)) if z.is_empty() => continue,
                        Some((i, _)) => {
                            first_empty = i + 1;
                            break;
                        },
                        None => {
                            first_empty = 0;    // All zones are empty!
                            break;
                        }
                    }
                }
            }
            self.zones.truncate(first_empty);
        }
    }

    fn free(&mut self, zone_id: ZoneT, length: LbaT) {
        let zone = self.zones.get_mut(zone_id as usize).expect(
            "Can't free from an empty zone");
        assert!(!zone.is_empty(), "Can't free from an empty zone");
        zone.freed_blocks += length;
        assert!(zone.freed_blocks <= zone.total_blocks,
                "Double free detected");
    }

    /// Try to allocate `lbas` worth of space in any open zone.  If no open
    /// zones can satisfy the allocation, return `None` instead.
    fn try_allocate(&mut self, lbas: LbaT) -> Option<(ZoneT, LbaT)> {
        let zones = &self.zones;
        self.open_zones.iter_mut().find(|&(zone_id, ref oz)| {
            let zone = &zones[*zone_id as usize];
            let avail_lbas = zone.total_blocks - (oz.write_pointer - oz.start);
            avail_lbas >= lbas
        }).and_then(|(zone_id, oz)| {
            let lba = oz.write_pointer;
            oz.write_pointer += lbas;
            Some((*zone_id, lba))
        })
    }
}

/// A `Cluster` is ArkFS's equivalent of ZFS's top-level Vdev.  It is the
/// highest level `Vdev` that has its own LBA space.
pub struct Cluster {
    /// 0-based index of the cluster within the pool
    idx: u16,

    free_space_map: FreeSpaceMap,

    /// Total number of zones in `vdev`
    total_zones: ZoneT,

    /// Underlying vdev (which may or may not use RAID)
    vdev: VdevRaid
}

impl Cluster {
    /// Delete the underlying storage for a Zone.
    pub fn delete(&mut self, zone: ZoneT) -> Box<ClusterFut> {
        self.free_space_map.erase_zone(zone);
        unimplemented!();
    }

    /// Mark `length` LBAs beginning at LBA `lba` as unused, but do not delete
    /// them from the underlying storage.
    ///
    /// Deleting data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // Before deleting the underlying storage, ArkFS should double-check that
    // nothing is using it.  That requires using the AllocationTable, which is
    // above the layer of the Cluster.
    pub fn free(&mut self, lba: LbaT, length: LbaT) {
        let start_zone = self.vdev.lba2zone(lba).expect(
            "Can't free from inter-zone padding");
        #[cfg(test)]
        {
            let end_zone = self.vdev.lba2zone(lba + length - 1).expect(
                "Can't free from inter-zone padding");
            assert_eq!(start_zone, end_zone);
        }
        self.free_space_map.free(start_zone, length);
    }

    /// Asynchronously read from the cluster
    pub fn read(&self, buf: IoVecMut, lba: LbaT) -> Box<ClusterFut> {
        self.vdev.read_at(buf, lba)
    }

    /// Write a buffer to the cluster
    ///
    /// # Returns
    ///
    /// The LBA where the data will be written, and a
    /// `Future` for the operation in progress.
    pub fn write(&self, _buf: IoVec) -> (LbaT, Box<ClusterFut>) {
        // Outline:
        // 1) Try allocating in an open zone
        // 2) If that doesn't work, try opening a new one, and allocating from
        //    that
        // 3) If that doesn't work, return ENOSPC
        // 4) write to the vdev
        unimplemented!();
    }
}

//#[cfg(test)]
//mod t {
    //use super::*;
//}
