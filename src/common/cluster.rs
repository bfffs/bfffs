// vim: tw=80

use bincode;
use crate::common::{*, label::*, vdev::{Vdev, VdevFut}};
#[cfg(not(test))] use crate::common::vdev_raid::*;
use futures::{ Future, IntoFuture, future};
use itertools::Itertools;
use metrohash::MetroHash64;
use std::{
    cell::RefCell,
    cmp,
    collections::{BTreeMap, BTreeSet, btree_map::Keys},
    fmt::{self, Display, Formatter},
    hash::Hash,
    rc::Rc,
    ops::Range
};
#[cfg(not(test))] use std::{
    num::NonZeroU64,
    path::Path
};
use uuid::Uuid;

pub type ClusterFut = Future<Item = (), Error = Error>;

#[cfg(test)]
/// Only exists so mockers can replace VdevRaid
pub trait VdevRaidTrait : Vdev {
    fn erase_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn finish_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn flush_zone(&self, zone: ZoneT) -> (LbaT, Box<VdevFut>);
    fn open_zone(&self, zone: ZoneT) -> Box<VdevFut>;
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut>;
    fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> Box<VdevFut>;
    fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> Box<VdevFut>;
    fn write_label(&self, labeller: LabelWriter) -> Box<VdevFut>;
    fn write_spacemap(&self, buf: IoVec, idx: u32, block: LbaT) -> Box<VdevFut>;
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
#[derive(Clone, Debug)]
struct Zone {
    /// Number of LBAs that have been freed from this `Zone` since it was
    /// opened.
    pub freed_blocks: u32,
    /// Total number of LBAs in the `Zone`.  It may never change while the
    /// `Zone` is open or full.
    pub total_blocks: u32,
    /// The range of transactions that have been written to this Zone.  The
    /// start is inclusive, and the end is exclusive
    ///
    /// The end is invalid for open zones, and both start and end are invalid
    /// for empty zones.
    pub txgs: Range<TxgT>
}

impl Default for Zone {
    fn default() -> Self {
        let txgs = TxgT::from(0)..TxgT::from(0);
        Zone{freed_blocks: 0, total_blocks: 0, txgs}
    }
}

/// Public representation of a closed zone
#[derive(Debug, Eq, PartialEq)]
pub struct ClosedZone {
    /// First LBA of the zone
    pub start: LbaT,
    /// Number of freed blocks in this zone
    pub freed_blocks: LbaT,
    /// Total number of blocks in this zone
    pub total_blocks: LbaT,
    /// Range of transaction groups included within this Zone
    pub txgs: Range<TxgT>,
    /// Zone Id within this Cluster
    pub zid: ZoneT

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
        self.start + LbaT::from(self.allocated_blocks)
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
#[derive(Debug)]
struct FreeSpaceMap {
    /// Stores the set of empty zones with id less than zones.len().  All zones
    /// with id greater than or equal to zones.len() are implicitly empty
    empty_zones: BTreeSet<ZoneT>,

    /// Currently open zones
    open_zones: BTreeMap<ZoneT, OpenZone>,

    /// Total number of zones in the vdev
    total_zones: ZoneT,

    /// `Vec` of all zones in the Vdev.  Any zones past the end of the `Vec` are
    /// implicitly Empty.  Any zones whose index is present in `empty_zones` are
    /// also Empty.  Any zones whose index is also present in `open_zones` are
    /// implicitly open.  All other zones are Closed.
    zones: Vec<Zone>,
}

impl<'a> FreeSpaceMap {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    fn allocated(&self) -> LbaT {
        (0..self.zones.len()).map(|idx| {
            let zid = idx as ZoneT;
            if let Some(oz) = self.open_zones.get(&zid) {
                LbaT::from(oz.allocated_blocks)
            } else if self.empty_zones.contains(&zid) {
                0
            } else {
                LbaT::from(self.zones[idx].total_blocks)
            }
        }).sum()
    }

    /// Assert that the given zone was clean as of the given transaction
    fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT) {
        assert!(self.is_empty(zone) ||
                self.zones[zone as usize].txgs.start >= txg,
                "Zone {} did not get fully cleaned: {:?}", zone,
                self.zones[zone as usize]);
    }

    /// How many blocks are available to be immediately written in the Zone?
    fn available(&self, zone_id: ZoneT) -> LbaT {
        if let Some(oz) = self.open_zones.get(&zone_id) {
            let z = &self.zones[zone_id as usize];
            LbaT::from(z.total_blocks - oz.allocated_blocks)
        } else {
            0
        }
    }

    fn deserialize(vdev: VdevRaidLike, buf: DivBuf, zones: ZoneT)
        -> bincode::Result<impl Future<Item=(Self, VdevRaidLike), Error=Error>>
    {
        let mut fsm = FreeSpaceMap::new(zones);
        let mut oz_futs = Vec::new();
        let sods = SpacemapOnDisk::deserialize(buf).unwrap();
        let mut zid: ZoneT = 0;
        for sod in sods.into_iter() {
            if sod.is_err() {
                let fut = Err(sod.unwrap_err()).into_future();
                let r = Box::new(fut) as Box<Future<Item=_, Error=_>>;
                return Ok(r);
            }
            for zod in sod.unwrap().zones.into_iter() {
                if zod.allocated_blocks > 0 {
                    let zl = vdev.zone_limits(zid);
                    fsm.open_zone(zid, zl.0, zl.1, 0, zod.txgs.start).unwrap();
                    if zod.allocated_blocks == u32::max_value() {
                        // Zone is closed
                        fsm.finish_zone(zid, zod.txgs.end - 1);
                    } else {
                        // Zone is Open
                        let allocated = LbaT::from(zod.allocated_blocks);
                        oz_futs.push(vdev.reopen_zone(zid, allocated));
                        let azid = fsm.try_allocate(allocated).0.unwrap().0;
                        assert_eq!(azid, zid);
                    }
                    fsm.zones[zid as usize].freed_blocks = zod.freed_blocks;
                    fsm.zones[zid as usize].txgs = zod.txgs;
                } else {
                    // Zone is empty
                }
                zid += 1;
            }
        }
        assert_eq!(zid, zones);
        let fut = Box::new(future::join_all(oz_futs).map(|_| (fsm, vdev)))
            as Box<Future<Item=_, Error=Error>>;
        Ok(fut)
    }

    /// Return Zone `zone_id` to an Empty state
    fn erase_zone(&mut self, zone_id: ZoneT) {
        let zone_idx = zone_id as usize;
        assert!(!self.is_open(zone_id),
            "Can't erase an open zone");
        assert!(zone_idx < self.zones.len(),
            "Can't erase an empty zone");
        self.empty_zones.insert(zone_id);
        self.zones[zone_idx].txgs = TxgT::from(0)..TxgT::from(0);
        // If this was the last zone, then remove all trailing Empty zones
        if zone_idx == self.zones.len() - 1 {
            let first_empty = {
                let last_nonempty = (0..self.zones.len())
                    .rfind(|i| !self.is_empty(*i as ZoneT));
                if let Some(i) = last_nonempty {
                    i as ZoneT + 1
                } else {
                    0   // All zones are empty!
                }
            };
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

    /// Mark the Zone as closed.  `txg` is the current transaction group
    fn finish_zone(&mut self, zone_id: ZoneT, txg: TxgT) {
        let available = self.available(zone_id) as u32;
        assert!(self.open_zones.remove(&zone_id).is_some(),
            "Can't finish a Zone that isn't open");
        self.zones[zone_id as usize].freed_blocks += available;
        self.zones[zone_id as usize].txgs.end = txg + 1;
    }

    fn free(&mut self, zone_id: ZoneT, length: LbaT) {
        assert!(!self.is_empty(zone_id), "Can't free from an empty zone");
        let zone = self.zones.get_mut(zone_id as usize).expect(
            "Can't free from an empty zone");
        // NB: the next two lines can be replaced by u32::try_from(length), once
        // that feature is stabilized
        // https://github.com/rust-lang/rust/issues/33417
        assert!(length < LbaT::from(u32::max_value()));
        zone.freed_blocks += length as u32;
        assert!(zone.freed_blocks <= zone.total_blocks,
                "Double free detected.  freed={:?}, total={:?}",
                zone.freed_blocks, zone.total_blocks);
        if let Some(oz) = self.open_zones.get(&zone_id) {
            assert!(oz.allocated_blocks >= zone.freed_blocks,
                    "Double free detected in an open zone");
        }
    }

    /// How many blocks are currently allocated and not freed from this zone?
    fn in_use(&self, zone_id: ZoneT) -> LbaT {
        if self.is_empty(zone_id) {
            0
        } else if let Some(oz) = self.open_zones.get(&zone_id) {
            let z = &self.zones[zone_id as usize];
            LbaT::from(oz.allocated_blocks - z.freed_blocks)
        } else /* zone is closed */ {
            let z = &self.zones[zone_id as usize];
            LbaT::from(z.total_blocks - z.freed_blocks)
        }
    }

    /// Is the Zone with the given id closed?
    fn is_closed(&self, zone_id: ZoneT) -> bool {
        zone_id < self.zones.len() as ZoneT &&
            ! self.empty_zones.contains(&zone_id) &&
            ! self.open_zones.contains_key(&zone_id)
    }

    /// Is the Zone with the given id empty?
    fn is_empty(&self, zone_id: ZoneT) -> bool {
        zone_id >= self.zones.len() as ZoneT ||
            self.empty_zones.contains(&zone_id)
    }

    /// Is the Zone with the given id open?
    fn is_open(&self, zone_id: ZoneT) -> bool {
        self.open_zones.contains_key(&zone_id)
    }

    /// Find the next closed zone including or after `start`
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::if_same_then_else))]
    fn find_closed_zone(&'a self, start: ZoneT) -> Option<ClosedZone>
    {
        if start as usize >= self.zones.len() {
            return None;
        }
        self.zones[(start as usize)..].iter()
            .enumerate()
            .filter_map(move |(i, z)| {
                let zid = start + i as ZoneT;
                if self.empty_zones.contains(&zid) {
                    None
                } else if self.open_zones.contains_key(&zid) {
                    None
                } else {
                    Some(ClosedZone {
                        zid,
                        start: LbaT::max_value(),   // sentinel value
                        freed_blocks: LbaT::from(z.freed_blocks),
                        total_blocks: LbaT::from(z.total_blocks),
                        txgs: z.txgs.clone()
                    })
                }
            })
            .nth(0)
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
    /// - `txg`:    Current transaction group
    ///
    /// # Returns
    ///
    /// If `lbas` was nonzero, return the zone id and LBA of the newly allocated
    /// space.  If `lbas` was zero, return `None`.  If `lbas` was nonzero and
    /// the requested zone has insufficient space, return ENOSPC.
    fn open_zone(&mut self, id: ZoneT, start: LbaT, end: LbaT, lbas: LbaT,
                 txg: TxgT) -> Result<Option<(ZoneT, LbaT)>, Error> {
        let idx = id as usize;
        let space = end - start;
        assert!(self.is_empty(id), "Can only open empty zones");
        if space < lbas {
            return Err(Error::ENOSPC);
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
        self.zones[idx].txgs = txg..TxgT(u32::max_value());
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

    /// Open a FreeSpaceMap from an already-formatted `VdevRaid`.
    fn open(vdev: VdevRaidLike)
        -> impl Future<Item=(Self, VdevRaidLike), Error=Error>
    {
        let total_zones = vdev.zones();
        // VdevFile will resize dbs as needed.  NB: it would be slightly faster
        // to created it with the correct capacity and uninitialized.
        let dbs = DivBufShared::with_capacity(0);
        let dbm = dbs.try_mut().unwrap();
        vdev.read_spacemap(dbm, 0)
        .and_then(move |_| {
            FreeSpaceMap::deserialize(vdev, dbs.try().unwrap(), total_zones)
            .unwrap()
        })
    }

    /// Return an iterator over the zone IDs of all open zones
    fn open_zone_ids(&self) -> Keys<ZoneT, OpenZone> {
        self.open_zones.keys()
    }

    /// Serialize this `FreeSpaceMap` so it can be written to a disk's reserved
    /// area
    fn serialize(&self) -> DivBufShared {
        let fsm = (0..self.total_zones).chunks(SPACEMAP_ZONES_PER_LBA)
            .into_iter()
            .enumerate()
            .map(|(i, zids)| {
            let v = zids.map(|z| {
                let allocated_blocks = if self.is_empty(z) {
                    0
                } else {
                    match self.open_zones.get(&z) {
                        Some(oz) => oz.allocated_blocks,
                        None => u32::max_value()
                    }
                };
                let freed_blocks = if self.is_empty(z) {
                    0
                } else {
                    self.zones[z as usize].freed_blocks
                };
                let txgs = if self.is_empty(z) {
                    TxgT::from(0)..TxgT::from(0)
                } else {
                    self.zones[z as usize].txgs.clone()
                };
                ZoneOnDisk{allocated_blocks, freed_blocks, txgs}
            }).collect::<Vec<_>>();
            SpacemapOnDisk::new(i as u64, v)
        }).collect::<Vec<_>>();
        DivBufShared::from(bincode::serialize(&fsm).unwrap())
    }

    /// Try to allocate `space` worth of space in any open zone.  If no open
    /// zones can satisfy the allocation, return `None` instead.
    ///
    /// # Returns
    ///
    /// The Zone and LBA where the allocation happened, and a vector of Zone IDs
    /// of Zones which have too little space.
    fn try_allocate(&mut self, space: LbaT)
        -> (Option<(ZoneT, LbaT)>, Vec<ZoneT>)
    {
        let zones = &self.zones;
        let mut nearly_full_zones = Vec::with_capacity(1);
        let result = self.open_zones.iter_mut().find(|&(zone_id, ref oz)| {
            let zone = &zones[*zone_id as usize];
            let avail_lbas = zone.total_blocks - oz.allocated_blocks;
            // NB the next two lines can be replaced by u32::try_from(space),
            // once that feature is stabilized
            // https://github.com/rust-lang/rust/issues/33417
            assert!(space < LbaT::from(u32::max_value()));
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
    fn waste_space(&mut self, zid: ZoneT, space: LbaT) {
        let oz = self.open_zones.get_mut(&zid).unwrap();
        oz.waste_space(space);
        self.zones[zid as usize].freed_blocks += space as u32;
        assert!(oz.allocated_blocks <= self.zones[zid as usize].total_blocks,
            concat!("waste_space: Wasted too much space!",
                    "zid={} space={} allocated_blocks={} total_blocks={}\n"),
            zid, space, oz.allocated_blocks,
            self.zones[zid as usize].total_blocks);
    }
}

impl Display for FreeSpaceMap {
    /// Print a human-readable summary of the FreeSpaceMap
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        let t = self.total_zones;
        let le = self.empty_zones.len();
        let o = self.open_zones.len();
        let c = self.zones.len() - o - le;
        let e = (t as usize) - c - o;
        let max_txg: u32 = cmp::max(1, (0..self.zones.len())
            .map(|idx| idx as ZoneT)
            .filter(|zid| !self.is_empty(*zid))
            .map(|zid| {
                let z = &self.zones[zid as usize];
                if self.is_open(zid) {
                    z.txgs.start
                } else {
                    cmp::max(z.txgs.start, z.txgs.end)
                }.into()
            }).max()
            .unwrap());

        // First print the header
        writeln!(f, "FreeSpaceMap: {} Zones: {} Closed, {} Empty, {} Open",
            t, c, e, o)?;
        let zone_width = cmp::max(5, f64::from(t + 1).log(16.0).ceil() as usize);
        let txg_width = cmp::max(1,
            f64::from(max_txg + 1).log(16.0).ceil() as usize);
        let space_width = 80 - zone_width - 2 * txg_width - 7;
        let sw64 = space_width as f64;
        writeln!(f, "{0:^1$}|{2:^3$}|{4:^5$}|", "Zone", zone_width + 1,
               "TXG", txg_width * 2 + 3, "Space", space_width)?;
        writeln!(f, "{0:-^1$}|{2:-^3$}|{4:-^5$}|", "", zone_width + 1,
               "", txg_width * 2 + 3, "", space_width)?;

        // Now loop over the zones
        let mut last_row: Option<String> = None;
        for i in 0..self.zones.len() {
            let total = f64::from(self.zones[i].total_blocks);
            // We want to round used + free up so the graph won't overestimate
            // available space, but we want to display used and free separately.
            let (used_width, freed_width) = if self.is_empty(i as ZoneT) {
                (0, 0)
            } else {
                let used = self.in_use(i as ZoneT) as f64;
                let freed = f64::from(self.zones[i].freed_blocks);
                let not_avail_width = ((used + freed) / total * sw64).round()
                    as usize;
                let used_width = (used / total * sw64).round() as usize;
                let freed_width = not_avail_width - used_width;
                (used_width, freed_width)
            };
            let avail_width = space_width - freed_width - used_width;
            let start = if self.is_empty(i as ZoneT) {
                format!("{0:1$}", "", txg_width)
            } else {
                let x: u32 = self.zones[i].txgs.start.into();
                format!("{0:>1$x}", x, txg_width)
            };
            let end = if self.is_closed(i as ZoneT) {
                let x: u32 = self.zones[i].txgs.end.into();
                format!("{0:>1$x}", x, txg_width)
            } else {
                format!("{0:1$}", "", txg_width)
            };
            // Repeated row compression: if two or more rows are identical but
            // for the zone number, only print the first
            let this_row = format!("{0}-{1} |{2:3$}{4:=>5$}{6:7$}", start, end,
                                   "", freed_width, "", used_width,
                                   "", avail_width);
            if let Some(ref row) = last_row {
                if *row == this_row {
                    continue;
                }
            }
            writeln!(f, "{0:>1$x} | {2}|", i, zone_width, this_row)?;
            last_row = Some(this_row);
        }

        // Print a single row for trailing empty zones, if any
        if t > self.zones.len() as u32 {
            writeln!(f, "{0:>1$x} | {2:^3$} |{4:5$}|",
                   self.zones.len(), zone_width, "-", 2 * txg_width + 1,
                   "", space_width)?;
        }
        Ok(())
    }
}

/// Persists the `FreeSpaceMap` in the reserved region of the disk
#[derive(Serialize, Deserialize, Debug, Hash)]
struct ZoneOnDisk {
    /// The number of blocks that have been allocated in each Zone.  If zero,
    /// then the zone is empty.  If `u32::max_value()`, then the zone is closed.
    allocated_blocks: u32,

    /// Number of LBAs that have been freed from this `Zone` since it was
    /// opened.
    freed_blocks: u32,

    /// The range of transactions that have been written to this Zone.  The
    /// start is inclusive, and the end is exclusive
    ///
    /// The end is invalid for open zones, and both start and end are invalid
    /// for empty zones.
    txgs: Range<TxgT>
}

/// Persists the `FreeSpaceMap` in the reserved region of the disk.  Each one of
/// these structures stores the allocations of as many zones as can fit into
/// 4KB.
#[derive(Serialize, Deserialize, Debug)]
struct SpacemapOnDisk {
    /// MetroHash64 self-checksum.  Includes the index of this `SpacemapOnDisk`
    /// within the overall spacemap, to detect misdirected writes.
    checksum: u64,
    zones: Vec<ZoneOnDisk>
}

impl SpacemapOnDisk {
    fn deserialize(buf: DivBuf) -> bincode::Result<Vec<Result<Self, Error>>> {
        bincode::deserialize::<Vec<SpacemapOnDisk>>(&buf[..])
        .map(|v| {
            v.into_iter()
            .enumerate()
            .map(|(i, sod)| {
                let mut hasher = MetroHash64::new();
                hasher.write_u64(i as u64);
                sod.zones.hash(&mut hasher);
                if hasher.finish() == sod.checksum {
                    Ok(sod)
                } else {
                    Err(Error::ECKSUM)
                }
            }).collect::<Vec<_>>()
        })
    }

    fn new(i: u64, v: Vec<ZoneOnDisk>) -> Self {
        debug_assert!(v.len() <= SPACEMAP_ZONES_PER_LBA);
        let mut hasher = MetroHash64::new();
        hasher.write_u64(i);
        v.hash(&mut hasher);
        SpacemapOnDisk {
            checksum: hasher.finish(),
            zones: v
        }
    }
}

/// A `Cluster` is BFFFS's equivalent of ZFS's top-level Vdev.  It is the
/// highest level `Vdev` that has its own LBA space.
pub struct Cluster {
    fsm: RefCell<FreeSpaceMap>,

    /// Underlying vdev (which may or may not use RAID)
    // The Rc is necessary in order for some methods to return futures with
    // 'static lifetimes
    vdev: Rc<VdevRaidLike>
}

/// Finish any zones that are too full for new allocations.
///
/// This defines the policy of when to close nearly full zones.
// Logically, it's a method of Cluster.  But it needs to be implemented as a
// macro, because it returns a Vec of an anonymous type
macro_rules! close_zones{
    ( $self:ident, $nearly_full_zones:expr, $txg:expr) => {
        $nearly_full_zones.iter().map(|&zone_id| {
            $self.fsm.borrow_mut().finish_zone(zone_id, $txg);
            $self.vdev.finish_zone(zone_id)
        }).collect::<Vec<_>>()
    }
}

impl<'a> Cluster {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.fsm.borrow().allocated()
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Assert that the given zone was clean as of the given transaction
    pub fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT) {
        self.fsm.borrow().assert_clean_zone(zone, txg)
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Create a new `Cluster` from unused files or devices
    ///
    /// * `chunksize`:          RAID chunksize in LBAs, if specified.  This is
    ///                         the largest amount of data that will be
    ///                         read/written to a single device before the
    ///                         `Locator` switches to the next device.
    /// * `num_disks`:          Total number of disks in the array
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than
    ///                         or equal to `num_disks`.
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `paths`:              Slice of pathnames of files and/or devices
    #[cfg(not(test))]
    pub fn create<P: AsRef<Path>>(chunksize: Option<NonZeroU64>,
                                  num_disks: i16,
                                  disks_per_stripe: i16,
                                  lbas_per_zone: Option<NonZeroU64>,
                                  redundancy: i16,
                                  paths: &[P]) -> Self
    {
        let vdev = VdevRaid::create(chunksize, num_disks, disks_per_stripe,
                                    lbas_per_zone, redundancy, paths);
        let total_zones = vdev.zones();
        let fsm = FreeSpaceMap::new(total_zones);
        Cluster::new(fsm, vdev)
    }

    /// Dump the FreeSpaceMap in human-readable form, for debugging purposes
    #[doc(hidden)]
    pub fn dump_fsm(&self) -> String {
        format!("{}", self.fsm.borrow())
    }

    /// Delete the underlying storage for a Zone.
    fn erase_zone(&self, zone: ZoneT)
        -> impl Future<Item=(), Error=Error>
    {
        self.fsm.borrow_mut().erase_zone(zone);
        self.vdev.erase_zone(zone)
    }

    /// Find the first closed zone whose index is greater than or equal to `zid`
    pub fn find_closed_zone(&self, zid: ZoneT) -> Option<ClosedZone> {
        self.fsm.borrow().find_closed_zone(zid)
            .map(|mut zone| {
                zone.start = self.vdev.zone_limits(zone.zid).0;
                zone
            })
    }

    /// Mark `length` LBAs beginning at LBA `lba` as unused, and possibly delete
    /// them from the underlying storage.
    ///
    /// Deleting data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    pub fn free(&self, lba: LbaT, length: LbaT)
        -> Box<Future<Item=(), Error=Error>>
    {
        let start_zone = self.vdev.lba2zone(lba).expect(
            "Can't free from inter-zone padding");
        #[cfg(test)]
        {
            let end_zone = self.vdev.lba2zone(lba + length - 1).expect(
                "Can't free from inter-zone padding");
            assert_eq!(start_zone, end_zone,
                "Can't free across multiple zones");
        }
        let mut fsm = self.fsm.borrow_mut();
        fsm.free(start_zone, length);
        // Erase the zone if it is fully freed
        if fsm.is_closed(start_zone) && fsm.in_use(start_zone) == 0 {
            drop(fsm);
            Box::new(self.erase_zone(start_zone))
        } else {
            Box::new(Ok(()).into_future())
        }
    }

    /// Construct a new `Cluster` from an already constructed
    /// [`VdevRaid`](struct.VdevRaid.html)
    fn new(fsm: FreeSpaceMap, vdev: VdevRaidLike) -> Self {
        Cluster{fsm: RefCell::new(fsm), vdev: Rc::new(vdev)}
    }

    /// Open a `Cluster` from an already opened
    /// [`VdevRaid`](struct.VdevRaid.html)
    ///
    /// Returns a new `Cluster` and a `LabelReader` that may be used to
    /// construct other vdevs stacked on top.
    pub fn open(vdev_raid: VdevRaidLike) -> impl Future<Item=Self, Error=Error>
    {
        FreeSpaceMap::open(vdev_raid)
            .map(move |(fsm, vdev_raid)| (Cluster::new(fsm, vdev_raid)))
    }

    /// Returns the "best" number of operations to queue to this `Cluster`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    pub fn optimum_queue_depth(&self) -> u32 {
        self.vdev.optimum_queue_depth()
    }

    /// Asynchronously read from the cluster
    pub fn read(&self, buf: IoVecMut, lba: LbaT)
        -> impl Future<Item=(), Error=Error>
    {
        self.vdev.read_at(buf, lba)
    }

    /// Return approximately the usable space of the Cluster in LBAs.
    pub fn size(&self) -> LbaT {
        self.vdev.size()
    }

    /// Sync the `Cluster`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&self) -> impl Future<Item=(), Error=Error> {
        let mut fsm = self.fsm.borrow_mut();
        let zone_ids = fsm.open_zone_ids().cloned().collect::<Vec<_>>();
        let flush_futs = zone_ids.iter().map(|&zone_id| {
            let (gap, fut) = self.vdev.flush_zone(zone_id);
            fsm.waste_space(zone_id, gap);
            fut
        }).collect::<Vec<_>>();
        let flush_fut = future::join_all(flush_futs);
        let vdev2 = self.vdev.clone();
        flush_fut.and_then(move |_| vdev2.sync_all())
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Return the `Cluster`'s UUID.  It's the same as its RAID device's.
    pub fn uuid(&self) -> Uuid {
        self.vdev.uuid()
    }

    /// Write a buffer to the cluster
    ///
    /// # Returns
    ///
    /// The LBA where the data will be written, and a
    /// `Future` for the operation in progress.
    pub fn write(&self, buf: IoVec, txg: TxgT)
        -> Result<(LbaT, Box<ClusterFut>), Error> {
        // Outline:
        // 1) Try allocating in an open zone
        // 2) If that doesn't work, try opening a new one, and allocating from
        //    that
        // 3) If that doesn't work, return ENOSPC
        // 4) write to the vdev
        let space = div_roundup(buf.len(), BYTES_PER_LBA) as LbaT;
        let (alloc_result, nearly_full_zones) =
            self.fsm.borrow_mut().try_allocate(space);
        let finish_futs = close_zones!(self, &nearly_full_zones, txg);
        let vdev2 = self.vdev.clone();
        let vdev3 = self.vdev.clone();
        alloc_result.map(|(zone_id, lba)| {
            let oz_fut: Box<ClusterFut> = Box::new(future::ok::<(),
                                                            Error>(()));
            (zone_id, lba, oz_fut)
        }).or_else(|| {
            let empty_zone = self.fsm.borrow().find_empty();
            empty_zone.and_then(|zone_id| {
                let zl = vdev2.zone_limits(zone_id);
                let e = self.fsm.borrow_mut().open_zone(zone_id, zl.0, zl.1,
                                                        space, txg);
                match e {
                    Ok(Some((zone_id, lba))) => {
                        let oz_fut = Box::new(
                            vdev2.open_zone(zone_id)
                        ) as Box<VdevFut>;
                        Some((zone_id, lba, oz_fut))
                    },
                    Err(_) => None,
                    Ok(None) => panic!("Tried a 0-length write?"),
                }
            })  // LCOV_EXCL_LINE   kcov false negative
        }).map(|(zone_id, lba, oz_fut)| {
            let fut : Box<Future<Item = (), Error = Error>>;
            let wfut = vdev3.write_at(buf, zone_id, lba);
            let owfut = oz_fut.and_then(move |_| {
                wfut
            }
            );
            fut = Box::new(future::join_all(finish_futs).join(owfut).map(|_| ()));
            (lba, fut)
        }).ok_or(Error::ENOSPC)
    }

    /// Asynchronously write this cluster's label to all component devices
    /// All data and spacemap should be written and synced first!
    pub fn write_label(&self, labeller: LabelWriter)
        -> impl Future<Item=(), Error=Error>
    {   // LCOV_EXCL_LINE   kcov false negative
        self.vdev.write_label(labeller)
    }

    /// Asynchronously write this Cluster's spacemap to all component devices
    /// `idx` is the index of the label being written.
    pub fn write_spacemap(&self, idx: u32)
        -> impl Future<Item=(), Error=Error>
    {
        let fsm = self.fsm.borrow().serialize();
        self.vdev.write_spacemap(fsm.try().unwrap(), idx, 0)
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mod t {

mod open_zone {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let oz = OpenZone{start: 0, allocated_blocks: 0};
        format!("{:?}", oz);
    }
}

#[cfg(feature = "mocks")]
mod cluster {
    use super::super::*;
    use divbuf::DivBufShared;
    use mockers::{Scenario, check, matchers};
    use mockers_derive::mock;
    use tokio::runtime::current_thread;

    mock!{
        MockVdevRaid,
        vdev,
        trait Vdev {
            fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
            fn optimum_queue_depth(&self) -> u32;
            fn size(&self) -> LbaT;
            fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
            fn uuid(&self) -> Uuid;
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
            fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut>;
            fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> Box<VdevFut>;
            fn write_at(&self, buf: IoVec, zone: ZoneT,
                        lba: LbaT) -> Box<VdevFut>;
            fn write_label(&self, labeller: LabelWriter) -> Box<VdevFut>;
            fn write_spacemap(&self, buf: IoVec, idx: u32, block: LbaT)
                -> Box<VdevFut>;
        }
    }

    #[test]
    fn free_and_erase_full_zone() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.lba2zone_call(1).and_return_clone(Some(0)).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((1, 2)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((2, 200)).times(..));
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 0, matchers::ANY)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.finish_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.open_zone_call(1)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 1, matchers::ANY)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.erase_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let db1 = db0.clone();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let (lba, fut1) = cluster.write(db0, TxgT::from(0))
                .expect("write failed early");
            // Write a 2nd time so the first zone will get closed
            fut1.and_then(|_| {
                let (_, fut2) = cluster.write(db1, TxgT::from(0))
                    .expect("write failed early");
                fut2
            }).map(move|_| lba)
                .and_then(|lba| {
                cluster.free(lba, 1)
            })
        })).unwrap();
    }

    #[test]
    fn free_and_erase_nonfull_zone() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.lba2zone_call(1).and_return_clone(Some(0)).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((1, 3)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((3, 200)).times(..));
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 0, matchers::ANY)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.finish_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.open_zone_call(1)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 1, matchers::ANY)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.erase_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs0.try().unwrap();
        let db1 = dbs1.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let (lba, fut1) = cluster.write(db0, TxgT::from(0))
                .expect("write failed early");
            // Write a larger buffer so the first zone will get closed
            fut1.and_then(|_| {
                let (_, fut2) = cluster.write(db1, TxgT::from(0))
                    .expect("write failed early");
                fut2
            }).map(move|_| lba)
                .and_then(|lba| {
                cluster.free(lba, 1)
            })
        })).unwrap();
    }

    #[test]
    fn free_and_dont_erase_zone() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.lba2zone_call(1).and_return_clone(Some(0)).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((1, 3)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((3, 200)).times(..));
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.open_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));

        // .times can't be used with and_call
        // https://github.com/kriomant/mockers/issues/32
        s.expect(vr.write_at_call(matchers::ANY, 0, 1)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 0, 2)
            .and_return(Box::new( future::ok::<(), Error>(()))));

        s.expect(vr.finish_zone_call(0)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.open_zone_call(1)
            .and_return(Box::new( future::ok::<(), Error>(()))));
        s.expect(vr.write_at_call(matchers::ANY, 1, matchers::ANY)
            .and_return(Box::new( future::ok::<(), Error>(()))));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs0.try().unwrap();
        let db1 = dbs0.try().unwrap();
        let db2 = dbs1.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let (lba, fut1) = cluster.write(db0, TxgT::from(0))
                .expect("write failed early");
            fut1.and_then(|_| {
                let (_, fut2) = cluster.write(db1, TxgT::from(0))
                    .expect("write failed early");
                fut2
            })
            // Write a larger buffer so the first zone will get closed
            .and_then(|_| {
                let (_, fut3) = cluster.write(db2, TxgT::from(0))
                    .expect("write failed early");
                fut3
            }).map(move|_| lba)
                .and_then(|lba| {
                cluster.free(lba, 1)
            })
        })).unwrap();
    }

    #[test]
    #[should_panic(expected = "Can't free across")]
    fn free_crosszone_padding() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.lba2zone_call(900).and_return_clone(Some(0)).times(..));
        s.expect(vr.lba2zone_call(1099).and_return_clone(Some(1)).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((1, 1000)).times(..));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));
        cluster.free(900, 200);
    }

    #[test]
    #[should_panic(expected = "Can't free from inter-zone padding")]
    fn free_interzone_padding() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.lba2zone_call(1000).and_return_clone(None).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((1, 1000)).times(..));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));
        cluster.free(1000, 10);
    }

    // FreeSpaceMap::open with the following conditions:
    // A closed zone with no freed blocks
    // A closed zone with some freed blocks
    // An empty zone before the maximum open or full zone
    // An open zone with some freed blocks
    // A trailing empty zone
    #[test]
    fn freespacemap_open() {
        // Serialized spacemap
        const SPACEMAP: [u8; 104] = [
            1, 0, 0, 0, 0, 0, 0, 0,         // 1 SOD
            0xb2, 0x99, 0x6a, 0xb1, 0xa0, 0x6f, 0xb4, 0x9c, // Checksum
            5, 0, 0, 0, 0, 0, 0, 0,         // 5 entries
            255, 255, 255, 255,             // zone0: allocated_blocks
            0, 0, 0, 0,                     // zone0: freed blocks
            0, 0, 0, 0, 2, 0, 0, 0,         // zone0 txgs: 0..2
            255, 255, 255, 255,             // zone1: allocated_blocks
            22, 0, 0, 0,                    // zone1: freed blocks
            1, 0, 0, 0, 3, 0, 0, 0,         // zone1 txgs: 1..3
            0, 0, 0, 0,                     // zone2: allocated_blocks
            0, 0, 0, 0,                     // zone2: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone2 txgs: 0..0
            77, 0, 0, 0,                    // zone3: allocated_blocks
            33, 0, 0, 0,                    // zone3: freed blocks
            2, 0, 0, 0, 255, 255, 255, 255, // zone3 txgs: 2..u32::MAX
            0, 0, 0, 0,                     // zone4: allocated_blocks
            0, 0, 0, 0,                     // zone4: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone4 txgs: 0..0
        ];
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(5).times(..));
        s.expect(vr.read_spacemap_call(matchers::ANY, 0)
                 .and_call(|mut dbm: DivBufMut, _idx: u32| {
                     dbm.try_truncate(0).unwrap();
                     dbm.extend(SPACEMAP.iter());
                     Box::new(future::ok::<(), Error>(()))
                 })
        );

        s.expect(vr.reopen_zone_call(3, 77).and_return(
                Box::new(Ok(()).into_future())
        ));
        s.expect(vr.zone_limits_call(0).and_return_clone((4, 96)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((104, 196)).times(..));
        s.expect(vr.zone_limits_call(2).and_return_clone((204, 296)).times(..));
        s.expect(vr.zone_limits_call(3).and_return_clone((304, 396)).times(..));
        s.expect(vr.zone_limits_call(4).and_return_clone((404, 496)).times(..));
        let mock_vr: Box<VdevRaidTrait> = Box::new(vr);
        let (fsm, _mock_vr) = FreeSpaceMap::open(mock_vr).wait().unwrap();
        assert_eq!(fsm.zones.len(), 4);
        assert_eq!(fsm.zones[0].freed_blocks, 0);
        assert_eq!(fsm.zones[0].total_blocks, 92);
        assert_eq!(fsm.zones[0].txgs, TxgT::from(0)..TxgT::from(2));
        assert_eq!(fsm.zones[1].freed_blocks, 22);
        assert_eq!(fsm.zones[1].total_blocks, 92);
        assert_eq!(fsm.zones[1].txgs, TxgT::from(1)..TxgT::from(3));
        assert!(fsm.is_empty(2));
        assert_eq!(fsm.zones[3].freed_blocks, 33);
        assert_eq!(fsm.zones[3].total_blocks, 92);
        assert_eq!(fsm.zones[3].txgs.start, TxgT::from(2));
        let oz = &fsm.open_zones[&3];
        assert_eq!(oz.start, 304);
        assert_eq!(oz.allocated_blocks, 77);
        assert!(fsm.is_empty(4));
    }

    // TODO: also, serde tests for > 1 SOD
    #[test]
    fn freespacemap_open_ecksum() {
        // Serialized spacemap
        const SPACEMAP: [u8; 24] = [
            1, 0, 0, 0, 0, 0, 0, 0,         // 1 SOD
            0, 0, 0, 0, 0, 0, 0, 0,         // Checksum
            0, 0, 0, 0, 0, 0, 0, 0,         // 0 entries
        ];
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(0).times(..));
        s.expect(vr.read_spacemap_call(matchers::ANY, 0)
                 .and_call(|mut dbm: DivBufMut, _idx: u32| {
                     dbm.try_truncate(0).unwrap();
                     dbm.extend(SPACEMAP.iter());
                     Box::new(future::ok::<(), Error>(()))
                 })
        );

        let mock_vr: Box<VdevRaidTrait> = Box::new(vr);
        let r = FreeSpaceMap::open(mock_vr).wait();
        assert_eq!(Error::ECKSUM, r.err().unwrap());
    }

    #[test]
    fn find_closed_zone() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1)).times(..));
        s.expect(vr.zone_limits_call(1).and_return_clone((1, 2)).times(..));
        s.expect(vr.zone_limits_call(3).and_return_clone((3, 4)).times(..));
        s.expect(vr.zone_limits_call(4).and_return_clone((4, 5)).times(..));
        let mut fsm = FreeSpaceMap::new(10);
        fsm.open_zone(0, 0, 1, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        fsm.open_zone(2, 2, 3, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(3, 3, 4, 0, TxgT::from(1)).unwrap();
        fsm.finish_zone(3, TxgT::from(3));
        fsm.open_zone(4, 4, 5, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(4, TxgT::from(0));
        let cluster = Cluster::new(fsm, Box::new(vr));
        assert_eq!(cluster.find_closed_zone(0).unwrap(),
            ClosedZone{zid: 0, start: 0, freed_blocks: 1, total_blocks: 1,
                       txgs: TxgT::from(0)..TxgT::from(1)});
        assert_eq!(cluster.find_closed_zone(1).unwrap(),
            ClosedZone{zid: 3, start: 3, freed_blocks: 1, total_blocks: 1,
                       txgs: TxgT::from(1)..TxgT::from(4)});
        assert_eq!(cluster.find_closed_zone(4).unwrap(),
            ClosedZone{zid: 4, start: 4, freed_blocks: 1, total_blocks: 1,
                       txgs: TxgT::from(0)..TxgT::from(1)});
        assert!(cluster.find_closed_zone(5).is_none());
    }

    // VdevRaid::write_at must be called synchronously with Cluster::write, even
    // if opening a zone is slow.
    #[test]
    fn open_zone_slow() {
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
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let _ = cluster.write(db0, TxgT::from(0)).expect("write failed early");
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
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let (_, fut) = cluster.write(db0, TxgT::from(0))
                .expect("write failed early");
            fut.and_then(|_| cluster.sync_all())
        })).unwrap();
        let fsm = cluster.fsm.borrow();
        assert_eq!(fsm.open_zones[&0].write_pointer(), 6);
        assert_eq!(fsm.zones[0].freed_blocks, 5);
    }

    #[test]
    fn write_zones_too_small() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        s.expect(vr.zones_call().and_return_clone(32768).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 1)).times(..));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let mut rt = current_thread::Runtime::new().unwrap();
        let result = rt.block_on(future::lazy(|| {
            cluster.write(dbs.try().unwrap(), TxgT::from(0))
        }));
        assert_eq!(result.err().unwrap(), Error::ENOSPC);
    }

    #[test]
    fn write_no_available_zones() {
        let s = Scenario::new();
        let vr = s.create_mock::<MockVdevRaid>();
        // What a useless disk ...
        s.expect(vr.zones_call().and_return_clone(0).times(..));
        s.expect(vr.zone_limits_call(0).and_return_clone((0, 0)).times(..));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let result = cluster.write(dbs.try().unwrap(), TxgT::from(0));
        assert_eq!(result.err().unwrap(), Error::ENOSPC);
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
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let mut rt = current_thread::Runtime::new().unwrap();
        let result = rt.block_on(future::lazy(|| {
            let (lba, fut) = cluster.write(db0, TxgT::from(0))
                .expect("write failed early");
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
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try().unwrap();
        let db1 = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let cluster_ref = &cluster;
            let (_, fut0) = cluster.write(db0, TxgT::from(0))
                .expect("Cluster::write");
            fut0.and_then(move |_| {
                let (lba1, fut1) = cluster_ref.write(db1, TxgT::from(0))
                    .expect("Cluster::write");
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
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new(fsm, Box::new(vr));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs.try().unwrap();
        let db1 = dbs.try().unwrap();
        current_thread::Runtime::new().unwrap().block_on(future::lazy(|| {
            let cluster_ref = &cluster;
            let (_, fut0) = cluster.write(db0, TxgT::from(0))
                .expect("Cluster::write");
            fut0.and_then(move |_| {
                let (lba1, fut1) = cluster_ref.write(db1, TxgT::from(0))
                    .expect("Cluster::write");
                assert_eq!(lba1, 3);
                fut1
            })
        })).expect("write failed");
    }

}

mod free_space_map {
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let fsm = FreeSpaceMap::new(10);
        format!("{:?}", fsm);
    }

    #[test]
    fn allocated_all_empty() {
        let fsm = FreeSpaceMap::new(32768);
        assert_eq!(0, fsm.allocated());
    }

    #[test]
    fn allocated_one_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        assert_eq!(1000, fsm.allocated());
    }

    #[test]
    fn allocated_one_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 200, TxgT::from(0)).unwrap();
        assert_eq!(200, fsm.allocated());
    }

    #[test]
    fn allocated_one_empty_two_closed_two_open_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 200, TxgT::from(0)).unwrap();
        // Leave zone 1 empty
        fsm.open_zone(2, 3000, 4000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.open_zone(3, 4000, 5000, 500, TxgT::from(0)).unwrap();
        fsm.open_zone(4, 5000, 7000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(4, TxgT::from(0));
        assert_eq!(3700, fsm.allocated());
    }

    // FreeSpaceMap::display with the following conditions:
    // A full zone with some freed blocks
    // Two empty zones before the maximum open or full zone
    // An open zone with some freed blocks
    // One million trailing empty zones
    #[test]
    fn display() {
        let mut fsm = FreeSpaceMap::new(1_000_004);
        fsm.open_zone(0, 4, 96, 88, TxgT::from(1)).unwrap();
        fsm.finish_zone(0, TxgT::from(2));
        fsm.free(0, 22);
        fsm.open_zone(3, 204, 296, 77, TxgT::from(10)).unwrap();
        fsm.free(3, 33);
        let expected =
r#"FreeSpaceMap: 1000004 Zones: 1 Closed, 1000002 Empty, 1 Open
 Zone | TXG |                              Space                               |
------|-----|------------------------------------------------------------------|
    0 | 1-3 |                   ===============================================|
    1 |  -  |                                                                  |
    3 | a-  |                       ================================           |
    4 |  -  |                                                                  |
"#;
        assert_eq!(expected, format!("{}", fsm));
    }

    // FreeSpaceMap::display with no closed zones.  Just an empty and an open
    #[test]
    fn display_no_closed_zones() {
        let mut fsm = FreeSpaceMap::new(2);
        fsm.open_zone(0, 4, 96, 88, TxgT::from(0)).unwrap();
        fsm.free(0, 22);
        let expected =
r#"FreeSpaceMap: 2 Zones: 0 Closed, 1 Empty, 1 Open
 Zone | TXG |                              Space                               |
------|-----|------------------------------------------------------------------|
    0 | 0-  |                ===============================================   |
    1 |  -  |                                                                  |
"#;
        assert_eq!(expected, format!("{}", fsm));
    }

    // FreeSpaceMap::display where the used and free space both want to round
    // up.
    #[test]
    fn display_used_free_half_columns() {
        let mut fsm = FreeSpaceMap::new(1);
        fsm.open_zone(0, 0, 2048, 1648, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(15));
        let expected =
r#"FreeSpaceMap: 1 Zones: 1 Closed, 0 Empty, 0 Open
 Zone |  TXG  |                             Space                              |
------|-------|----------------------------------------------------------------|
    0 |  0-10 |            ====================================================|
"#;
        assert_eq!(expected, format!("{}", fsm));
    }

    #[test]
    fn erase_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap();
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
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(1, 1000, 2000, 1, TxgT::from(0)).unwrap();
        fsm.finish_zone(1, TxgT::from(0));
        fsm.erase_zone(1);
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.in_use(1), 0);
        assert_eq!(fsm.zones.len(), 1);
    }

    #[test]
    fn erase_last_zone_with_empties_behind_it() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.erase_zone(2);
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.zones.len(), 1);
    }

    #[test]
    fn erase_last_zone_with_all_other_zones_empty() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.erase_zone(2);
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Can't erase an open zone")]
    fn erase_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.erase_zone(0);
    }

    // Find the first closed zone starting with a given ZoneT
    // FSM should look like this:
    // 0:   closed
    // 1:   empty
    // 2:   open
    // 3:   closed
    // 4:   closed
    // ...  empty
    #[test]
    fn find_closed_zone() {
        let mut fsm = FreeSpaceMap::new(10);
        fsm.open_zone(0, 0, 1, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        fsm.open_zone(2, 2, 3, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(3, 3, 4, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(3, TxgT::from(0));
        fsm.open_zone(4, 4, 5, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(4, TxgT::from(0));
        assert_eq!(fsm.find_closed_zone(1).unwrap().zid, 3);
    }

    // find_closed_zone should fail because there are no closed zones
    #[test]
    fn find_closed_zone_no_closed_zones() {
        let mut fsm = FreeSpaceMap::new(10);
        fsm.open_zone(0, 0, 1, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        fsm.open_zone(2, 2, 3, 0, TxgT::from(0)).unwrap();
        assert!(fsm.find_closed_zone(1).is_none());
    }

    // find_closed_zone should fail for requests past the end of the last
    // non-empty zone.
    #[test]
    fn find_closed_zone_out_of_bounds() {
        let mut fsm = FreeSpaceMap::new(10);
        fsm.open_zone(0, 0, 1, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        assert!(fsm.find_closed_zone(2).is_none());
    }

    #[test]
    fn find_empty_enospc() {
        let mut fsm = FreeSpaceMap::new(2);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none();
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap().is_none();
        fsm.finish_zone(1, TxgT::from(0));
        assert_eq!(fsm.find_empty(), None);
    }

    #[test]
    fn find_empty_explicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none();
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap().is_none();
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn find_empty_implicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none();
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn finish() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap().is_none();
        fsm.finish_zone(zid, TxgT::from(1));
        assert!(!fsm.open_zones.contains_key(&zid));
        assert!(!fsm.is_empty(zid));
        assert_eq!(fsm.zones[zid as usize].txgs, TxgT::from(0)..TxgT::from(2));
    }

    #[should_panic(expected = "Can't finish a Zone that isn't open")]
    #[test]
    fn finish_explicitly_empty() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        // First, open zone 1 so zone 0 will become explicitly empty
        let txg = TxgT::from(0);
        assert!(fsm.open_zone(1, 1000, 2000, 0, txg).unwrap().is_none());
        fsm.finish_zone(zid, txg);
    }

    #[should_panic(expected = "Can't finish a Zone that isn't open")]
    #[test]
    fn finish_implicitly_empty() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.finish_zone(zid, TxgT::from(0));
    }

    #[test]
    #[should_panic(expected = "Double free")]
    fn free_double_free_from_closed_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 10;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space, TxgT::from(0)).unwrap();
        fsm.finish_zone(zid, TxgT::from(0));
        fsm.free(zid, space);
        fsm.free(zid, space);
    }

    #[test]
    #[should_panic(expected = "Double free")]
    fn free_double_free_from_open_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, space, TxgT::from(0)).unwrap();
        fsm.free(zid, space);
        fsm.free(zid, space);
    }

    #[test]
    fn free_from_closed_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 1000;
        let used: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, used, TxgT::from(0)).unwrap();
        fsm.finish_zone(zid, TxgT::from(0));
        assert_eq!(LbaT::from(fsm.zones[zid as usize].freed_blocks),
                   space - used);
        fsm.free(zid, used);
        assert_eq!(LbaT::from(fsm.zones[zid as usize].freed_blocks), space);
    }

    #[test]
    #[should_panic(expected = "free from an empty zone")]
    fn free_from_explicitly_empty_zone() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        // First, open zone 1 so zone 0 will become explicitly empty
        let txg = TxgT::from(0);
        assert!(fsm.open_zone(1, 1000, 2000, 0, txg).unwrap().is_none());
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
        fsm.open_zone(zid, 0, 1000, space, TxgT::from(0)).unwrap();
        fsm.free(zid, space);
        assert_eq!(LbaT::from(fsm.zones[zid as usize].freed_blocks), space);
    }

    #[test]
    fn open() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(5);
        assert!(fsm.open_zone(zid, 0, 1000, 0, txg).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 1);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.zones[zid as usize].txgs.start, txg);
        assert_eq!(fsm.open_zones[&zid].start, 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 0);
    }

    #[test]
    fn open_and_allocate() {
        let zid: ZoneT = 0;
        let space: LbaT = 17;
        let mut fsm = FreeSpaceMap::new(32768);
        assert_eq!(fsm.open_zone(zid, 0, 1000, space, TxgT::from(0)).unwrap(),
                   Some((zid, 0)));
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
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(zid, 0, 1000, space, txg).unwrap_err(),
            Error::ENOSPC);
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    fn open_explicitly_empty() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);

        // First, open zone 1 so zone 0 will become explicitly empty
        assert!(fsm.open_zone(1, 1000, 2000, 0, txg).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 2);

        // Now try to open an explicitly empty zone
        let zid: ZoneT = 0;
        assert!(fsm.open_zone(zid, 0, 1000, 0, txg).unwrap().is_none());
        assert_eq!(fsm.zones.len(), 2);
        assert_eq!(fsm.zones[zid as usize].total_blocks, 1000);
        assert_eq!(fsm.zones[zid as usize].freed_blocks, 0);
        assert_eq!(fsm.open_zones[&zid].start, 0);
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 0);
    }

    #[test]
    fn open_implicitly_empty() {
        let zid: ZoneT = 1;
        let txg = TxgT::from(0);
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 1000, 2000, 0, txg).unwrap().is_none());
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
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap();
    }

    #[test]
    #[should_panic(expected="Can only open empty zones")]
    fn open_closed_zone() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(zid, TxgT::from(0));
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap();
    }

    // FreeSpaceMap::serialize with the following conditions:
    // A full zone with some freed blocks
    // An empty zone before the maximum open or full zone
    // An open zone with some freed blocks
    // A trailing empty zone
    #[test]
    fn serialize() {
        const EXPECTED: [u8; 88] = [
            1, 0, 0, 0, 0, 0, 0, 0,         // 1 SOD
            18, 213, 216, 8, 231, 94, 198, 193, // Checksum
            4, 0, 0, 0, 0, 0, 0, 0,         // 4 ZODs
            255, 255, 255, 255,             // zone0: allocated_blocks
            26, 0, 0, 0,                    // zone0: freed blocks
            1, 0, 0, 0, 3, 0, 0, 0,         // zone0 txgs: 1..3
            0, 0, 0, 0,                     // zone1: allocated_blocks
            0, 0, 0, 0,                     // zone1: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone1 txgs: DON'T CARE
            77, 0, 0, 0,                    // zone2: allocated_blocks
            33, 0, 0, 0,                    // zone2: freed blocks
            2, 0, 0, 0, 255, 255, 255, 255, // zone2 txgs: 2..DON'T CARE
            0, 0, 0, 0,                     // zone3: allocated_blocks
            0, 0, 0, 0,                     // zone3: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone3 txgs: DON'T CARE
        ];
        let mut fsm = FreeSpaceMap::new(4);
        fsm.open_zone(0, 4, 96, 88, TxgT::from(1)).unwrap();
        fsm.finish_zone(0, TxgT::from(2));
        fsm.free(0, 22);
        fsm.open_zone(2, 204, 296, 77, TxgT::from(2)).unwrap();
        fsm.free(2, 33);

        let dbs = fsm.serialize();
        let db = dbs.try().unwrap();
        assert_eq!(&EXPECTED[..], &db[..]);
    }

    #[test]
    fn try_allocate() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert!(fsm.open_zone(zid, 0, 1000, 0, txg).unwrap().is_none());
        let (res, full_zones) = fsm.try_allocate(64);
        assert_eq!(res, Some((zid, 0)));
        assert!(full_zones.is_empty());
        assert_eq!(fsm.open_zones[&zid].write_pointer(), 64);
        assert_eq!(fsm.in_use(zid), 64);
    }

    #[test]
    fn try_allocate_enospc() {
        let zid: ZoneT = 0;
        let txg = TxgT::from(0);
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(zid, 0, 1000, 0, txg).unwrap().is_none());
        assert!(fsm.try_allocate(2000).0.is_none());
    }

    #[test]
    fn try_allocate_from_zone_1() {
        let zid: ZoneT = 1;
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        // Pretend that zone 0 is too small for our allocation, but zone 1 isn't
        assert!(fsm.open_zone(0, 0, 10, 0, txg).unwrap().is_none());
        assert!(fsm.open_zone(zid, 10, 1000, 0, txg).unwrap().is_none());
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
        fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap().is_none();
        fsm.finish_zone(zid, TxgT::from(0));
        assert!(fsm.try_allocate(64).0.is_none());
    }

    #[test]
    fn try_allocate_only_empty_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.try_allocate(64).0.is_none());
    }
}
}
// LCOV_EXCL_STOP
