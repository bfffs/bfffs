// vim: tw=80

use crate::{
    label::*,
    raid::VdevRaidApi,
    types::*,
    util::*,
    vdev::BoxVdevFut
};
use divbuf::{DivBuf, DivBufShared};
#[cfg(test)] use crate::raid::MockVdevRaid;
use fixedbitset::FixedBitSet;
use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::FuturesUnordered
};
use metrohash::MetroHash64;
#[cfg(test)] use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::{
    cmp,
    collections::{BTreeMap, BTreeSet, btree_map::Keys},
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    ops::Range,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
        RwLock
    },
};

/// Minimal in-memory representation of a zone.
///
/// A Closed zone is one which contains data, and may not be written to again
/// until it has been garbage collected.  An Open zone is one which may contain
/// data, and may be written to.  A Dead zone is one which has been entirely
/// freed, but not yet erased.  An Empty zone contains nothing.
///
/// Zone life cycle
///
/// +-------+
/// | Empty |
/// +-------+
///     |
///     | Cluster::write() -> Cluster::open_zone() when a new zone is needed.
///     V
/// +-------+
/// | Open  |
/// +-------+
///     |
///     | Cluster::write() -> Cluster::close_zone() when the zone is full.
///     V
/// +--------+
/// | Closed |
/// +--------+
///     |
///     | Cluster::free() -> Cluster::kill_zone() when all data has been freed
///     V
/// +------+
/// | Dead |
/// +------+
///     |
///     | Cluster::reap()
///     V
/// +-------+
/// | Empty |
/// +-------+
#[derive(Clone, Debug)]
struct Zone {
    /// Number of LBAs that have been freed from this `Zone` since it was
    /// opened.
    pub freed_blocks: u32,
    /// Total number of LBAs in the `Zone`.  It may never change while the
    /// `Zone` is open or full.
    pub total_blocks: u32,
    /// The range of transactions that have been written to this Zone.  The
    /// start is inclusive, and the end is exclusive.
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
///
/// Common operations include:
/// * Choose an open zone to write X bytes, or open a new one
/// * Choose a zone to reclaim
/// * Find a zone by Zone ID, to rebuild it
/// * Find all zones modified in a certain txg range
#[derive(Debug)]
struct FreeSpaceMap {
    /// Which Blocks (not Zones) have been modified since the last
    /// Cluster::flush?
    dirty: FixedBitSet,

    /// Stores the set of dead zones.
    //dead_zones: BTreeSet<ZoneT>,

    /// Stores the set of empty or dead zones with id less than zones.len().
    /// All zones with id greater than or equal to zones.len() are implicitly
    /// empty.  A zone here listed is dead if its end txg > 0, empty otherwise.
    empty_zones: BTreeSet<ZoneT>,

    /// Currently open zones
    open_zones: BTreeMap<ZoneT, OpenZone>,

    /// Total number of zones in the vdev
    total_zones: ZoneT,

    /// `Vec` of all zones in the Vdev.  Any zones past the end of the `Vec` are
    /// implicitly Empty.  Any zones whose index is present in `empty_zones` and
    /// whose end txg is nonzero are implicitly dead.  Any zones whose index is
    /// present in `empty_zones` but whose end txg is zero are also Empty.  Any
    /// zones whose index is also present in `open_zones` are implicitly open.
    /// All other zones are Closed.
    zones: Vec<Zone>,
}

impl<'a> FreeSpaceMap {
    /// How many blocks have been allocated from this Zone, including blocks
    /// that have been freed but not erased?
    fn allocated(&self, zid: ZoneT) -> LbaT {
        if let Some(oz) = self.open_zones.get(&zid) {
            LbaT::from(oz.allocated_blocks)
        } else if self.empty_zones.contains(&zid) {
            0
        } else {
            LbaT::from(self.zones[zid as usize].total_blocks)
        }
    }

    /// How many blocks have been allocated from all zones, including blocks
    /// that have been freed but not erased?
    fn allocated_total(&self) -> LbaT {
        (0..self.zones.len() as ZoneT).map(|idx| {
            self.allocated(idx)
        }).sum()
    }

    /// Assert that the given zone was clean as of the given transaction
    fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT) {
        assert!(self.is_dead(zone) || self.is_empty(zone) ||
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

    /// Mark all zones as clean.  Call this method after writing the
    /// FreeSpaceMap to disk.
    fn clear_dirty_zones(&mut self) {
        self.dirty.clear();
    }

    fn deserialize(vdev: Arc<dyn VdevRaidApi>, buf: DivBuf, zones: ZoneT)
        -> Pin<Box<
                dyn Future<Output=Result<(Self, Arc<dyn VdevRaidApi>)>>
                + Send
            >>
    {
        let mut fsm = FreeSpaceMap::new(zones);
        let oz_futs = FuturesUnordered::new();
        let mut zid: ZoneT = 0;
        for (i, db) in buf.into_chunks(BYTES_PER_LBA).enumerate() {
            let sod = SpacemapOnDisk::deserialize(i as u64, &db).unwrap();
            if let Err(e) = sod {
                let fut = future::err(e);
                return Box::pin(fut);
            }
            for zod in sod.unwrap().zones.into_iter() {
                if zod.txgs.end > TxgT::from(0) {
                    let zl = vdev.zone_limits(zid);
                    fsm.open_zone(zid, zl.0, zl.1, 0, zod.txgs.start).unwrap();
                    if zod.allocated_blocks == u32::max_value() {
                        // Zone is closed
                        fsm.finish_zone(zid, zod.txgs.end - 1);
                    } else if zod.allocated_blocks > 0 {
                        // Zone is Open
                        let allocated = LbaT::from(zod.allocated_blocks);
                        oz_futs.push(vdev.reopen_zone(zid, allocated));
                        let azid = fsm.try_allocate(allocated).0.unwrap().0;
                        assert_eq!(azid, zid);
                    } else {
                        // Zone is dead
                        fsm.finish_zone(zid, zod.txgs.end - 1);
                        fsm.kill_zone(zid);
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
        fsm.clear_dirty_zones();
        let fut = oz_futs.try_collect::<Vec<_>>().map(|_| Ok((fsm, vdev)));
        Box::pin(fut)
    }

    fn dirty_zone_priv(dirty: &mut FixedBitSet, zone_id: ZoneT) {
        let block = zone_id as usize / SPACEMAP_ZONES_PER_LBA;
        dirty.insert(block);
    }

    /// Mark zone `zone_id` as dirty
    fn dirty_zone(&mut self, zone_id: ZoneT) {
        Self::dirty_zone_priv(&mut self.dirty, zone_id);
    }

    /// Kill this zone and mark it for erasing during the next reap operation.
    /// Return the number of its previously allocated blocks.
    fn kill_zone(&mut self, zone_id: ZoneT) -> LbaT {
        self.dirty_zone(zone_id);
        let zone_idx = zone_id as usize;
        assert!(!self.is_open(zone_id),
            "Can't kill an open zone");
        assert!(zone_idx < self.zones.len(),
            "Can't kill an empty zone");
        let allocated_blocks = self.allocated(zone_id);
        self.empty_zones.insert(zone_id);
        // TODO: set txgs.end to one past the current txg, in order to support
        // importing pools at a variable transaction level, like ZFS's
        // "zpool import -F"
        self.zones[zone_idx].txgs = TxgT::from(0)..TxgT::from(1);
        allocated_blocks
    }

    /// Find the first Empty zone
    fn find_empty(&self) -> Option<ZoneT> {
        self.empty_zones.iter()
            .find(|zid| self.zones[**zid as usize].txgs.end == TxgT::from(0))
            .cloned()
            .or({
                if (self.zones.len() as ZoneT) < self.total_zones {
                    Some(self.zones.len() as ZoneT)
                } else {
                    None
                }
            })
    }

    /// Mark the Zone as closed.  `txg` is the current transaction group.
    /// Return the number of wasted blocks, those that can no longer be
    /// used because the zone is closed.
    fn finish_zone(&mut self, zone_id: ZoneT, txg: TxgT) -> LbaT {
        self.dirty_zone(zone_id);
        let available = self.available(zone_id);
        assert!(self.open_zones.remove(&zone_id).is_some(),
            "Can't finish a Zone that isn't open");
        self.zones[zone_id as usize].freed_blocks += available as u32;
        self.zones[zone_id as usize].txgs.end = txg + 1;
        available
    }

    /// Record `length` LBAs in zone `zone_id` as freed.
    fn free(&mut self, zone_id: ZoneT, length: LbaT) {
        self.dirty_zone(zone_id);
        assert!(!self.is_empty(zone_id), "Can't free from an empty zone");
        let zone = self.zones.get_mut(zone_id as usize).expect(
            "Can't free from an empty zone");
        zone.freed_blocks += u32::try_from(length).expect(
            "Freeing multiple GB at a time?  Zones can't be that big...");
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
        if self.is_dead(zone_id) || self.is_empty(zone_id) {
            0
        } else if let Some(oz) = self.open_zones.get(&zone_id) {
            let z = &self.zones[zone_id as usize];
            LbaT::from(oz.allocated_blocks - z.freed_blocks)
        } else /* zone is closed */ {
            let z = &self.zones[zone_id as usize];
            LbaT::from(z.total_blocks - z.freed_blocks)
        }
    }

    /// How many blocks have been used in total, across all zones?
    fn in_use_total(&self) -> LbaT {
        (0..ZoneT::try_from(self.zones.len()).unwrap())
            .map(|i| self.in_use(i))
            .sum()
    }

    /// Is the Zone with the given id closed?
    fn is_closed(&self, zone_id: ZoneT) -> bool {
        zone_id < self.zones.len() as ZoneT &&
            ! self.empty_zones.contains(&zone_id) &&
            ! self.open_zones.contains_key(&zone_id)
    }

    /// Is the Zone with the given id dead?
    fn is_dead(&self, zone_id: ZoneT) -> bool {
        zone_id < self.zones.len() as ZoneT &&
            self.empty_zones.contains(&zone_id) &&
            self.zones[zone_id as usize].txgs.end > TxgT(0)
    }

    /// Is the Zone with the given id empty?
    fn is_empty(&self, zone_id: ZoneT) -> bool {
        zone_id >= self.zones.len() as ZoneT ||
            self.empty_zones.contains(&zone_id) &&
            self.zones[zone_id as usize].txgs.end == TxgT(0)
    }

    /// Is the Zone with the given id open?
    fn is_open(&self, zone_id: ZoneT) -> bool {
        self.open_zones.contains_key(&zone_id)
    }

    /// Find the next closed zone including or after `start`
    #[allow(clippy::if_same_then_else)]
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
            }).next()
    }

    fn new(total_zones: ZoneT) -> Self {
        let spacemap_blocks = spacemap_space(u64::from(total_zones));
        let mut dirty = FixedBitSet::with_capacity(spacemap_blocks as usize);
        // When newly created, all blocks are considered dirty.  This forces
        // them to be written out when formatting a new disk.
        dirty.insert_range(..);
        FreeSpaceMap{
            dirty,
            empty_zones: BTreeSet::new(),
            open_zones: BTreeMap::new(),
            total_zones,
            zones: Vec::new()
        }
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
                 txg: TxgT) -> Result<Option<(ZoneT, LbaT)>> {
        self.dirty_zone(id);
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
    async fn open(vdev: Arc<dyn VdevRaidApi>)
        -> Result<(Self, Arc<dyn VdevRaidApi + 'static>)>
    {
        let total_zones = vdev.zones();
        // NB: it would be slightly faster to created it with the correct
        // capacity and uninitialized.
        let blocks = div_roundup(total_zones as usize, SPACEMAP_ZONES_PER_LBA);
        let dbs = DivBufShared::from(vec![0u8; blocks * BYTES_PER_LBA]);
        let dbm = dbs.try_mut().unwrap();
        vdev.read_spacemap(dbm, 0)
        .and_then(move |_| {
            FreeSpaceMap::deserialize(vdev, dbs.try_const().unwrap(),
                                      total_zones)
        }).await
    }

    /// Return an iterator over the zone IDs of all open zones
    fn open_zone_ids(&self) -> Keys<ZoneT, OpenZone> {
        self.open_zones.keys()
    }

    /// Reap dead zones, returning them to an empty state.
    /// Returns the zone id of every reaped zone.
    fn reap(&mut self) -> impl IntoIterator<Item = ZoneT> {
        // Scan through all empty zones, erasing all.  Shrink self.zones, if
        // possible.
        let mut reaped = Vec::with_capacity(self.empty_zones.len());
        let mut highest_emptied = 0;
        for zid in self.empty_zones.iter().cloned() {
            reaped.push(zid);
            highest_emptied = highest_emptied.max(zid);
            Self::dirty_zone_priv(&mut self.dirty, zid);
            debug_assert_eq!(self.zones[zid as usize].txgs.start, TxgT::from(0));
            self.zones[zid as usize].txgs.end = TxgT::from(0);
        }
        if highest_emptied as usize + 1 == self.zones.len() {
            // Remove all trailing Empty zones from self.zones
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
        reaped
    }

    /// Serialize this `FreeSpaceMap` so it can be written to a disk's reserved
    /// area
    fn serialize(&'a self) -> impl Iterator<Item=(LbaT, DivBufShared)> + 'a {
        self.dirty.ones()
        .map(move |i| {
            let block = i as ZoneT;
            let szpl = SPACEMAP_ZONES_PER_LBA as ZoneT;
            let start = block * szpl;
            let end = cmp::min((block + 1) * szpl, self.total_zones);
            let v = (start..end).map(|z| {
                let allocated_blocks = if self.is_dead(z) || self.is_empty(z) {
                    0
                } else {
                    match self.open_zones.get(&z) {
                        Some(oz) => oz.allocated_blocks,
                        None => u32::max_value()
                    }
                };
                let freed_blocks = if self.is_dead(z) || self.is_empty(z) {
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
            let sod = SpacemapOnDisk::new(u64::from(block), v);
            let dbs = DivBufShared::from(bincode::serialize(&sod).unwrap());
            // TODO: if dbs isn't a multiple of a blocksize, and it's the last
            // block in the FSM, zero-extend it up to a full blocksize, so lower
            // levels won't have to copy the data.  Then remove the zero-padding
            // parts downstac,.
            (LbaT::from(block), dbs)
        })
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
        let mut nearly_full_zones = Vec::with_capacity(1);
        let result = {
            let zones = &self.zones;
            self.open_zones.iter_mut().find(|&(zone_id, ref oz)| {
                let zone = &zones[*zone_id as usize];
                let avail_lbas = zone.total_blocks - oz.allocated_blocks;
                // NB the next two lines can be replaced by
                // u32::try_from(space), once that feature is stabilized
                // https://github.com/rust-lang/rust/issues/33417
                assert!(space < LbaT::from(u32::max_value()));
                if avail_lbas < space as u32 {
                    nearly_full_zones.push(*zone_id);
                    false
                } else {
                    true
                }
            })
        }.map(|(zone_id, oz)| {
            let lba = oz.write_pointer();
            oz.allocated_blocks += space as u32;
            (*zone_id, lba)
        });
        if let Some((zid, _)) = result {
            self.dirty_zone(zid);
        }
        (result, nearly_full_zones)
    }

    /// Mark the next `space` LBAs in zone `zid` as wasted
    fn waste_space(&mut self, zid: ZoneT, space: LbaT) {
        self.dirty_zone(zid);
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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let t = self.total_zones;
        let le = self.empty_zones.len();
        let o = self.open_zones.len();
        let c = self.zones.len() - o - le;
        let e = (t as usize) - c - o;
        let max_txg: u32 = cmp::max(1, (0..self.zones.len())
            .filter(|zid| !self.is_empty(*zid as ZoneT))
            .map(|zid| {
                let z = &self.zones[zid];
                if self.is_open(zid as ZoneT) {
                    z.txgs.start
                } else {
                    cmp::max(z.txgs.start, z.txgs.end)
                }.into()
            }).max()
            .unwrap_or(0));

        // First print the header
        writeln!(f, "FreeSpaceMap: {t} Zones: {c} Closed, {e} Empty, {o} Open")?;
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
                format!("{x:>txg_width$x}")
            };
            let end = if self.is_closed(i as ZoneT) {
                let x: u32 = self.zones[i].txgs.end.into();
                format!("{x:>txg_width$x}")
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
            writeln!(f, "{i:>zone_width$x} | {this_row}|")?;
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
    /// then the zone is dead or empty.  If `u32::max_value()`, then the zone is
    /// closed.
    allocated_blocks: u32,

    /// Number of LBAs that have been freed from this `Zone` since it was
    /// opened.
    freed_blocks: u32,

    /// The range of transactions that have been written to this Zone.  The
    /// start is inclusive, and the end is exclusive
    ///
    /// The end is invalid for open zones, and both start and end are invalid
    /// for empty zones.  TODO: ensure that the end is valid for dead zones.
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
    fn deserialize(block: LbaT, buf: &DivBuf)
        -> bincode::Result<Result<Self>>
    {
        bincode::deserialize::<SpacemapOnDisk>(&buf[..])
        .map(|sod| {
            let mut hasher = MetroHash64::new();
            hasher.write_u64(block);
            sod.zones.hash(&mut hasher);
            let expected = hasher.finish();
            if expected == sod.checksum {
                Ok(sod)
            } else {
                Err(Error::EINTEGRITY)
            }
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
    /// The total amount of allocated space in the `Cluster`, excluding
    /// space that has already been freed but not erased.  Redundant with
    /// detailed information in the `FreeSpaceMap`.
    allocated_space: AtomicU64,

    fsm: RwLock<FreeSpaceMap>,

    /// Underlying vdev (which may or may not use RAID)
    // The Arc is necessary in order for some methods to return futures with
    // 'static lifetimes
    vdev: Arc<dyn VdevRaidApi>
}

#[cfg_attr(test, automock)]
impl Cluster {
    /// How many blocks have been allocated, including blocks that have been
    /// freed but not erased?
    pub fn allocated(&self) -> LbaT {
        self.allocated_space.load(Ordering::Relaxed)
    }

    /// Assert that the given zone was clean as of the given transaction
    pub fn assert_clean_zone(&self, zone: ZoneT, txg: TxgT) {
        self.fsm.read().unwrap().assert_clean_zone(zone, txg)
    }

    /// Finish any zones that are too full for new allocations.
    ///
    /// This defines the policy of when to close nearly full zones.
    fn close_zones(&self, nearly_full_zones: &[ZoneT], txg: TxgT)
        -> FuturesUnordered<BoxVdevFut>
    {
        nearly_full_zones.iter().map(|&zone_id| {
            let blocks = self.fsm.write().unwrap().finish_zone(zone_id, txg);
            self.allocated_space.fetch_add(blocks, Ordering::Relaxed);
            let fut = self.vdev.finish_zone(zone_id);
            Box::pin(fut) as BoxVdevFut
        }).collect::<FuturesUnordered<BoxVdevFut>>()
    }

    /// Create a new `Cluster` from unused files or devices
    ///
    /// * `raids`:              Already labeled raid vdev
    pub fn create(vdev: Arc<dyn VdevRaidApi>) -> Self
    {
        let total_zones = vdev.zones();
        let fsm = FreeSpaceMap::new(total_zones);
        Cluster::new((fsm, vdev))
    }

    /// Dump the FreeSpaceMap in human-readable form, for debugging purposes
    #[doc(hidden)]
    pub fn dump_fsm(&self) -> String {
        format!("{}", self.fsm.read().unwrap())
    }

    /// Delete the underlying storage for a Zone.
    fn kill_zone(&self, zone: ZoneT)
    {
        let blocks = self.fsm.write().unwrap().kill_zone(zone);
        self.allocated_space.fetch_sub(blocks, Ordering::Relaxed);
    }

    /// Find the first closed zone whose index is greater than or equal to `zid`
    pub fn find_closed_zone(&self, zid: ZoneT) -> Option<ClosedZone> {
        self.fsm.read().unwrap().find_closed_zone(zid)
            .map(|mut zone| {
                zone.start = self.vdev.zone_limits(zone.zid).0;
                zone
            })
    }

    /// Flush all data and metadata to disk, but don't sync yet.  This should
    /// normally be called just before [`sync_all`](#method.sync_all).  `idx` is
    /// the index of the label that is about to be written.
    pub fn flush(&self, idx: u32) -> BoxVdevFut
    {
        let mut fsm = self.fsm.write().unwrap();
        let vdev2 = self.vdev.clone();
        let zone_ids = fsm.open_zone_ids().cloned().collect::<Vec<_>>();
        let mut futs = zone_ids.iter().map(|&zone_id| {
            let (gap, fut) = self.vdev.flush_zone(zone_id);
            fsm.waste_space(zone_id, gap);
            self.allocated_space.fetch_add(gap, Ordering::Relaxed);
            fut
        }).collect::<FuturesUnordered<BoxVdevFut>>();
        // Since FreeSpaceMap::waste_space is synchronous, we can serialize the
        // FSM here; we don't need to copy it into a Future's continuation.
        let sm_futs = fsm.serialize()
        .map(|(block, dbs)| {
            let db = dbs.try_const().unwrap();
            // TODO: copy the last block's worth of buffer, rather than merely
            // pad it, so that vdev_block won't have to.  Better to do it here,
            // because vdev_raid duplicates the command for each disk.
            let sglist = if db.len() % BYTES_PER_LBA != 0 {
                // This can happen in the last block of the spacemap.  Pad out.
                let padlen = BYTES_PER_LBA - db.len() % BYTES_PER_LBA;
                let pad = ZERO_REGION.try_const().unwrap().slice_to(padlen);
                vec![db, pad]
            } else {
                vec![db]
            };
            vdev2.write_spacemap(sglist, idx, block)
        });
        futs.extend(sm_futs);
        let fut = futs.try_collect::<Vec<_>>()
        .map_ok(drop);
        fsm.clear_dirty_zones();
        drop(fsm);
        Box::pin(fut)
    }

    /// Erase all zones that are no longer needed.
    pub fn advance_transaction(&self, _txg: TxgT)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        let mut fsm = self.fsm.write().unwrap();
        fsm.reap()
            .into_iter()
            .map(|zid| self.vdev.erase_zone(zid))
            .collect::<FuturesUnordered<BoxVdevFut>>()
            .try_collect::<Vec<_>>()
            .map_ok(drop)
    }

    /// Mark `length` LBAs beginning at LBA `lba` as unused, and possibly delete
    /// them from the underlying storage.
    ///
    /// Deleting data in increments other than it was written is unsupported.
    /// In particular, it is not allowed to delete across zone boundaries.
    // TODO: return () instead of future
    pub fn free(&self, lba: LbaT, length: LbaT) -> BoxVdevFut
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
        let mut fsm = self.fsm.write().unwrap();
        fsm.free(start_zone, length);
        // Kill the zone if it is fully freed
        if fsm.is_closed(start_zone) && fsm.in_use(start_zone) == 0 {
            drop(fsm);
            self.kill_zone(start_zone)
        }
        Box::pin(future::ok(()))
    }

    /// Construct a new `Cluster` from an already constructed
    /// [`VdevRaidApi`](trait.VdevRaidApi.html)
    fn new(args: (FreeSpaceMap, Arc<dyn VdevRaidApi>)) -> Self {
        let (fsm, vdev) = args;
        let allocated_space = fsm.allocated_total().into();
        Cluster{allocated_space, fsm: RwLock::new(fsm), vdev}
    }

    /// Open a `Cluster` from an already opened
    /// [`VdevRaidApi`](trait.VdevRaidApi.html)
    ///
    /// Returns a new `Cluster` and a `LabelReader` that may be used to
    /// construct other vdevs stacked on top.
    pub async fn open(vdev_raid: Arc<dyn VdevRaidApi>) -> Result<Self>
    {
        FreeSpaceMap::open(vdev_raid).await
            .map(Cluster::new)
    }

    /// Returns the "best" number of operations to queue to this `Cluster`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    pub fn optimum_queue_depth(&self) -> u32 {
        self.vdev.optimum_queue_depth()
    }

    /// Asynchronously read from the cluster
    pub fn read(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut
    {
        self.vdev.clone().read_at(buf, lba)
    }

    /// Return approximately the usable space of the Cluster in LBAs.
    pub fn size(&self) -> LbaT {
        self.vdev.size()
    }

    /// Sync the `Cluster`, ensuring that all data written so far reaches stable
    /// storage.
    pub fn sync_all(&self) -> BoxVdevFut {
        self.vdev.sync_all()
    }

    /// How many blocks are currently in use?
    pub fn used(&self) -> LbaT {
        self.fsm.read().unwrap().in_use_total()
    }

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
        -> Result<(LbaT, BoxVdevFut)>
    {
        // Outline:
        // 1) Try allocating in an open zone
        // 2) If that doesn't work, try opening a new one, and allocating from
        //    that.
        // 3) If that doesn't work, return ENOSPC
        // 4) write to the vdev
        let space = div_roundup(buf.len(), BYTES_PER_LBA) as LbaT;
        let (alloc_result, nearly_full_zones) =
            self.fsm.write().unwrap().try_allocate(space);
        let futs = self.close_zones(&nearly_full_zones, txg);
        let vdev2: Arc<dyn VdevRaidApi> = self.vdev.clone();
        let vdev3 = self.vdev.clone();
        alloc_result.map(|(zone_id, lba)| {
            let oz_fut = Box::pin(future::ok(())) as BoxVdevFut;
            (zone_id, lba, oz_fut)
        }).or_else(|| {
            let empty_zone = self.fsm.read().unwrap().find_empty();
            empty_zone.and_then(move |zone_id| {
                let zl = vdev2.zone_limits(zone_id);
                let e = self.fsm.write().unwrap()
                    .open_zone(zone_id, zl.0, zl.1, space, txg);
                match e {
                    Ok(Some((zone_id, lba))) => {
                        let fut = Box::pin(vdev2.open_zone(zone_id)) as BoxVdevFut;
                        Some((zone_id, lba, fut))
                    },
                    Err(_) => None,
                    Ok(None) => panic!("Tried a 0-length write?"),
                }
            })
        }).map(|(zone_id, lba, oz_fut)| {
            self.allocated_space.fetch_add(space, Ordering::Relaxed);
            let wfut = vdev3.write_at(buf, zone_id, lba);
            let owfut = oz_fut.and_then(move |_| {
                wfut
            });
            futs.push(Box::pin(owfut));
            let fut = Box::pin(
                futs
                .try_collect::<Vec<_>>()
                .map_ok(drop)
            ) as BoxVdevFut;
            (lba, fut)
        }).ok_or(Error::ENOSPC)
    }

    /// Asynchronously write this cluster's label to all component devices
    /// All data and spacemap should be written and synced first!
    pub fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut
    {
        self.vdev.write_label(labeller)
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
        format!("{oz:?}");
    }
}

mod cluster {
    use super::super::*;
    use crate::vdev::*;
    use divbuf::DivBufShared;
    use itertools::Itertools;
    use mockall::{Sequence, predicate::*};
    use pretty_assertions::assert_eq;
    use std::iter;

    #[tokio::test]
    async fn free_and_erase_full_zone() {
        let mut vr = MockVdevRaid::default();
        vr.expect_lba2zone()
            .with(eq(1))
            .return_const(Some(0));
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 2));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((2, 200));
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(0), always())
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_finish_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_open_zone()
            .once()
            .with(eq(1))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(1), always())
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_erase_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let db1 = db0.clone();
        let (lba, fut1) = cluster.write(db0, TxgT::from(0))
            .expect("write failed early");
        fut1.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        // Write a 2nd time so the first zone will get closed
        let (_, fut1) = cluster.write(db1, TxgT::from(0))
            .expect("write failed early");
        fut1.await.unwrap();
        assert_eq!(cluster.allocated(), 2);
        cluster.free(lba, 1).await.unwrap();
        assert_eq!(cluster.allocated(), 1);

        cluster.advance_transaction(TxgT::from(0)).await.unwrap();
    }

    #[tokio::test]
    async fn free_and_erase_nonfull_zone() {
        let mut vr = MockVdevRaid::default();
        vr.expect_lba2zone()
            .with(eq(1))
            .return_const(Some(0));
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 3));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((3, 200));
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(0), always())
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_finish_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_open_zone()
            .once()
            .with(eq(1))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(1), always())
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_erase_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs0.try_const().unwrap();
        let db1 = dbs1.try_const().unwrap();
        let (lba, fut1) = cluster.write(db0, TxgT::from(0))
            .expect("write failed early");
        fut1.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        // Write a larger buffer so the first zone will get closed
        let (_, fut2) = cluster.write(db1, TxgT::from(0))
                .expect("write failed early");
        fut2.await.unwrap();
        assert_eq!(cluster.allocated(), 4);
        cluster.free(lba, 1).await.unwrap();
        assert_eq!(cluster.allocated(), 2);

        cluster.advance_transaction(TxgT::from(0)).await.unwrap();
    }

    #[tokio::test]
    async fn free_and_dont_erase_zone() {
        let mut vr = MockVdevRaid::default();
        vr.expect_lba2zone()
            .with(eq(1))
            .return_const(Some(0));
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 3));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((3, 200));
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(0), eq(1))
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(0), eq(2))
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));

        vr.expect_finish_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_open_zone()
            .once()
            .with(eq(1))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .with(always(), eq(1), always())
            .once()
            .return_once(|_, _, _| Box::pin(future::ok(())));

        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs0.try_const().unwrap();
        let db1 = dbs0.try_const().unwrap();
        let db2 = dbs1.try_const().unwrap();
        let (lba, fut1) = cluster.write(db0, TxgT::from(0))
            .expect("write failed early");
        fut1.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        let (_, fut2) = cluster.write(db1, TxgT::from(0))
            .expect("write failed early");
        fut2.await.unwrap();
        assert_eq!(cluster.allocated(), 2);
        // Write a larger buffer so the first zone will get closed
        let (_, fut3) = cluster.write(db2, TxgT::from(0))
            .expect("write failed early");
        fut3.await.unwrap();
        assert_eq!(cluster.allocated(), 4);
        cluster.free(lba, 1).await.unwrap();
        assert_eq!(cluster.allocated(), 4);

        cluster.advance_transaction(TxgT::from(0)).await.unwrap();
    }

    #[test]
    #[should_panic(expected = "Can't free across")]
    fn free_crosszone_padding() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_lba2zone()
            .with(eq(900))
            .return_const(Some(0));
        vr.expect_lba2zone()
            .with(eq(1099))
            .return_const(Some(1));
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 1000));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));
        drop(cluster.free(900, 200));
    }

    #[test]
    #[should_panic(expected = "Can't free from inter-zone padding")]
    fn free_interzone_padding() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_lba2zone()
            .with(eq(1000))
            .return_const(None);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((1, 1000));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));
        drop(cluster.free(1000, 10));
    }

    // FreeSpaceMap::open with the following conditions:
    // 0. A closed zone with no freed blocks
    // 1. A closed zone with some freed blocks
    // 2. An empty zone before the maximum open or full zone
    // 3. A dead zone before the maximum full or open zone
    // 4. An open zone with some freed blocks
    // 6. A trailing empty zone
    // TODO: add dead zones
    #[test]
    fn freespacemap_open() {
        // Serialized spacemap
        const SPACEMAP: [u8; 112] = [
            0x0b, 0xed, 0xf7, 0xb2, 0xb7, 0x81, 0x79, 0x37, // Checksum
            6, 0, 0, 0, 0, 0, 0, 0,         // 5 entries
            255, 255, 255, 255,             // zone0: allocated_blocks
            0, 0, 0, 0,                     // zone0: freed blocks
            0, 0, 0, 0, 2, 0, 0, 0,         // zone0 txgs: 0..2
            255, 255, 255, 255,             // zone1: allocated_blocks
            22, 0, 0, 0,                    // zone1: freed blocks
            1, 0, 0, 0, 3, 0, 0, 0,         // zone1 txgs: 1..3
            0, 0, 0, 0,                     // zone2: allocated_blocks
            0, 0, 0, 0,                     // zone2: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone2 txgs: 0..0
            0, 0, 0, 0,                     // zone3: allocated_blocks
            0, 0, 0, 0,                     // zone3: freed_blocks
            0, 0, 0, 0, 1, 0, 0, 0,         // zone3: txgs: 0..1
            77, 0, 0, 0,                    // zone4: allocated_blocks
            33, 0, 0, 0,                    // zone4: freed blocks
            2, 0, 0, 0, 255, 255, 255, 255, // zone4 txgs: 2..u32::MAX
            0, 0, 0, 0,                     // zone5: allocated_blocks
            0, 0, 0, 0,                     // zone5: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone5 txgs: 0..0
        ];
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(6u32);
        vr.expect_read_spacemap()
            .with(always(), eq(0))
            .once()
            .returning(|mut dbm, _idx| {
                 assert_eq!(dbm.len(), BYTES_PER_LBA);
                 dbm[0..112].copy_from_slice(&SPACEMAP[..]);
                 dbm[112..4096].iter_mut().set_from(iter::repeat(0));
                 Box::pin(future::ok(()))
            });

        vr.expect_reopen_zone()
            .once()
            .with(eq(4), eq(77))
            .return_once(|_, _| Box::pin(future::ok(())));
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((4, 96));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((104, 196));
        vr.expect_zone_limits()
            .with(eq(2))
            .return_const((204, 296));
        vr.expect_zone_limits()
            .with(eq(3))
            .return_const((304, 396));
        vr.expect_zone_limits()
            .with(eq(4))
            .return_const((404, 496));
        vr.expect_zone_limits()
            .with(eq(5))
            .return_const((504, 596));
        let (fsm, _mock_vr) = FreeSpaceMap::open(Arc::new(vr))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(fsm.zones.len(), 5);
        assert_eq!(fsm.zones[0].freed_blocks, 0);
        assert_eq!(fsm.zones[0].total_blocks, 92);
        assert_eq!(fsm.zones[0].txgs, TxgT::from(0)..TxgT::from(2));
        assert_eq!(fsm.zones[1].freed_blocks, 22);
        assert_eq!(fsm.zones[1].total_blocks, 92);
        assert_eq!(fsm.zones[1].txgs, TxgT::from(1)..TxgT::from(3));
        assert!(fsm.is_empty(2));
        assert!(fsm.is_dead(3));
        assert_eq!(fsm.zones[4].freed_blocks, 33);
        assert_eq!(fsm.zones[4].total_blocks, 92);
        assert_eq!(fsm.zones[4].txgs.start, TxgT::from(2));
        let oz = &fsm.open_zones[&4];
        assert_eq!(oz.start, 404);
        assert_eq!(oz.allocated_blocks, 77);
        assert!(fsm.is_empty(5));
        assert_eq!(0, fsm.dirty.count_ones(..));
    }

    // FreeSpaceMap::open with more zones that can fit into a single block
    #[test]
    fn freespacemap_open_300_zones() {
        // Serialized spacemap
        const SPACEMAP_B0: [u8; 32] = [
            0x5e, 0xe9, 0x96, 0x17, 0xc2, 0xfe, 0xa0, 0x8e, // Checksum
            255, 0, 0, 0, 0, 0, 0, 0,       // 255 entries
            255, 255, 255, 255,             // zone0: allocated_blocks
            0, 0, 0, 0,                     // zone0: freed blocks
            0, 0, 0, 0, 2, 0, 0, 0,         // zone0 txgs: 0..2
            // Rest is all 0, indicating empty zones
        ];
        const SPACEMAP_B1: [u8; 32] = [
            0x07, 0x5e, 0x95, 0xf0, 0x93, 0x3f, 0xa7, 0xb0, // Checksum
            45, 0, 0, 0, 0, 0, 0, 0,        // 45 entries
            255, 255, 255, 255,             // zone255: allocated_blocks
            1, 0, 0, 0,                     // zone255: freed blocks
            3, 0, 0, 0, 4, 0, 0, 0,         // zone255 txgs: 3..4
            // Rest is all 0, indicating empty zones
        ];
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(300u32);
        vr.expect_read_spacemap()
            .with(always(), eq(0))
            .once()
            .returning(|mut dbm, _idx| {
                assert_eq!(dbm.len(), 8192);
                dbm[0..32].copy_from_slice(&SPACEMAP_B0[..]);
                dbm[32..4096].iter_mut().set_from(iter::repeat(0));
                dbm[4096..4128].copy_from_slice(&SPACEMAP_B1[..]);
                dbm[4192..8192].iter_mut().set_from(iter::repeat(0));
                Box::pin(future::ok(()))
            });

        vr.expect_zone_limits()
             .returning(|zid: ZoneT| {
                 let i = LbaT::from(zid);
                 (100 * i + 4, 100 * i + 96)
             });

        let (fsm, _mock_vr) = FreeSpaceMap::open(Arc::new(vr))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(fsm.zones.len(), 256);
        assert_eq!(fsm.zones[0].freed_blocks, 0);
        assert_eq!(fsm.zones[0].total_blocks, 92);
        assert_eq!(fsm.zones[0].txgs, TxgT::from(0)..TxgT::from(2));
        assert!(fsm.is_empty(1));
        assert!(fsm.is_empty(254));
        assert_eq!(fsm.zones[255].freed_blocks, 1);
        assert_eq!(fsm.zones[255].total_blocks, 92);
        assert_eq!(fsm.zones[255].txgs, TxgT::from(3)..TxgT::from(4));
        assert!(fsm.is_empty(256));
        assert!(fsm.is_empty(299));
    }

    #[test]
    fn freespacemap_open_ecksum() {
        // Serialized spacemap
        const SPACEMAP: [u8; 16] = [
            0, 0, 0, 0, 0, 0, 0, 0,         // Checksum
            0, 0, 0, 0, 0, 0, 0, 0,         // 0 entries
        ];
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(0u32);
        vr.expect_read_spacemap()
            .with(always(), eq(0))
            .once()
            .returning(|mut dbm, _idx| {
                dbm.try_truncate(0).unwrap();
                dbm.extend(SPACEMAP.iter());
                Box::pin(future::ok(()))
            });

        let r = FreeSpaceMap::open(Arc::new(vr)).now_or_never().unwrap();
        assert_eq!(Error::EINTEGRITY, r.err().unwrap());
    }

    #[test]
    fn find_closed_zone() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((1, 2));
        vr.expect_zone_limits()
            .with(eq(3))
            .return_const((3, 4));
        vr.expect_zone_limits()
            .with(eq(4))
            .return_const((4, 5));
        let mut fsm = FreeSpaceMap::new(10);
        fsm.open_zone(0, 0, 1, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        fsm.open_zone(2, 2, 3, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(3, 3, 4, 0, TxgT::from(1)).unwrap();
        fsm.finish_zone(3, TxgT::from(3));
        fsm.open_zone(4, 4, 5, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(4, TxgT::from(0));
        let cluster = Cluster::new((fsm, Arc::new(vr)));
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
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1000));
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 0
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let _ = cluster.write(db0, TxgT::from(0)).expect("write failed early");
    }

    // During transaction sync, Cluster::flush should flush all open VdevRaid
    // zones and write the spacemap.  Then Cluster.sync_all should sync_all the
    // VdevRaid
    #[tokio::test]
    async fn txg_sync() {
        let mut seq = Sequence::new();
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(100u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1000));
        vr.expect_open_zone()
            .once()
            .in_sequence(&mut seq)
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .once()
            .in_sequence(&mut seq)
            .withf(|buf, zone, lba|
                buf.len() == BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 0
            ).return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_flush_zone()
            .once()
            .in_sequence(&mut seq)
            .with(eq(0))
            .return_once(|_| (5, Box::pin(future::ok(()))));
        vr.expect_write_spacemap()
            .once()
            .in_sequence(&mut seq)
            .withf(|sglist, idx, block|
                sglist.iter().map(DivBuf::len).sum::<usize>() == 4096 &&
                *idx == 0 &&
                *block == 0
            ).return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_sync_all()
            .once()
            .in_sequence(&mut seq)
            .return_once(|| Box::pin(future::ok(())));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));
        cluster.fsm.write().unwrap().clear_dirty_zones();

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let (_, fut) = cluster.write(db0, TxgT::from(0))
            .expect("write failed early");
        fut.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        cluster.flush(0).await.unwrap();
        assert_eq!(cluster.allocated(), 6);
        cluster.sync_all().await.unwrap();
        let fsm = cluster.fsm.read().unwrap();
        assert_eq!(fsm.open_zones[&0].write_pointer(), 6);
        assert_eq!(fsm.zones[0].freed_blocks, 5);
        assert_eq!(0, fsm.dirty.count_ones(..));
    }

    #[test]
    fn write_zones_too_small() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let result = cluster.write(dbs.try_const().unwrap(), TxgT::from(0));
        assert_eq!(result.err().unwrap(), Error::ENOSPC);
        assert_eq!(cluster.allocated(), 0);
    }

    #[test]
    fn write_no_available_zones() {
        let mut vr = MockVdevRaid::default();
        // What a useless disk ...
        vr.expect_zones()
            .return_const(0u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 0));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let result = cluster.write(dbs.try_const().unwrap(), TxgT::from(0));
        assert_eq!(result.err().unwrap(), Error::ENOSPC);
        assert_eq!(cluster.allocated(), 0);
    }

    #[tokio::test]
    async fn write_with_no_open_zones() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1000));
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 0
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let (lba, fut) = cluster.write(db0, TxgT::from(0))
            .expect("write failed early");
        fut.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        assert_eq!(lba, 0);
    }

    #[tokio::test]
    async fn write_with_open_zones() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 1000));
        vr.expect_open_zone()
            .once()
            .with(eq(0))
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 0
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 1
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let db0 = dbs.try_const().unwrap();
        let db1 = dbs.try_const().unwrap();
        let cluster_ref = &cluster;
        let (_, fut0) = cluster.write(db0, TxgT::from(0))
            .expect("Cluster::write");
        fut0.await.unwrap();
        assert_eq!(cluster.allocated(), 1);
        let (lba1, fut1) = cluster_ref.write(db1, TxgT::from(0))
            .expect("Cluster::write");
        assert_eq!(lba1, 1);
        fut1.await.expect("write failed");
        assert_eq!(cluster.allocated(), 2);
    }

    // When one zone is too full to satisfy an allocation, it should be closed
    // and a new zone opened.
    #[tokio::test]
    async fn write_zone_full() {
        let mut vr = MockVdevRaid::default();
        vr.expect_zones()
            .return_const(32768u32);
        vr.expect_zone_limits()
            .with(eq(0))
            .return_const((0, 3));
        vr.expect_zone_limits()
            .with(eq(1))
            .return_const((3, 6));
        vr.expect_open_zone()
            .with(eq(0))
            .once()
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == 2 * BYTES_PER_LBA &&
                *zone == 0 &&
                *lba == 0
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        vr.expect_finish_zone()
            .with(eq(0))
            .once()
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_open_zone()
            .with(eq(1))
            .once()
            .return_once(|_| Box::pin(future::ok(())));
        vr.expect_write_at()
            .withf(|buf, zone, lba|
                buf.len() == 2 * BYTES_PER_LBA &&
                *zone == 1 &&
                *lba == 3
            ).once()
            .return_once(|_, _, _| Box::pin(future::ok(())));
        let fsm = FreeSpaceMap::new(vr.zones());
        let cluster = Cluster::new((fsm, Arc::new(vr)));

        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let db0 = dbs.try_const().unwrap();
        let db1 = dbs.try_const().unwrap();
        let cluster_ref = &cluster;
        let (_, fut0) = cluster.write(db0, TxgT::from(0))
            .expect("Cluster::write");
        fut0.await.unwrap();
        assert_eq!(cluster.allocated(), 2);
        let (lba1, fut1) = cluster_ref.write(db1, TxgT::from(0))
            .expect("Cluster::write");
        assert_eq!(lba1, 3);
        fut1.await.expect("write failed");
        assert_eq!(cluster.allocated(), 5);
    }
}

mod free_space_map {
    use pretty_assertions::assert_eq;
    use super::super::*;

    // pet kcov
    #[test]
    fn debug() {
        let fsm = FreeSpaceMap::new(10);
        format!("{fsm:?}");
    }

    #[test]
    fn allocated_total_all_empty() {
        let fsm = FreeSpaceMap::new(32768);
        assert_eq!(0, fsm.allocated_total());
    }

    #[test]
    fn allocated_total_one_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(0, TxgT::from(0));
        assert_eq!(1000, fsm.allocated_total());
    }

    #[test]
    fn allocated_total_one_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 200, TxgT::from(0)).unwrap();
        assert_eq!(200, fsm.allocated_total());
    }

    #[test]
    fn allocated_total_one_empty_two_closed_two_open_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 1000, 2000, 200, TxgT::from(0)).unwrap();
        // Leave zone 1 empty
        fsm.open_zone(2, 3000, 4000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.open_zone(3, 4000, 5000, 500, TxgT::from(0)).unwrap();
        fsm.open_zone(4, 5000, 7000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(4, TxgT::from(0));
        assert_eq!(3700, fsm.allocated_total());
    }

    //#[test]
    //fn deserialize() {
        //let b: Vec<u8> = vec![
            //18, 213, 216, 8, 231, 94, 198, 193, // Checksum
            //4, 0, 0, 0, 0, 0, 0, 0,         // 6 ZODs
            //255, 255, 255, 255,             // zone0: allocated_blocks
            //26, 0, 0, 0,                    // zone0: freed blocks
            //1, 0, 0, 0, 3, 0, 0, 0,         // zone0 txgs: 1..3
            //0, 0, 0, 0,                     // zone1: allocated_blocks
            //0, 0, 0, 0,                     // zone1: freed blocks
            //0, 0, 0, 0, 0, 0, 0, 0,         // zone1 txgs: DON'T CARE..0
            //0, 0, 0, 0,                     // zone2: allocated blocks
            //0, 0, 0, 0,                     // zone2: freed blocks
            //0, 0, 0, 0, 3, 0, 0, 0,         // zone2: txgs: DON'T CARE..3
            //77, 0, 0, 0,                    // zone3: allocated_blocks
            //33, 0, 0, 0,                    // zone3: freed blocks
            //2, 0, 0, 0, 255, 255, 255, 255, // zone3 txgs: 2..DON'T CARE
            //0, 0, 0, 0,                     // zone4: allocated_blocks
            //0, 0, 0, 0,                     // zone4: freed blocks
            //0, 0, 0, 0, 0, 0, 0, 0,         // zone4 txgs: DON'T CARE..0
            //0, 0, 0, 0,                     // zone5: allocated_blocks
            //0, 0, 0, 0,                     // zone5: freed blocks
            //0, 0, 0, 0, 3, 0, 0, 0,         // zone5 txgs: DON'T CARE..3
        //];

        //let dbs = DivBufShared::from(b);
        //let db = dbs.try_const().unwrap();
        //let sod = SpacemapOnDisk::deserialize(0, &db).unwrap();
        //assert!(sm.is_closed(0));
        //assert_eq!(sm.allocated(0), 42);    //TODO
        //assert!(sm.is_empty(1));
        //assert!(sm.is_dead(2));
        //assert!(sm.is_open(3));
        //assert!(sm.is_dead(4));
        //assert!(sm.is_empty(5));
    //}

    #[test]
    fn dirty() {
        let mut fsm = FreeSpaceMap::new(4096);
        // A freshly created FreeSpaceMap should be all dirty
        assert_eq!(&[0b1_1111_1111_1111_1111], fsm.dirty.as_slice());

        // clear_dirty_zones should clear it
        fsm.clear_dirty_zones();
        assert_eq!(0, fsm.dirty.count_ones(..));

        // open_zone should dirty a zone
        fsm.open_zone(0, 100, 200, 20, TxgT::from(0)).unwrap();
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // Allocating should dirty a zone, too
        fsm.try_allocate(64);
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // Wasting space should dirty a zone, too
        fsm.clear_dirty_zones();
        fsm.waste_space(0, 10);
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // Finishing a zone should also dirty it
        fsm.clear_dirty_zones();
        fsm.finish_zone(0, TxgT::from(0));
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // As should freeing
        fsm.clear_dirty_zones();
        fsm.free(0, 10);
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // And killing.
        fsm.clear_dirty_zones();
        fsm.kill_zone(0);
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // Finally, so should reaping a zone
        fsm.clear_dirty_zones();
        fsm.reap();
        assert_eq!(&[0b1], fsm.dirty.as_slice());

        // The dirty bitmap should also work for zones in other spacemap blocks
        fsm.open_zone(512, 51200, 51300, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(2048, 204_000, 204_900, 0, TxgT::from(0)).unwrap();
        assert_eq!(&[0b1_0000_0101], fsm.dirty.as_slice());
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
        assert_eq!(expected, format!("{fsm}"));
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
        assert_eq!(expected, format!("{fsm}"));
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
        assert_eq!(expected, format!("{fsm}"));
    }

    #[test]
    fn kill_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.kill_zone(0);
        assert!(fsm.is_dead(0));
        assert!(!fsm.is_empty(0));
        assert!(!fsm.is_dead(1));
        assert!(!fsm.is_empty(1));
    }

    #[test]
    #[should_panic(expected = "Can't kill an empty zone")]
    fn kill_empty_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.kill_zone(0);
    }

    #[test]
    fn kill_last_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(1, 1000, 2000, 1, TxgT::from(0)).unwrap();
        fsm.finish_zone(1, TxgT::from(0));
        fsm.kill_zone(1);
        assert!(!fsm.is_dead(0));
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.in_use(1), 0);
        assert_eq!(fsm.zones.len(), 2);
        assert!(fsm.is_dead(1));
        assert!(!fsm.is_empty(1));
    }

    #[test]
    fn kill_last_zone_with_empties_behind_it() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.kill_zone(2);
        assert!(!fsm.is_empty(0));
        assert!(fsm.is_empty(1));
        assert!(fsm.is_dead(2));
        assert_eq!(fsm.zones.len(), 3);
    }

    #[test]
    fn kill_last_zone_with_all_other_zones_empty() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.kill_zone(2);
        assert!(fsm.is_empty(0));
        assert!(fsm.is_empty(1));
        assert!(fsm.is_dead(2));
        assert_eq!(fsm.zones.len(), 3);
    }

    #[test]
    #[should_panic(expected = "Can't kill an open zone")]
    fn kill_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.kill_zone(0);
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
        assert!(fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none());
        assert!(
            fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap().is_none());
        fsm.finish_zone(1, TxgT::from(0));
        assert_eq!(fsm.find_empty(), None);
    }

    /// FreeSpaceMap::find_empty should skip over dead zones
    #[test]
    fn find_empty_dead_at_end() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(0, 0, 10, 0, TxgT::from(0)).unwrap().is_none());
        fsm.finish_zone(0, TxgT::from(0));
        fsm.kill_zone(0);
        assert_eq!(fsm.find_empty(), Some(1));
    }

    /// FreeSpaceMap::find_empty should skip over dead zones
    #[test]
    fn find_empty_dead_before_end() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(0, 0, 10, 0, TxgT::from(0)).unwrap().is_none());
        fsm.finish_zone(0, TxgT::from(0));
        fsm.kill_zone(0);
        assert!(fsm.open_zone(1, 10, 20, 0, TxgT::from(0)).unwrap().is_none());
        assert_eq!(fsm.find_empty(), Some(2));
    }

    #[test]
    fn find_empty_explicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none());
        assert!(
            fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap().is_none());
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn find_empty_implicit() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap().is_none());
        assert_eq!(fsm.find_empty(), Some(1));
    }

    #[test]
    fn finish() {
        let zid: ZoneT = 0;
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(
            fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap().is_none());
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

    #[test]
    fn reap_closed_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(1, TxgT::from(0));
        fsm.reap();     // Should do nothing
        assert_eq!(fsm.zones.len(), 2);
        assert!(fsm.is_closed(1));
    }

    #[test]
    fn reap_dead_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.kill_zone(0);
        fsm.reap();
        assert!(!fsm.is_dead(0));
        assert!(fsm.is_empty(0));
        assert!(!fsm.is_dead(1));
        assert!(!fsm.is_empty(1));
    }

    #[test]
    fn reap_last_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(1, 1000, 2000, 1, TxgT::from(0)).unwrap();
        fsm.finish_zone(1, TxgT::from(0));
        fsm.kill_zone(1);
        fsm.reap();
        assert_eq!(fsm.zones.len(), 1);
        assert!(!fsm.is_dead(0));
        assert!(!fsm.is_empty(0));
    }

    #[test]
    fn reap_last_zone_with_empties_behind_it() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(0, 0, 1000, 0, TxgT::from(0)).unwrap();
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.kill_zone(2);
        fsm.reap();
        assert!(!fsm.is_empty(0));
        assert_eq!(fsm.zones.len(), 1);
    }

    #[test]
    fn reap_last_zone_with_all_other_zones_empty() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(2, 2000, 3000, 0, TxgT::from(0)).unwrap();
        fsm.finish_zone(2, TxgT::from(0));
        fsm.kill_zone(2);
        fsm.reap();
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    fn reap_empty_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.reap();     // Should do nothing
        assert_eq!(fsm.zones.len(), 0);
    }

    #[test]
    fn reap_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        fsm.open_zone(1, 1000, 2000, 0, TxgT::from(0)).unwrap();
        fsm.reap();     // Should do nothing
        assert_eq!(fsm.zones.len(), 2);
        assert!(fsm.is_open(1));
    }

    // FreeSpaceMap::serialize with the following conditions:
    // 0. A full zone with some freed blocks
    // 1. An empty zone before the maximum open or full zone
    // 2. A dead zone before the maximum open or full zone
    // 3. An open zone with some freed blocks
    // 4. A trailing dead zone
    // 5. A trailing empty zone
    #[test]
    fn serialize() {
        const EXPECTED: [u8; 112] = [
            61, 213, 211, 6, 223, 126, 49, 158, // Checksum
            6, 0, 0, 0, 0, 0, 0, 0,         // 6 ZODs
            255, 255, 255, 255,             // zone0: allocated_blocks
            26, 0, 0, 0,                    // zone0: freed blocks
            1, 0, 0, 0, 3, 0, 0, 0,         // zone0 txgs: 1..3
            0, 0, 0, 0,                     // zone1: allocated_blocks
            0, 0, 0, 0,                     // zone1: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone1 txgs: DON'T CARE..0
            0, 0, 0, 0,                     // zone2: allocated blocks
            0, 0, 0, 0,                     // zone2: freed blocks
            0, 0, 0, 0, 1, 0, 0, 0,         // zone2: txgs: DON'T CARE..1
            77, 0, 0, 0,                    // zone3: allocated_blocks
            33, 0, 0, 0,                    // zone3: freed blocks
            2, 0, 0, 0, 255, 255, 255, 255, // zone3 txgs: 2..DON'T CARE
            0, 0, 0, 0,                     // zone4: allocated_blocks
            0, 0, 0, 0,                     // zone4: freed blocks
            0, 0, 0, 0, 1, 0, 0, 0,         // zone4 txgs: DON'T CARE..1
            0, 0, 0, 0,                     // zone5: allocated_blocks
            0, 0, 0, 0,                     // zone5: freed blocks
            0, 0, 0, 0, 0, 0, 0, 0,         // zone5 txgs: DON'T CARE..0
        ];
        let mut fsm = FreeSpaceMap::new(6);
        fsm.open_zone(0, 4, 96, 88, TxgT::from(1)).unwrap();
        fsm.finish_zone(0, TxgT::from(2));
        fsm.free(0, 22);
        fsm.open_zone(3, 204, 296, 77, TxgT::from(2)).unwrap();
        fsm.free(3, 33);
        fsm.open_zone(5, 388, 480, 92, TxgT::from(2)).unwrap();
        fsm.finish_zone(5, TxgT::from(2));
        fsm.kill_zone(5);
        fsm.reap();
        fsm.open_zone(4, 296, 388, 0, TxgT::from(3)).unwrap();
        fsm.finish_zone(4, TxgT::from(3));
        fsm.kill_zone(4);
        fsm.kill_zone(2);

        let mut fsm_iter = fsm.serialize();
        let (block, dbs) = fsm_iter.next().unwrap();
        assert_eq!(0, block);
        let db = dbs.try_const().unwrap();
        assert_eq!(&EXPECTED[..], &db[..112]);
        assert!(&db[112..].iter().all(|&x| x == 0));
        assert!(fsm_iter.next().is_none());
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
        assert!(
            fsm.open_zone(zid, 0, 1000, 0, TxgT::from(0)).unwrap().is_none());
        fsm.finish_zone(zid, TxgT::from(0));
        assert!(fsm.try_allocate(64).0.is_none());
    }

    #[test]
    fn try_allocate_only_empty_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        assert!(fsm.try_allocate(64).0.is_none());
    }

    #[test]
    fn in_use_total_empty() {
        let fsm = FreeSpaceMap::new(32768);
        assert_eq!(fsm.in_use_total(), 0);
    }

    #[test]
    fn in_use_total_one_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(0, 0, 1000, 10, txg).unwrap(), Some((0, 0)));
        assert_eq!(fsm.in_use_total(), 10);
    }

    #[test]
    fn in_use_total_two_open_zones() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(0, 0, 20, 15, txg).unwrap(), Some((0, 0)));
        assert_eq!(fsm.open_zone(1, 20, 40, 15, txg).unwrap(), Some((1, 20)));
        assert_eq!(fsm.in_use_total(), 30);
    }

    #[test]
    fn in_use_total_one_closed_and_one_empty_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(0, 0, 20, 5, txg).unwrap(), Some((0, 0)));
        fsm.finish_zone(0, txg);
        assert_eq!(fsm.open_zone(1, 20, 40, 5, txg).unwrap(), Some((1, 20)));
        fsm.kill_zone(0);
        assert_eq!(fsm.in_use_total(), 5);
    }

    #[test]
    fn in_use_total_one_closed_and_one_open_zone() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(0, 0, 20, 5, txg).unwrap(), Some((0, 0)));
        fsm.finish_zone(0, txg);
        assert_eq!(fsm.open_zone(1, 20, 40, 5, txg).unwrap(), Some((1, 20)));
        assert_eq!(fsm.in_use_total(), 10);
    }

    #[test]
    fn in_use_total_one_open_zone_with_freed_lbas() {
        let mut fsm = FreeSpaceMap::new(32768);
        let txg = TxgT::from(0);
        assert_eq!(fsm.open_zone(0, 0, 20, 5, txg).unwrap(), Some((0, 0)));
        assert_eq!(fsm.try_allocate(6), (Some((0, 5)), vec![]));
        fsm.free(0, 5);
        assert_eq!(fsm.in_use_total(), 6);
    }
}
}
// LCOV_EXCL_STOP
