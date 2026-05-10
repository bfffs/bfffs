// vim: tw=80
//! BFFFS Mirror layer
//!
//! This provides vdevs which slot between `raid` and `VdevBlock` and
//! provide mirror functionality.  That includes both permanent mirrors as well
//! as temporary mirrors, used for spares and replacements.

use std::{
    collections::BTreeMap,
    io,
    mem,
    num::{NonZeroU8, NonZeroU64},
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering}
    }
};

use divbuf::{DivBufInaccessible, DivBufMut, DivBufShared};
use futures::{
    Future,
    FutureExt,
    SinkExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    channel::mpsc,
    stream::FuturesUnordered,
    task::{Context, Poll}
};
use nonzero_ext::nonzero;
use pin_project::pin_project;
use speedy::{Readable, Writable};

use crate::{
    label::*,
    types::*,
    vdev::*,
    vdev_block,
    BYTES_PER_LBA
};

cfg_select! {
    test => {
        use mockall::mock;
        use BoxVdevFut as VdevBlockFut;
        use vdev_block::MockVdevBlock as VdevBlock;
    }
    _ => {
        use vdev_block::VdevBlockFut;
        use vdev_block::VdevBlock;
    }
}


#[derive(Debug, Readable, Writable)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:           Uuid,
    pub children:       Vec<Uuid>
}

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager {
    vbm: crate::vdev_block::Manager,
    mirrors: BTreeMap<Uuid, Label>,
}

impl Manager {
    /// Import a mirror that is already known to exist
    #[cfg(not(test))]
    #[auto_enums::auto_enum(Future)]
    pub fn import(&mut self, uuid: Uuid)
        -> impl Future<Output=Result<(Mirror, LabelReader)>>
    {
        match self.mirrors.remove(&uuid) {
            None => futures::future::err(Error::ENOENT),
            Some(ml) => {
                ml.children.into_iter()
                    .map(move |child_uuid| self.vbm.import(child_uuid))
                    .collect::<FuturesUnordered<_>>()
                    .collect::<Vec<_>>()
                    .map(move |v| {
                        let mut pairs = Vec::with_capacity(v.len());
                        let mut error = Error::ENOENT;
                        for r in v.into_iter() {
                            match r {
                                Ok(pair) => pairs.push(pair),
                                Err(e) => {
                                    error = e;
                                }
                            }
                        };
                        if !pairs.is_empty() {
                            Ok(Mirror::open(Some(uuid), pairs))
                        } else {
                            Err(error)
                        }
                    })
            }
        }
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `Manager` for use as a spare or
    /// for building Pools.
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<LabelReader> {
        let mut reader = self.vbm.taste(p).await?;
        let ml: Label = reader.deserialize().unwrap();
        self.mirrors.insert(ml.uuid, ml);
        Ok(reader)
    }
}

/// Return value of [`Mirror::status`]
#[derive(Clone, Debug)]
pub struct Status {
    pub health: Health,
    pub leaves: Vec<vdev_block::Status>,
    /// The oldest transaction number that is synced to rebuilding children.
    pub rebuilding: Option<TxgT>,
    pub uuid: Uuid
}

impl Status {
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

/// A child of a Mirror.  Probably either a VdevBlock or a missing disk
enum Child {
    Present(Arc<VdevBlock>),
    Missing(Uuid),
    Rebuilding(Arc<VdevBlock>),
    /// Administratively faulted
    Faulted(Arc<VdevBlock>),
}

impl Child {
    #[cfg(test)]
    fn faulted(vdev: VdevBlock) -> Self {
        Child::Faulted(Arc::new(vdev))
    }

    fn present(vdev: VdevBlock) -> Self {
        Child::Present(Arc::new(vdev))
    }

    fn rebuilding(vdev: VdevBlock) -> Self {
        Child::Rebuilding(Arc::new(vdev))
    }

    fn missing(uuid: Uuid) -> Self {
        Child::Missing(uuid)
    }

    fn as_present(&self) -> Option<&Arc<VdevBlock>> {
        if let Child::Present(vb) = self {
            Some(vb)
        } else {
            None
        }
    }

    fn as_rebuilding(&self) -> Option<&Arc<VdevBlock>> {
        if let Child::Rebuilding(vb) = self {
            Some(vb)
        } else {
            None
        }
    }

    fn erase_zone(&self, start: LbaT, end: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.erase_zone(start, end))
    }

    fn fault(&mut self) {
        let old = mem::replace(self, Child::Missing(Uuid::default()));
        *self = match old {
            Child::Present(vb) => Child::Faulted(vb),
            Child::Faulted(vb) => Child::Faulted(vb),
            Child::Rebuilding(vb) => Child::Faulted(vb),
            Child::Missing(uuid) => Child::Missing(uuid),
        };
    }

    fn finish_zone(&self, start: LbaT, end: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.finish_zone(start, end))
    }

    fn is_present(&self) -> bool {
        matches!(self, Child::Present(_))
    }

    fn is_rebuilding(&self) -> bool {
        matches!(self, Child::Rebuilding(_))
    }

    fn open_zone(&self, start: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.open_zone(start))
    }

    /// Place a faulted child back into service and begin to rebuild it
    fn rebuild(&mut self) {
        let old = mem::replace(self, Child::Missing(Uuid::default()));
        *self = match old {
            Child::Present(vb) => Child::Present(vb),
            Child::Faulted(vb) => Child::Rebuilding(vb),
            Child::Rebuilding(vb) => Child::Rebuilding(vb),
            Child::Missing(uuid) => Child::Missing(uuid),
        };
    }

    /// Mark a fully rebuilt child as healthy again.
    ///
    /// It is the caller's responsibility to ensure that every zone in the
    /// Mirror has been rebuilt onto this device.
    fn restore(&mut self) {
        let old = mem::replace(self, Child::Missing(Uuid::default()));
        *self = match old {
            Child::Present(vb) => Child::Present(vb),
            Child::Faulted(_) => panic!("Must rebuild before restore"),
            Child::Rebuilding(vb) => Child::Present(vb),
            Child::Missing(uuid) => Child::Missing(uuid),
        };
    }

    fn status(&self) -> Option<vdev_block::Status> {
        match self {
            Child::Present(vb) => Some(vb.status()),
            Child::Faulted(vb) => {
                let mut status = vb.status();
                status.health = Health::Faulted(FaultedReason::User);
                Some(status)
            },
            Child::Rebuilding(vb) => {
                let mut status = vb.status();
                status.health = Health::Degraded(nonzero!(1u8));
                Some(status)
            },
            Child::Missing(_) => None
        }
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.write_at(buf, lba))
    }

    fn write_label(&self, labeller: LabelWriter, txg: TxgT)
        -> Option<VdevBlockFut>
    {
        // Notably: don't write the label for Rebuilding disks, because they
        // don't have every part of this txg.
        self.as_present().map(|vb| vb.write_label(labeller, txg))
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> Option<VdevBlockFut>
    {
        self.as_present().map(|vb| vb.write_spacemap(sglist, idx, block))
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|vb| vb.writev_at(bufs, lba))
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.as_present().unwrap().lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> Option<u32> {
        self.as_present().map(|vb| vb.optimum_queue_depth())
    }

    fn size(&self) -> Option<LbaT> {
        self.as_present().map(|vb| vb.size())
    }

    fn sync_all(&self) -> Option<VdevBlockFut> {
        self.as_present().map(|vb| vb.sync_all())
    }

    fn uuid(&self) -> Uuid {
        match self {
            Child::Present(vb) => vb.uuid(),
            Child::Rebuilding(vb) => vb.uuid(),
            Child::Faulted(vb) => vb.uuid(),
            Child::Missing(uuid) => *uuid,
        }
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.as_present().unwrap().zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.as_present().unwrap().zones()
    }
}

/// `Mirror`: Device mirroring, both permanent and temporary
///
/// This Vdev mirrors two or more children.  It is used for both permanent
/// mirrors and for children which are spared or being replaced.
pub struct Mirror {
    /// Underlying block devices.
    children: Vec<Child>,

    /// Wrapping index of the next child to read from during read operations
    // To eliminate the need for atomic divisions, the index is allowed to wrap
    // at 2**32.
    next_read_idx: AtomicU32,

    /// Best number of queued commands for the whole `Mirror`
    // NB it might be different for reads than for writes
    optimum_queue_depth: u32,

    /// Size of the vdev in bytes.  It's the minimum of the childrens' sizes.
    size: LbaT,

    uuid: Uuid,
}

impl Mirror {
    /// Create a new Mirror from unused files or devices
    ///
    /// * `lbas_per_zone`:      If specified, this many LBAs will be assigned to
    ///                         simulated zones on devices that don't have
    ///                         native zones.
    /// * `paths`:              Slice of pathnames of files and/or devices
    pub fn create<P>(
        paths: &[P],
        lbas_per_zone: Option<NonZeroU64>
    ) -> io::Result<Self>
        where P: AsRef<Path>
    {
        let uuid = Uuid::new_v4();
        let mut children = Vec::with_capacity(paths.len());
        for path in paths {
            let vb = VdevBlock::create(path, lbas_per_zone)?;
            children.push(Child::present(vb));
        }
        Ok(Mirror::new(uuid, children))
    }

    /// Asynchronously erase a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.erase_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    /// Mark a leaf device as faulted.
    /// 
    /// # Returns
    /// * Ok(()) if the device could be faulted
    /// * Err(Error::ENOENT) if there is no such leaf device
    pub fn fault(&mut self, uuid: Uuid) -> Result<()> {
        for child in self.children.iter_mut() {
            if child.uuid() == uuid {
                child.fault();
                return Ok(());
            }
        }
        Err(Error::ENOENT)
    }

    /// Asynchronously finish a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.finish_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn first_present_child(&self) -> &Child {
        for child in self.children.iter() {
            if child.is_present() {
                return child
            }
        }
        panic!("No present children");
    }

    fn new(uuid: Uuid, children: Vec<Child>) -> Self
    {
        assert!(!children.is_empty(), "Need at least one disk");

        let next_read_idx = AtomicU32::new(0);

        // NB: the optimum queue depth should actually be greater for healthy
        // reads than for writes.  This calculation computes it for writes.
        let optimum_queue_depth = children.iter()
        .filter_map(Child::optimum_queue_depth)
        .min()
        .unwrap();

        // XXX BUG!!! The size should be written to the Label at construction
        // time.  Otherwise, creating a mirror from dissimilar children and
        // then removing the smallest could cause the size to increase.
        // TODO: FIXME.
        let size = children.iter()
        .filter_map(Child::size)
        .min()
        .unwrap();

        Self {
            uuid,
            next_read_idx,
            optimum_queue_depth,
            size,
            children
        }
    }

    /// Open an existing `VdevMirror` from its component devices
    ///
    /// # Parameters
    ///
    /// * `uuid`:       Uuid of the desired `Mirror`, if present.  If `None`,
    ///                 then it will not be verified.
    /// * `combined`:   All the present children `VdevBlock`s with their label
    ///                 readers.
    fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
        -> (Self, LabelReader)
    {
        let mut label_pair = None;
        let highest_txg = combined.iter()
            .map(|(vb, _)| vb.txg())
            .max()
            .expect("Must have at least one child");
        let mut leaves = combined.into_iter()
            .map(|(vdev_block, mut label_reader)| {
                let label: Label = label_reader.deserialize().unwrap();
                if let Some(u) = uuid {
                    assert_eq!(u, label.uuid,
                               "Opening disk from wrong mirror");
                }
                if label_pair.is_none() {
                    label_pair = Some((label, label_reader));
                }
                (vdev_block.uuid(), vdev_block)
            }).collect::<BTreeMap<Uuid, VdevBlock>>();
        let (label, reader) = label_pair.unwrap();
        assert!(!leaves.is_empty(), "Must have at least one child");
        let mut children = Vec::with_capacity(label.children.len());
        for lchild in label.children.iter() {
            if let Some(vb) = leaves.remove(lchild) {
                if vb.txg() == highest_txg {
                    children.push(Child::present(vb));
                } else {
                    children.push(Child::rebuilding(vb));
                }
            } else {
                children.push(Child::missing(*lchild));
            }
        }
        (Mirror::new(label.uuid, children), reader)
    }

    pub fn open_zone(&self, start: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.open_zone(start)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> ReadAt {
        // TODO: optimize for null-mirror case; reduce clones
        let dbi = buf.clone_inaccessible();
        let (fut, children) = self.read_something(move |c| c.read_at(buf, lba));
        ReadAt {
            children,
            dbi,
            lba,
            fut
        }
    }

    /// Return the index of the next child to read from
    fn read_idx(&self) -> usize {
        self.next_read_idx.fetch_add(1, Ordering::Relaxed) as usize %
            self.children.len()
    }

    /// Read an LBA range including all parity.  Return an iterator that will
    /// yield every possible reconstruction of the data.
    ///
    /// As an optimization, if only one reconstruction is possible then
    /// immediately return EINTEGRITY, under the assumption that this method
    /// should only be called after a normal read already returned such an
    /// error.
    #[auto_enums::auto_enum(Future)]
    pub fn read_long(&self, len: LbaT, lba: LbaT)
        -> impl Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send
    {
        if self.children.iter().filter(|c| c.is_present()).count() <= 1 {
            future::err(Error::EINTEGRITY)
        } else {
            self.children.iter()
            .filter_map(Child::as_present)
            .map(|child| {
                let dbs = DivBufShared::from(vec![0u8; len as usize * BYTES_PER_LBA]);
                let dbm = dbs.try_mut().unwrap();
                child.read_at(dbm, lba)
                    .map_ok(move |_| dbs)
            }).collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .map(|r| {
                if r.iter().all(Result::is_err) {
                    Err(*r[0].as_ref().unwrap_err())
                } else {
                    Ok(Box::new(r.into_iter().filter_map(Result::ok)) as Box<dyn Iterator<Item=DivBufShared> + Send>)
                }
            })
        }
    }

    /// Issue a read operation on the one child, and return a Vec of the other
    /// Present children.
    fn read_something<F, R>(&self, f: F) -> (Option<R>, Vec<Arc<VdevBlock>>)
        where F: FnOnce(&Arc<VdevBlock>) -> R
    {
        let nchildren = self.children.len();
        let idx = self.read_idx();
        let first_child_idx = (idx..(idx + nchildren))
            .map(|i| i % nchildren)
            .find(|i| self.children[*i].is_present());
        match first_child_idx {
            Some(idx) => {
                let active_child = self.children[idx].as_present().unwrap();
                let fut = Some(f(active_child));
                let children = (0..nchildren)
                    .filter_map(|child_idx| {
                        if child_idx != idx {
                            self.children[child_idx].as_present().cloned()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                (fut, children)
            },
            None => (None, Vec::new())
        }
    }

    pub fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> ReadSpacemap
    {
        let dbi = buf.clone_inaccessible();
        let (fut, children) = self.read_something(move |c|
            c.read_spacemap(buf, smidx)
        );
        ReadSpacemap {
            children,
            dbi,
            smidx,
            fut
        }
    }

    #[tracing::instrument(skip(self, bufs))]
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> ReadvAt
    {
        let dbis = bufs.iter()
            .map(DivBufMut::clone_inaccessible)
            .collect::<Vec<_>>();
        let (fut, children) = self.read_something(move |c|
            c.readv_at(bufs, lba)
        );
        ReadvAt {
            children,
            dbis,
            lba,
            fut
        }
    }

    /// Repair any degraded children by rewriting the given Zone, if
    /// necessary.  Implicitly handles any required zone operations, like
    /// erase/open.
    // If multiple mirror children are repairing, then this function will repair
    // all of them, in lockstep.  This strategy avoids reading the same data
    // multiple times, which would be necessary if we were to repair each child
    // independently.
    // TODO: return a different error code for errors during read than for
    // errors during write.  In the former case, RAID error recovery may help.
    // TODO: handle Open Zones by supplying some kind of maximum LBA argument.
    pub fn repair_zone(&self, zone: ZoneT)
        -> impl Future<Output=Result<()>> + Send + Sync
    {
        /* Outline:
         * create 2 loops:
         *   - First loop reads blocks from good disks
         *   - First loop writes into a size-bounded queue
         *   - Second loop reads from queue and writes to bad disks
         */
        /* These constants are unoptimized guesses */
        const BLOCKSIZE_BYTES: usize = 1 << 18;
        const BLOCKSIZE_LBAS: LbaT = (BLOCKSIZE_BYTES / BYTES_PER_LBA) as LbaT;
        const QDEPTH: usize = 1;

        let (mut start, end) = self.zone_limits(zone);
        let read_children = self.children.iter()
            .filter_map(Child::as_present)
            .cloned()
            .collect::<Vec<_>>();
        let write_children = self.children.iter()
            .filter_map(|c| match c {
                Child::Rebuilding(vb) => Some(vb.clone()),
                _ => None
            }).collect::<Vec<_>>();
        let have_writers: bool = !write_children.is_empty();

        let (mut tx, rx) = mpsc::channel(QDEPTH);
        let mut idx = self.read_idx();
        let mut wlba = start;
        let reader = async move {
            while start < end && have_writers {
                let bufsize = BLOCKSIZE_LBAS.min(end - start) as usize *
                    BYTES_PER_LBA;
                let dbs = DivBufShared::uninitialized(bufsize);
                let dbm = dbs.try_mut().unwrap();
                let child = idx % read_children.len();
                // TODO: recover from a read EIO, if there are other children.
                // Can't use Mirror::read_at, because we lack `self`.
                read_children[child].read_at(dbm, start).await?;
                idx += 1;
                start += BLOCKSIZE_LBAS;
                tx.feed(dbs).await.map_err(|_| Error::EPIPE)?;
            }
            Ok(())
        };
        let writer = rx.map(Ok)
            .try_for_each_concurrent(Some(QDEPTH), move |dbs| {
                let db = dbs.try_const().unwrap();
                let fut = write_children.iter().map(|vb| {
                    vb.write_at(db.clone(), wlba)
                }).collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
                .map_ok(drop);
                wlba += BLOCKSIZE_LBAS;
                fut
            });
        future::try_join(reader, writer)
            .map_ok(drop)
    }

    /// Return a previously faulted device to service
    /// 
    /// # Returns
    /// * Ok(()) if the device could be rebuilt
    /// * Err(Error::ENOENT) if there is no such leaf device
    pub fn rebuild(&mut self, uuid: Uuid) -> Result<()> {
        for child in self.children.iter_mut() {
            if child.uuid() == uuid {
                child.rebuild();
                return Ok(());
            }
        }
        Err(Error::ENOENT)
    }

    /// Return any fully rebuilt disks to service
    ///
    /// It is the caller's responsibility to ensure that all data has been fully
    /// rebuilt on the Mirror.  It is also the caller's responsibility to ensure
    /// that no Mirror child transitions from Faulted to Rebuilding unless it
    /// already has all of the Zones that any other Rebuilding children have.
    pub fn restore(&mut self) {
        for child in self.children.iter_mut() {
            if child.is_rebuilding() {
                child.restore();
            }
        }
    }

    // XXX TODO: in the case of a missing child, get path from the label
    pub fn status(&self) -> Status {
        let nchildren = self.children.len();
        let mut leaves = Vec::with_capacity(nchildren);
        for child in self.children.iter() {
            let cs = child.status().unwrap_or(vdev_block::Status {
                health: Health::Faulted(FaultedReason::Removed),
                uuid: child.uuid(),
                path: std::path::PathBuf::new()
            });
            leaves.push(cs);
        }
        let sick_children = leaves.iter()
            .filter(|l| l.health != Health::Online)
            .count();
        let health = if sick_children == nchildren {
            Health::Faulted(FaultedReason::InsufficientRedundancy)
        } else {
            NonZeroU8::new(sick_children as u8)
                .map(Health::Degraded)
                .unwrap_or(Health::Online)
        };
        let rebuilding = self.children.iter()
            .filter_map(Child::as_rebuilding)
            .map(|vb| vb.txg())
            .min();
        Status {
            health,
            leaves,
            rebuilding,
            uuid: self.uuid()
        }
    }

    pub fn sync_all(&self) -> BoxVdevFut {
        // TODO: handle errors on some devices
        let fut = self.children.iter()
        .filter_map(Child::sync_all)
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    // TODO: can we eliminate this box?
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.write_at(buf.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_label(&self, mut labeller: LabelWriter, txg: TxgT)
        -> BoxVdevFut
    {
        let children_uuids = self.children.iter().map(Child::uuid)
            .collect::<Vec<_>>();
        let label = Label {
            uuid: self.uuid,
            children: children_uuids
        };
        labeller.serialize(&label).unwrap();
        let fut = self.children.iter().filter_map(|bd| {
           bd.write_label(labeller.clone(), txg)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  BoxVdevFut
    {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.write_spacemap(sglist.clone(), idx, block)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.writev_at(bufs.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }
}

impl Vdev for Mirror {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.first_present_child().lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.optimum_queue_depth
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.first_present_child().zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.first_present_child().zones()
    }
}

/// Future type of [`Mirror::read_at`]
// It's a custom future in order to implement error recovery
#[pin_project]
pub struct ReadAt {
    children: Vec<Arc<VdevBlock>>,
    dbi: DivBufInaccessible,
    lba: LbaT,
    #[pin]
    fut: Option<VdevBlockFut>,
}
impl Future for ReadAt {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>
    {
        let mut pinned = self.as_mut().project();
        if pinned.fut.is_none() {
            return Poll::Ready(Err(Error::ENXIO));
        }
        loop {
            match pinned.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => {
                    if pinned.children.is_empty() {
                        // Out of children.  Fail the Read.
                        return Poll::Ready(Err(e));
                    }
                }
            }
            // Try a different child.
            let buf = pinned.dbi.try_mut().unwrap();
            let child = pinned.children.pop().unwrap();
            let new_fut = child.read_at(buf, *pinned.lba);
            pinned.fut.replace(new_fut);
        }
    }
}

/// Future type of [`Mirror::read_spacemap`]
#[pin_project]
pub struct ReadSpacemap {
    children: Vec<Arc<VdevBlock>>,
    dbi: DivBufInaccessible,
    /// Spacemap index to read
    smidx: u32,
    #[pin]
    fut: Option<VdevBlockFut>,
}
impl Future for ReadSpacemap {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>
    {
        let mut pinned = self.as_mut().project();
        if pinned.fut.is_none() {
            return Poll::Ready(Err(Error::ENXIO));
        }
        loop {
            match pinned.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => {
                    if pinned.children.is_empty() {
                        // Out of children.  Fail the Read.
                        return Poll::Ready(Err(e));
                    }
                }
            }
            // Try a different child.
            let buf = pinned.dbi.try_mut().unwrap();
            let child = pinned.children.pop().unwrap();
            let new_fut = child.read_spacemap(buf, *pinned.smidx);
            pinned.fut.replace(new_fut);
        }
    }
}

/// Future type of [`Mirror::readv_at`]
#[pin_project]
pub struct ReadvAt {
    children: Vec<Arc<VdevBlock>>,
    dbis: Vec<DivBufInaccessible>,
    /// Address to read from the lower devices
    lba: LbaT,
    #[pin]
    fut: Option<VdevBlockFut>,
}
impl Future for ReadvAt {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output>
    {
        let mut pinned = self.as_mut().project();
        if pinned.fut.is_none() {
            return Poll::Ready(Err(Error::ENXIO));
        }
        loop {
            match pinned.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => {
                    if pinned.children.is_empty() {
                        // Out of children.  Fail the Read.
                        return Poll::Ready(Err(e));
                    }
                }
            }
            // Try a different child.
            let bufs = pinned.dbis.iter()
                .map(|dbi| dbi.try_mut().unwrap())
                .collect::<Vec<_>>();
            let child = pinned.children.pop().unwrap();
            let new_fut = child.readv_at(bufs, *pinned.lba);
            pinned.fut.replace(new_fut);
        }
    }
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub Mirror {
        #[mockall::concretize]
        pub fn create<P>(paths: &[P], lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path>;
        pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn fault(&mut self, uuid: Uuid) -> Result<()>;
        pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
            -> (Self, LabelReader);
        pub fn open_zone(&self, start: LbaT) -> BoxVdevFut;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_long(&self, len: LbaT, lba: LbaT)
            -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>;
        pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn repair_zone(&self, zone: ZoneT) -> BoxVdevFut;
        pub fn restore(&mut self);
        pub fn status(&self) -> Status;
        pub fn sync_all(&self) -> BoxVdevFut;
        pub fn uuid(&self) -> Uuid;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, labeller: LabelWriter, txg: TxgT)
            -> BoxVdevFut;
        pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            ->  BoxVdevFut;
        pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for Mirror {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {
    use std::{
        path::PathBuf,
        sync::{Arc, atomic::{AtomicU32, Ordering}}
    };
    use divbuf::DivBufShared;
    use futures::future;
    use itertools::Itertools;
    use mockall::predicate::*;
    use rstest::rstest;
    use super::*;

    fn mock_vdev_block() -> VdevBlock {
        const ZL0: (LbaT, LbaT) = (3, 32);
        //const ZL1 = (32, 64);

        let mut bd = VdevBlock::default();
        bd.expect_uuid()
            .return_const(Uuid::new_v4());
        bd.expect_optimum_queue_depth()
            .return_const(10u32);
        bd.expect_size()
            .return_const(262_144u64);
        bd.expect_zone_limits()
            .with(eq(0))
            .return_const(ZL0);
        bd.expect_lba2zone()
            .with(eq(30))
            .return_const(0);
        bd.expect_zones()
            .return_const(32768u32);
        bd
    }

    mod erase_zone {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_erase_zone()
                    .once()
                    .with(eq(3), eq(31))
                    .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.erase_zone(3, 31).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd0 = mock_vdev_block();
            bd0.expect_erase_zone()
                .once()
                .with(eq(3), eq(31))
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.erase_zone(3, 31).now_or_never().unwrap().unwrap();
        }
    }

    mod fault {
        use super::*;

        fn mock() -> VdevBlock {
            let mut bd = mock_vdev_block();
            bd.expect_status()
                .return_const(crate::vdev_block::Status {
                    health: Health::Online,
                    path: PathBuf::from("/dev/whatever"),
                    uuid: Uuid::new_v4()
                });
            bd
        }

        #[test]
        fn enoent() {
            let bd0 = mock();
            let children = vec![Child::present(bd0)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            assert_eq!(Error::ENOENT, mirror.fault(Uuid::new_v4()).unwrap_err());
            assert_eq!(Health::Online, mirror.status().health);
        }

        /// Mirror::fault is a no-op if the child is already Faulted
        #[test]
        fn faulted() {
            let bd0 = mock();
            let bd1 = mock();
            let bd1_uuid = bd1.uuid();
            let children = vec![
                Child::present(bd0),
                Child::faulted(bd1)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.fault(bd1_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Faulted(FaultedReason::User));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        #[test]
        fn present() {
            let bd0 = mock();
            let bd1 = mock();
            let bd0_uuid = bd0.uuid();
            let children = vec![Child::present(bd0), Child::present(bd1)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.fault(bd0_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[0].health,
                       Health::Faulted(FaultedReason::User));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        /// Mirror::fault is a no-op if the child is already Missing.  But the
        /// child's status will remain "Faulted" rather than switching to
        /// "Removed".
        #[test]
        fn missing() {
            let bd0 = mock();
            let faulty_uuid = Uuid::new_v4();
            let children = vec![
                Child::present(bd0),
                Child::missing(faulty_uuid)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.fault(faulty_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Faulted(FaultedReason::Removed));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        /// faulting the only child of a mirror should work, but the Mirror will
        /// end up faulted.
        #[test]
        fn only_child() {
            let bd0 = mock();
            let bd0_uuid = bd0.uuid();
            let children = vec![Child::present(bd0)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.fault(bd0_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[0].health,
                       Health::Faulted(FaultedReason::User));
            assert_eq!(Health::Faulted(FaultedReason::InsufficientRedundancy),
                status.health);
        }
    }

    mod finish_zone {
        use super::*;

        #[test]
        fn basic() {
            fn mock() -> Child {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_finish_zone()
                    .once()
                    .with(eq(3), eq(31))
                    .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            }
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.finish_zone(3, 31).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd0 = mock_vdev_block();
            bd0.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd0.expect_finish_zone()
                .once()
                .with(eq(3), eq(31))
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.finish_zone(3, 31).now_or_never().unwrap().unwrap();
        }
    }

    mod lba2zone {
        use super::*;

        #[test]
        fn degraded() {
            for i in 0..2 {
                let mut children = vec![
                    Child::present(mock_vdev_block()),
                    Child::present(mock_vdev_block()),
                ];
                children[i] = Child::missing(Uuid::new_v4());
                let mirror = Mirror::new(Uuid::new_v4(), children);
                assert_eq!(Some(0), mirror.lba2zone(30));
            }
        }
    }

    mod open {
        use super::*;

        /// Regardless of the order in which the devices are given to
        /// Mirror::open, it will construct itself in the correct order.
        #[test]
        fn ordering() {
            let child_uuid0 = Uuid::new_v4();
            let child_uuid1 = Uuid::new_v4();
            let child_uuid2 = Uuid::new_v4();
            fn mock(child_uuid: &Uuid) -> VdevBlock {
                let mut bd = VdevBlock::default();
                bd.expect_uuid()
                    .return_const(*child_uuid);
                bd.expect_optimum_queue_depth()
                    .return_const(10u32);
                bd.expect_size()
                    .return_const(262_144u64);
                bd.expect_txg()
                    .return_const(TxgT::from(42u32));
                bd
            }
            let label = Label {
                uuid: Uuid::new_v4(),
                children: vec![child_uuid0, child_uuid1, child_uuid2]
            };
            let mut serialized = Vec::new();
            let mut lw = LabelWriter::new(0);
            lw.serialize(&label).unwrap();
            for buf in lw.into_sglist().into_iter() {
                serialized.extend(&buf[..]);
            }
            let child_uuids = [child_uuid0, child_uuid1, child_uuid2];

            for perm in child_uuids.iter().permutations(child_uuids.len()) {
                let bd0 = mock(perm[0]);
                let bd1 = mock(perm[1]);
                let bd2 = mock(perm[2]);
                let mut combined = Vec::new();
                for bd in [bd0, bd1, bd2] {
                    let lr = LabelReader::new(serialized.clone()).unwrap();
                    combined.push((bd, lr));
                }
                let (mirror, _) = Mirror::open(Some(label.uuid), combined);
                assert!(mirror.children[0].is_present());
                assert_eq!(mirror.children[0].uuid(), child_uuid0);
                assert!(mirror.children[1].is_present());
                assert_eq!(mirror.children[1].uuid(), child_uuid1);
                assert!(mirror.children[2].is_present());
                assert_eq!(mirror.children[2].uuid(), child_uuid2);
            }
        }

        /// If one VdevBlock is out-of-date, it will be opened into the
        /// Rebuilding state.
        #[test]
        fn out_of_date() {
            let child_uuid0 = Uuid::new_v4();
            let child_uuid1 = Uuid::new_v4();
            fn mock(child_uuid: &Uuid) -> VdevBlock {
                let mut bd = VdevBlock::default();
                bd.expect_uuid()
                    .return_const(*child_uuid);
                bd.expect_optimum_queue_depth()
                    .return_const(10u32);
                bd.expect_size()
                    .return_const(262_144u64);
                bd
            }
            let label = Label {
                uuid: Uuid::new_v4(),
                children: vec![child_uuid0, child_uuid1]
            };
            let mut serialized = Vec::new();
            let mut lw = LabelWriter::new(0);
            lw.serialize(&label).unwrap();
            for buf in lw.into_sglist().into_iter() {
                serialized.extend(&buf[..]);
            }
            let mut bd0 = mock(&child_uuid0);
            bd0.expect_txg()
                .return_const(TxgT::from(42u32));
            let mut bd1 = mock(&child_uuid1);
            bd1.expect_txg()
                .return_const(TxgT::from(41u32));

            let mut combined = Vec::new();
            for bd in [bd0, bd1] {
                let lr = LabelReader::new(serialized.clone()).unwrap();
                combined.push((bd, lr));
            }
            let (mirror, _) = Mirror::open(Some(label.uuid), combined);
            assert!(mirror.children[0].is_present());
            assert_eq!(mirror.children[0].uuid(), child_uuid0);
            assert!(mirror.children[1].is_rebuilding());
            assert_eq!(mirror.children[1].uuid(), child_uuid1);
        }
    }

    mod open_zone {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd0 = mock_vdev_block();
            bd0.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
        }
    }

    mod optimum_queue_depth {
        use super::*;

        #[test]
        fn degraded() {
            for i in 0..2 {
                let mut children = vec![
                    Child::present(mock_vdev_block()),
                    Child::present(mock_vdev_block()),
                ];
                children[i] = Child::missing(Uuid::new_v4());
                let mirror = Mirror::new(Uuid::new_v4(), children);
                assert_eq!(10, mirror.optimum_queue_depth());
            }
        }
    }

    mod read_at {
        use super::*;

        fn mock(times: usize, r: Result<()>, total_reads: Arc<AtomicU32>)
            -> VdevBlock
        {
            let mut bd = mock_vdev_block();
            bd.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_read_at()
                .times(times)
                .withf(|buf, _lba| buf.len() == 4096)
                .returning(move |_, _| {
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ready(r))
                });
            bd
        }

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(0, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.read_at(buf, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        /// No read should be attempted on missing children
        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(2, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            for i in 3..5 {
                let buf = dbs.try_mut().unwrap();
                mirror.read_at(buf, i).now_or_never().unwrap().unwrap();
            }
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }

        /// Multiple reads should be distributed across all children
        #[test]
        fn distributes() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(3, Ok(()), total_reads.clone());
            let bd1 = mock(3, Ok(()), total_reads.clone());
            let bd2 = mock(3, Ok(()), total_reads);
            let children = vec![
                Child::present(bd0),
                Child::present(bd1),
                Child::present(bd2),
            ];
            let uuid = Uuid::new_v4();
            let mirror = Mirror::new(uuid, children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            for i in 3..12 {
                let buf = dbs.try_mut().unwrap();
                mirror.read_at(buf, i).now_or_never().unwrap().unwrap();
            }
        }

        /// If a read returns EIO, Mirror should retry it on another child.
        #[test]
        fn recoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            assert_eq!(mirror.next_read_idx.load(Ordering::Relaxed), 0,
                "Need to swap the disks' return values to fix the test");
            mirror.read_at(buf, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }

        /// If all children fail a read, it should be passed upstack
        #[test]
        fn unrecoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);

            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Err(Error::ENXIO), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let r = mirror.read_at(buf, 3).now_or_never().unwrap();
            assert!(r == Err(Error::EIO) || r == Err(Error::ENXIO));
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }
    }

    mod read_long {
        use super::*;

        fn mock(times: usize, r: Result<()>, total_reads: Arc<AtomicU32>)
            -> VdevBlock
        {
            let mut bd = mock_vdev_block();
            bd.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_read_at()
                .times(times)
                .withf(|buf, _lba| buf.len() == 4096)
                .returning(move |_, _| {
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ready(r))
                });
            bd
        }

        #[test]
        fn basic() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(1, Ok(()), total_reads.clone());
            let bd2 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1),
                Child::present(bd2)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let reconstructions = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 3);
            assert_eq!(3, reconstructions.count());
        }

        /// If only one reconstruction is possible, then immediately return
        /// EINTEGRITY.
        #[test]
        fn critically_degraded() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(0, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let err = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .err()
                .unwrap();
            assert_eq!(Error::EINTEGRITY, err);
            assert_eq!(total_reads.load(Ordering::Relaxed), 0);
        }

        /// No read should be attempted on missing children
        #[test]
        fn degraded() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let reconstructions = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
            assert_eq!(2, reconstructions.count());
        }

        /// If only one reconstruction is possible, then immediately return
        /// EINTEGRITY.
        #[test]
        fn nonredundant() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(0, Ok(()), total_reads.clone());
            let children = vec![Child::present(bd0)];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let err = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .err()
                .unwrap();
            assert_eq!(Error::EINTEGRITY, err);
            assert_eq!(total_reads.load(Ordering::Relaxed), 0);
        }

        /// If fewer than all children fail, the read_long should proceed with
        /// the other children.
        #[test]
        fn recoverable_eio() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd2 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1),
                Child::present(bd2)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let reconstructions = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 3);
            assert_eq!(2, reconstructions.count());
        }

        /// If all children fail, then the read_long must return an error.
        #[test]
        fn unrecoverable_eio() {
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd2 = mock(1, Err(Error::EIO), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1),
                Child::present(bd2)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let err = mirror.read_long(1, 3)
                .now_or_never()
                .unwrap()
                .err()
                .unwrap();
            assert_eq!(Error::EIO, err);
            assert_eq!(total_reads.load(Ordering::Relaxed), 3);
        }
    }

    mod read_spacemap {
        use super::*;

        fn mock(times: usize, r: Result<()>, total_reads: Arc<AtomicU32>)
            -> VdevBlock
        {
            let mut bd = mock_vdev_block();
            bd.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_read_spacemap()
                .times(times)
                .withf(|buf, idx|
                    buf.len() == 4096
                    && *idx == 1
                ).returning(move |_, _| {
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ready(r))
                });
            bd
        }

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(0, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.read_spacemap(buf, 1).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.read_spacemap(buf, 1).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        /// If a read returns EIO, Mirror should retry it on another child.
        #[test]
        fn recoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            assert_eq!(mirror.next_read_idx.load(Ordering::Relaxed), 0,
                "Need to swap the disks' return values to fix the test");
            mirror.read_spacemap(buf, 1).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }

        /// If all children fail a read, it should be passed upstack
        #[test]
        fn unrecoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);

            let buf = dbs.try_mut().unwrap();
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Err(Error::ENXIO), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let r = mirror.read_spacemap(buf, 1).now_or_never().unwrap();
            assert!(r == Err(Error::EIO) || r == Err(Error::ENXIO));
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }
    }

    mod readv_at {
        use super::*;

        fn mock(times: usize, r: Result<()>, total_reads: Arc<AtomicU32>)
            -> VdevBlock
        {
            let mut bd = mock_vdev_block();
            bd.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd.expect_readv_at()
                .times(times)
                .withf(|sglist, lba|
                    sglist.len() == 1
                    && sglist[0].len() == 4096
                    && *lba == 3
                ).returning(move |_, _| {
                    total_reads.fetch_add(1, Ordering::Relaxed);
                    Box::pin(future::ready(r))
                });
            bd
        }

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let sglist = vec![buf];
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let bd1 = mock(0, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.readv_at(sglist, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);

            let mut bd0 = mock_vdev_block();
            bd0.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd0.expect_readv_at()
                .times(1)
                .withf(|sglist, lba| {
                    sglist.len() == 1
                    && sglist[0].len() == 4096
                    && *lba == 3
                }).returning(move |_, _| Box::pin(future::ok(())));
            bd0.expect_readv_at()
                .times(1)
                .withf(|sglist, lba| {
                    sglist.len() == 1
                    && sglist[0].len() == 4096
                    && *lba == 4
                }).returning(move |_, _| Box::pin(future::ok(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            for i in 3..5 {
                let buf = dbs.try_mut().unwrap();
                let sglist = vec![buf];
                mirror.readv_at(sglist, i).now_or_never().unwrap().unwrap();
            }
        }

        /// If a read returns EIO, Mirror should retry it on another child.
        #[test]
        fn recoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let sglist = vec![buf];
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            assert_eq!(mirror.next_read_idx.load(Ordering::Relaxed), 0,
                "Need to swap the disks' return values to fix the test");
            mirror.readv_at(sglist, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }

        /// If all children fail a read, it should be passed upstack
        #[test]
        fn unrecoverable_eio() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);

            let buf = dbs.try_mut().unwrap();
            let sglist = vec![buf];
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Err(Error::EIO), total_reads.clone());
            let bd1 = mock(1, Err(Error::ENXIO), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let r = mirror.readv_at(sglist, 3).now_or_never().unwrap();
            assert!(r == Err(Error::EIO) || r == Err(Error::ENXIO));
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
        }
    }

    mod repair_zone {
        use mockall::Sequence;
        use super::*;

        // TODO:
        // * Repairing children, but they don't need this TXG
        // * Two reparing children, but only one needs this TXG

        /// Errors during read should be handled gracefully.
        /// If no other healthy mirror child is available, then repair should
        /// fail.
        #[tokio::test]
        async fn eio_during_read_nonredundant() {
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            bd0.expect_read_at()
                .never();
            bd0.expect_write_at()
                .never();
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::err(Error::EIO))
                });
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            let e = mirror.repair_zone(0).await.unwrap_err();
            assert_eq!(e, Error::EIO);
        }

        /// Errors during read should be handled gracefully.
        /// If another healthy mirror child is available, then repair should
        /// attempt to recover from the read error.
        #[tokio::test]
        #[ignore = "feature not yet implemented"]
        async fn eio_during_read_redundant() {
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            let mut bd2 = mock_vdev_block();
            let mut seq = Sequence::new();
            bd0.expect_read_at()
                .never();
            bd1.expect_read_at()
                .times(1)
                .in_sequence(&mut seq)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::err(Error::EIO))
                });
            bd2.expect_read_at()
                .times(1)
                .in_sequence(&mut seq)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::ok(()))
                });
            bd0.expect_write_at()
                .once()
                .in_sequence(&mut seq)
                .withf(|buf, lba|
                    buf.len() == 1 << 18
                    && *lba == 32
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1),
                Child::present(bd2)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.repair_zone(0).await.unwrap();
        }

        /// Errors during write should be handled gracefully
        #[tokio::test]
        async fn eio_during_write() {
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            bd0.expect_read_at()
                .never();
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 29 << 12
                    && *lba == 3
                ).return_once(|_, _| Box::pin(future::err::<(), Error>(Error::EIO)));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::ok(()))
                });
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            let e = mirror.repair_zone(0).await.unwrap_err();
            assert_eq!(e, Error::EIO);
        }


        /// When rebuilding a large range, it will be split into multiple
        /// blocks.  The final one may be shortened.
        #[tokio::test]
        async fn multiple_blocks() {
            const ZL1: (LbaT, LbaT) = (32, 192);
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            bd0.expect_zone_limits()
                .with(eq(1))
                .return_const(ZL1);
            bd1.expect_zone_limits()
                .with(eq(1))
                .return_const(ZL1);
            bd0.expect_read_at()
                .never();
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 1 << 18
                    && *lba == 32
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 1 << 18
                    && *lba == 96
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 1 << 17
                    && *lba == 160
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 1 << 18
                       && *lba == 32
                ).returning(move |_, _| Box::pin(future::ok(())));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 1 << 18
                       && *lba == 96
                ).returning(move |_, _| Box::pin(future::ok(())));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 1 << 17
                       && *lba == 160
                ).returning(move |_, _| Box::pin(future::ok(())));
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.repair_zone(1).await.unwrap();
        }

        /// If no children need to be repaired, then no reads are necessary.
        #[tokio::test]
        async fn no_rebuilding_children() {
            let mut bd0 = mock_vdev_block();
            bd0.expect_read_at()
                .never();
            let faulty_uuid = Uuid::new_v4();
            let children = vec![
                Child::present(bd0),
                Child::missing(faulty_uuid)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.repair_zone(0).await.unwrap();
        }

        #[tokio::test]
        async fn one_rebuilding_child() {
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            bd0.expect_read_at()
                .never();
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 29 << 12
                    && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::ok(()))
                });
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1)
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.repair_zone(0).await.unwrap();
        }

        #[tokio::test]
        async fn two_rebuilding_children() {
            let mut bd0 = mock_vdev_block();
            let mut bd1 = mock_vdev_block();
            let mut bd2 = mock_vdev_block();
            bd0.expect_read_at()
                .never();
            bd2.expect_read_at()
                .never();
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 29 << 12
                    && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd2.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 29 << 12
                    && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            bd1.expect_read_at()
                .times(1)
                .withf(|buf, lba|
                       buf.len() == 29 << 12
                       && *lba == 3
                ).returning(move |_, _| {
                    Box::pin(future::ok(()))
                });
            let children = vec![
                Child::rebuilding(bd0),
                Child::present(bd1),
                Child::rebuilding(bd2),
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.repair_zone(0).await.unwrap();
        }
    }

    mod rebuild {
        use super::*;

        fn mock() -> VdevBlock {
            let mut bd = mock_vdev_block();
            bd.expect_status()
                .return_const(crate::vdev_block::Status {
                    health: Health::Online,
                    path: PathBuf::from("/dev/whatever"),
                    uuid: Uuid::new_v4()
                });
            bd
        }

        #[test]
        fn enoent() {
            let bd0 = mock();
            let children = vec![Child::present(bd0)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            assert_eq!(Error::ENOENT, mirror.rebuild(Uuid::new_v4()).unwrap_err());
            assert_eq!(Health::Online, mirror.status().health);
        }

        #[test]
        fn faulted() {
            let txg = TxgT::from(42);
            let bd0 = mock();
            let mut bd1 = mock();
            bd1.expect_txg()
                .return_const(txg);
            let bd1_uuid = bd1.uuid();
            let children = vec![
                Child::present(bd0),
                Child::faulted(bd1)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.rebuild(bd1_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Degraded(nonzero!(1u8)));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
            assert_eq!(status.rebuilding, Some(txg));
        }

        /// A missing disk cannot be rebuilt
        #[test]
        fn missing() {
            let bd0 = mock();
            let faulty_uuid = Uuid::new_v4();
            let children = vec![
                Child::present(bd0),
                Child::missing(faulty_uuid)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.rebuild(faulty_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Faulted(FaultedReason::Removed));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        /// Rebuilding a healthy disk is a no-op
        #[test]
        fn present() {
            let bd0 = mock();
            let bd1 = mock();
            let bd0_uuid = bd0.uuid();
            let children = vec![Child::present(bd0), Child::present(bd1)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.rebuild(bd0_uuid).unwrap();
            let status = mirror.status();
            assert_eq!(status.leaves[0].health,
                       Health::Online);
            assert_eq!(Health::Online, status.health);
        }

    }

    mod restore {
        use super::*;

        fn mock() -> VdevBlock {
            let mut bd = mock_vdev_block();
            bd.expect_status()
                .return_const(crate::vdev_block::Status {
                    health: Health::Online,
                    path: PathBuf::from("/dev/whatever"),
                    uuid: Uuid::new_v4()
                });
            bd
        }

        #[test]
        fn faulted() {
            let txg = TxgT::from(42);
            let bd0 = mock();
            let mut bd1 = mock();
            bd1.expect_txg()
                .return_const(txg);
            let children = vec![
                Child::present(bd0),
                Child::faulted(bd1)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.restore();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Faulted(FaultedReason::User));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        /// A missing disk cannot be restored
        #[test]
        fn missing() {
            let bd0 = mock();
            let faulty_uuid = Uuid::new_v4();
            let children = vec![
                Child::present(bd0),
                Child::missing(faulty_uuid)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.restore();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health,
                       Health::Faulted(FaultedReason::Removed));
            assert_eq!(Health::Degraded(nonzero!(1u8)), status.health);
        }

        /// Restoring a healthy disk is a no-op
        #[test]
        fn present() {
            let bd0 = mock();
            let bd1 = mock();
            let children = vec![Child::present(bd0), Child::present(bd1)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.restore();
            let status = mirror.status();
            assert_eq!(status.leaves[0].health,
                       Health::Online);
            assert_eq!(Health::Online, status.health);
        }

        #[test]
        fn repairing() {
            let txg = TxgT::from(42);
            let bd0 = mock();
            let mut bd1 = mock();
            bd1.expect_txg()
                .return_const(txg);
            let children = vec![
                Child::present(bd0),
                Child::rebuilding(bd1)
            ];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.restore();
            let status = mirror.status();
            assert_eq!(status.leaves[1].health, Health::Online);
            assert_eq!(Health::Online, status.health);
            assert_eq!(status.rebuilding, None);
        }
    }

    mod size {
        use super::*;

        #[test]
        fn degraded() {
            for i in 0..2 {
                let mut children = vec![
                    Child::present(mock_vdev_block()),
                    Child::present(mock_vdev_block()),
                ];
                children[i] = Child::missing(Uuid::new_v4());
                let mirror = Mirror::new(Uuid::new_v4(), children);
                assert_eq!(262_144u64, mirror.size());
            }
        }
    }

    mod status {
        use super::*;

        use crate::vdev::Health::*;

        fn mock(health: Option<Health>) -> Child {
            let uuid = Uuid::new_v4();
            if let Some(health) = health {
                let mut bd = mock_vdev_block();
                bd.expect_status()
                    .return_const(crate::vdev_block::Status {
                        health,
                        path: PathBuf::from("/dev/whatever"),
                        uuid: Uuid::new_v4()
                    });
                Child::present(bd)
            } else {
                Child::missing(uuid)
            }
        }

        /// When degraded, the mirror's health should be the worst of all
        /// degraded disks.
        #[rstest]
        #[case(Online, vec![Some(Online), Some(Online)])]
        #[case(Degraded(nonzero!(1u8)), vec![Some(Online), None])]
        #[case(Degraded(nonzero!(2u8)), vec![None, Some(Online), None])]
        // Note: don't test the faulted case, because we can't construct a
        // Mirror that's already faulted.
        fn degraded(
            #[case] health: Health,
            #[case] child_healths: Vec<Option<Health>>)
        {
            let children = child_healths.into_iter()
                .map(mock)
                .collect::<Vec<_>>();
            let mirror = Mirror::new(Uuid::new_v4(), children);
            assert_eq!(health, mirror.status().health);
        }
    }

    mod sync_all {
        use super::*;

        #[test]
        fn basic() {
            fn mock() -> Child {
                let mut bd = mock_vdev_block();
                bd.expect_sync_all()
                    .once()
                    .return_once(|| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            }
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.sync_all().now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd0 = mock_vdev_block();
            bd0.expect_sync_all()
                .once()
                .return_once(|| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.sync_all().now_or_never().unwrap().unwrap();
        }
    }

    mod write_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_write_at()
                    .once()
                    .withf(|buf, lba|
                        buf.len() == 4096
                        && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.write_at(buf, 3).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();

            let mut bd0 = mock_vdev_block();
            bd0.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd0.expect_write_at()
                .once()
                .withf(|buf, lba|
                    buf.len() == 4096
                    && *lba == 3
            ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.write_at(buf, 3).now_or_never().unwrap().unwrap();
        }
    }

    mod writev_at {
        use super::*;

        #[test]
        fn basic() {
            let dbs0 = DivBufShared::from(vec![1u8; 4096]);
            let dbs1 = DivBufShared::from(vec![2u8; 8192]);
            let buf0 = dbs0.try_const().unwrap();
            let buf1 = dbs1.try_const().unwrap();
            let sglist = vec![buf0, buf1];

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_open_zone()
                    .once()
                    .with(eq(0))
                    .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                bd.expect_writev_at()
                    .once()
                    .withf(|sglist, lba|
                        sglist.len() == 2
                        && sglist[0].len() == 4096
                        && sglist[1].len() == 8192
                        && *lba == 3
                ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.writev_at(sglist, 3).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let dbs0 = DivBufShared::from(vec![1u8; 4096]);
            let dbs1 = DivBufShared::from(vec![2u8; 8192]);
            let buf0 = dbs0.try_const().unwrap();
            let buf1 = dbs1.try_const().unwrap();
            let sglist = vec![buf0, buf1];

            let mut bd1 = mock_vdev_block();
            bd1.expect_open_zone()
                .once()
                .with(eq(0))
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            bd1.expect_writev_at()
                .once()
                .withf(|sglist, lba|
                    sglist.len() == 2
                    && sglist[0].len() == 4096
                    && sglist[1].len() == 8192
                    && *lba == 3
            ).return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::missing(Uuid::new_v4()),
                Child::present(bd1),
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.writev_at(sglist, 3).now_or_never().unwrap().unwrap();
        }
    }

    mod write_label {
        use super::*;

        #[test]
        fn basic() {
            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_write_label()
                .once()
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            let labeller = LabelWriter::new(0);
            mirror.write_label(labeller, TxgT::from(1))
                .now_or_never()
                .unwrap()
                .unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd1 = mock_vdev_block();
            bd1.expect_write_label()
                .once()
                .return_once(|_, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::missing(Uuid::new_v4()),
                Child::present(bd1),
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            let labeller = LabelWriter::new(0);
            mirror.write_label(labeller, TxgT::from(1))
                .now_or_never()
                .unwrap()
                .unwrap();
        }
    }

    mod write_spacemap {
        use super::*;

        #[test]
        fn basic() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();
            let sgl = vec![buf];

            let mock = || {
                let mut bd = mock_vdev_block();
                bd.expect_write_spacemap()
                    .once()
                    .withf(|sglist, idx, lba|
                        sglist.len() == 1
                        && sglist[0].len() == 4096
                        && *idx == 1
                        && *lba == 2
                ).return_once(|_, _, _| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            mirror.write_spacemap(sgl, 1, 2).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![1u8; 4096]);
            let buf = dbs.try_const().unwrap();
            let sgl = vec![buf];

            let mut bd0 = mock_vdev_block();
            bd0.expect_write_spacemap()
                .once()
                .withf(|sglist, idx, lba|
                    sglist.len() == 1
                    && sglist[0].len() == 4096
                    && *idx == 1
                    && *lba == 2
            ).return_once(|_, _, _| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.write_spacemap(sgl, 1, 2).now_or_never().unwrap().unwrap();
        }
    }

    mod zone_limits {
        use super::*;

        #[test]
        fn degraded() {
            for i in 0..2 {
                let mut children = vec![
                    Child::present(mock_vdev_block()),
                    Child::present(mock_vdev_block()),
                ];
                children[i] = Child::missing(Uuid::new_v4());
                let mirror = Mirror::new(Uuid::new_v4(), children);
                let zl = mirror.zone_limits(0);
                assert_eq!(zl, (3, 32));
            }
        }
    }

    mod zones {
        use super::*;

        #[test]
        fn degraded() {
            for i in 0..2 {
                let mut children = vec![
                    Child::present(mock_vdev_block()),
                    Child::present(mock_vdev_block()),
                ];
                children[i] = Child::missing(Uuid::new_v4());
                let mirror = Mirror::new(Uuid::new_v4(), children);
                assert_eq!(32768, mirror.zones());
            }
        }
    }

}
// LCOV_EXCL_STOP
