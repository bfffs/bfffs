// vim: tw=80
//! BFFFS Mirror layer
//!
//! This provides vdevs which slot between `raid` and `VdevBlock` and
//! provide mirror functionality.  That includes both permanent mirrors as well
//! as temporary mirrors, used for spares and replacements.

use std::{
    collections::BTreeMap,
    io,
    num::{NonZeroU8, NonZeroU64},
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering}
    }
};

use cfg_if::cfg_if;
use divbuf::{DivBufInaccessible, DivBufMut, DivBufShared};
use futures::{
    Future,
    FutureExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::FuturesUnordered,
    task::{Context, Poll}
};
use pin_project::pin_project;
use serde_derive::{Deserialize, Serialize};

use crate::{
    label::*,
    types::*,
    vdev::*,
    vdev_block,
    BYTES_PER_LBA
};

cfg_if! {
    if #[cfg(test)] {
        use mockall::mock;
        use BoxVdevFut as VdevBlockFut;
        use vdev_block::MockVdevBlock as VdevBlock;
    } else {
        use vdev_block::VdevBlockFut;
        use vdev_block::VdevBlock;
    }
}


#[derive(Serialize, Deserialize, Debug)]
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
    pub fn import(&mut self, uuid: Uuid)
        -> impl Future<Output=Result<(Mirror, LabelReader)>>
    {
        let ml = match self.mirrors.remove(&uuid) {
            Some(ml) => ml,
            None => return futures::future::err(Error::ENOENT).boxed()
        };
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
            }).boxed()
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
    Missing(Uuid)
}

impl Child {
    fn present(vdev: VdevBlock) -> Self {
        Child::Present(Arc::new(vdev))
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

    fn erase_zone(&self, start: LbaT, end: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.erase_zone(start, end))
    }

    fn finish_zone(&self, start: LbaT, end: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.finish_zone(start, end))
    }

    fn is_present(&self) -> bool {
        matches!(self, Child::Present(_))
    }

    fn open_zone(&self, start: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.open_zone(start))
    }

    fn status(&self) -> Option<vdev_block::Status> {
        self.as_present().map(|vb| vb.status())
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Option<VdevBlockFut> {
        self.as_present().map(|bd| bd.write_at(buf, lba))
    }

    fn write_label(&self, labeller: LabelWriter) -> Option<VdevBlockFut> {
        self.as_present().map(|vb| vb.write_label(labeller))
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

    fn sync_all(&self) -> Option<BoxVdevFut> {
        self.as_present().map(|vb| vb.sync_all())
    }

    fn uuid(&self) -> Uuid {
        match self {
            Child::Present(vb) => vb.uuid(),
            Child::Missing(uuid) => *uuid
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
                *child = Child::missing(uuid);
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
                children.push(Child::present(vb));
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
    pub fn read_long(&self, len: LbaT, lba: LbaT)
        -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>
    {
        if self.children.iter().filter(|c| c.is_present()).count() <= 1 {
            return future::err(Error::EINTEGRITY).boxed();
        }

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
        }).boxed()
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

    // XXX TODO: in the case of a missing child, get path from the label
    pub fn status(&self) -> Status {
        let nchildren = self.children.len();
        let mut leaves = Vec::with_capacity(nchildren);
        for child in self.children.iter() {
            let cs = child.status().unwrap_or(vdev_block::Status {
                health: Health::Faulted,
                uuid: child.uuid(),
                path: std::path::PathBuf::new()
            });
            leaves.push(cs);
        }
        let sick_children = leaves.iter()
            .filter(|l| l.health != Health::Online)
            .count();
        let health = if sick_children == nchildren {
            Health::Faulted
        } else {
            NonZeroU8::new(sick_children as u8)
                .map(Health::Degraded)
                .unwrap_or(Health::Online)
        };
        Status {
            health,
            leaves,
            uuid: self.uuid()
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.children.iter().filter_map(|blockdev| {
            blockdev.write_at(buf.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut
    {
        let children_uuids = self.children.iter().map(Child::uuid)
            .collect::<Vec<_>>();
        let label = Label {
            uuid: self.uuid,
            children: children_uuids
        };
        labeller.serialize(&label).unwrap();
        let fut = self.children.iter().filter_map(|bd| {
           bd.write_label(labeller.clone())
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

    fn sync_all(&self) -> BoxVdevFut {
        // TODO: handle errors on some devices
        let fut = self.children.iter()
        .filter_map(Child::sync_all)
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.first_present_child().zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.first_present_child().zones()
    }
}

/// Future type of [`Mirror::read_at`]
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

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
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

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
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

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
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
        pub fn status(&self) -> Status;
        pub fn uuid(&self) -> Uuid;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            ->  BoxVdevFut;
        pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for Mirror {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
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
    use nonzero_ext::nonzero;
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

        use nonzero_ext::nonzero;

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

        #[test]
        fn present() {
            let bd0 = mock();
            let bd1 = mock();
            let bd0_uuid = bd0.uuid();
            let children = vec![Child::present(bd0), Child::present(bd1)];
            let mut mirror = Mirror::new(Uuid::new_v4(), children);
            mirror.fault(bd0_uuid).unwrap();
            assert_eq!(Health::Degraded(nonzero!(1u8)), mirror.status().health);
        }

        /// Mirror::fault is a no-op if the child is already Missing
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
            assert_eq!(Health::Faulted, mirror.status().health);
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
                assert_eq!(mirror.children[0].uuid(), child_uuid0);
                assert_eq!(mirror.children[1].uuid(), child_uuid1);
                assert_eq!(mirror.children[2].uuid(), child_uuid2);
            }
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
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
                Child::present(bd)
            };
            let bd0 = mock();
            let bd1 = mock();
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1]);
            let labeller = LabelWriter::new(0);
            mirror.write_label(labeller).now_or_never().unwrap().unwrap();
        }

        #[test]
        fn degraded() {
            let mut bd1 = mock_vdev_block();
            bd1.expect_write_label()
                .once()
                .return_once(|_| Box::pin(future::ok::<(), Error>(())));
            let children = vec![
                Child::missing(Uuid::new_v4()),
                Child::present(bd1),
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children);
            let labeller = LabelWriter::new(0);
            mirror.write_label(labeller).now_or_never().unwrap().unwrap();
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
