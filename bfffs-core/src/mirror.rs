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
use divbuf::{DivBufInaccessible, DivBufMut};
use futures::{
    Future,
    TryFutureExt,
    TryStreamExt,
    stream::FuturesUnordered,
    task::{Context, Poll}
};
#[cfg(not(test))]
use futures::{FutureExt, StreamExt, future};
use pin_project::pin_project;
use serde_derive::{Deserialize, Serialize};

use crate::{
    label::*,
    types::*,
    vdev::*,
    vdev_block,
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
            None => return future::err(Error::ENOENT).boxed()
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
// We optimize for the large case (Present) which is both more important and
// more common than the small case.
#[allow(clippy::large_enum_variant)]
enum Child {
    Present(VdevBlock),
    Missing(Uuid)
}

impl Child {
    fn present(vdev: VdevBlock) -> Self {
        Child::Present(vdev)
    }

    fn missing(uuid: Uuid) -> Self {
        Child::Missing(uuid)
    }

    fn as_present(&self) -> Option<&VdevBlock> {
        if let Child::Present(vb) = self {
            Some(vb)
        } else {
            None
        }
    }

    fn erase_zone(&self, start: LbaT, end: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().erase_zone(start, end)
    }

    fn finish_zone(&self, start: LbaT, end: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().finish_zone(start, end)
    }

    fn open_zone(&self, start: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().open_zone(start)
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().read_at(buf, lba)
    }

    fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> VdevBlockFut {
        self.as_present().unwrap().read_spacemap(buf, smidx)
    }

    fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().readv_at(bufs, lba)
    }

    fn status(&self) -> Option<vdev_block::Status> {
        self.as_present().map(VdevBlock::status)
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().write_at(buf, lba)
    }

    fn write_label(&self, labeller: LabelWriter) -> VdevBlockFut {
        self.as_present().unwrap().write_label(labeller)
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> VdevBlockFut
    {
        self.as_present().unwrap().write_spacemap(sglist, idx, block)
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> VdevBlockFut {
        self.as_present().unwrap().writev_at(bufs, lba)
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.as_present().unwrap().lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> Option<u32> {
        self.as_present().map(VdevBlock::optimum_queue_depth)
    }

    fn size(&self) -> Option<LbaT> {
        self.as_present().map(VdevBlock::size)
    }

    fn sync_all(&self) -> BoxVdevFut {
        self.as_present().unwrap().sync_all()
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
    children: Arc<Box<[Child]>>,

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
        Ok(Mirror::new(uuid, children.into_boxed_slice()))
    }

    /// Asynchronously erase a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().map(|blockdev| {
            blockdev.erase_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    /// Asynchronously finish a zone on a mirror
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().map(|blockdev| {
            blockdev.finish_zone(start, end)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn new(uuid: Uuid, children: Box<[Child]>) -> Self
    {
        assert!(children.len() > 0, "Need at least one disk");

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
            children: Arc::new(children)
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
        (Mirror::new(label.uuid, children.into_boxed_slice()), reader)
    }

    pub fn open_zone(&self, start: LbaT) -> BoxVdevFut {
        let fut = self.children.iter().map(|blockdev| {
            blockdev.open_zone(start)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> ReadAt {
        let idx = self.read_idx();
        // TODO: optimize for null-mirror case; reduce clones
        let dbi = buf.clone_inaccessible();
        let fut = self.children[idx].read_at(buf, lba);
        ReadAt {
            children: self.children.clone(),
            issued: 1,
            initial_idx: idx,
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

    pub fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> ReadSpacemap
    {
        let ridx = self.read_idx();
        let dbi = buf.clone_inaccessible();
        let fut = self.children[ridx].read_spacemap(buf, smidx);
        ReadSpacemap {
            children: self.children.clone(),
            issued: 1,
            initial_idx: ridx,
            dbi,
            smidx,
            fut
        }
    }

    #[tracing::instrument(skip(self, bufs))]
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> ReadvAt
    {
        let idx = self.read_idx();
        let dbis = bufs.iter()
            .map(DivBufMut::clone_inaccessible)
            .collect::<Vec<_>>();
        let fut = self.children[idx].readv_at(bufs, lba);
        ReadvAt {
            children: self.children.clone(),
            issued: 1,
            initial_idx: idx,
            dbis,
            lba,
            fut
        }
    }

    // XXX TODO: in the case of a missing child, get path from the label
    pub fn status(&self) -> Status {
        let mut leaves = Vec::with_capacity(self.children.len());
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
            .count() as u8;
        let health = NonZeroU8::new(sick_children)
            .map(Health::Degraded)
            .unwrap_or(Health::Online);
        Status {
            health,
            leaves,
            uuid: self.uuid()
        }
    }

    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.children.iter().map(|blockdev| {
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
        let fut = self.children.iter().map(|bd| {
           bd.write_label(labeller.clone())
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  BoxVdevFut
    {
        let fut = self.children.iter().map(|blockdev| {
            blockdev.write_spacemap(sglist.clone(), idx, block)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut
    {
        let fut = self.children.iter().map(|blockdev| {
            blockdev.writev_at(bufs.clone(), lba)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }
}

impl Vdev for Mirror {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.children[0].lba2zone(lba)
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
        .map(Child::sync_all)
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.children[0].zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.children[0].zones()
    }
}

/// Future type of [`Mirror::read_at`]
#[pin_project]
pub struct ReadAt {
    children: Arc<Box<[Child]>>,
    /// Reads already issued so far
    issued: usize,
    /// Index of the first child read from
    initial_idx: usize,
    dbi: DivBufInaccessible,
    lba: LbaT,
    #[pin]
    fut: VdevBlockFut,
}
impl Future for ReadAt {
    type Output = Result<()>;

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
    {
        let nchildren = self.children.len();
        Poll::Ready(loop {
            let pinned = self.as_mut().project();
            let r = futures::ready!(pinned.fut.poll(cx));
            if r.is_ok() {
                break Ok(());
            }
            if *pinned.issued >= nchildren {
                // Out of children.  Fail the Read.
                break r;
            }
            // Try a different child.
            let buf = pinned.dbi.try_mut().unwrap();
            let idx = (*pinned.initial_idx + *pinned.issued) % nchildren;
            let new_fut = pinned.children[idx].read_at(buf, *pinned.lba);
            self.set(ReadAt {
                children: self.children.clone(),
                issued: self.issued + 1,
                initial_idx: self.initial_idx,
                dbi: self.dbi.clone(),
                lba: self.lba,
                fut: new_fut
            });
        })
    }
}

/// Future type of [`Mirror::read_spacemap`]
#[pin_project]
pub struct ReadSpacemap {
    children: Arc<Box<[Child]>>,
    /// Reads already issued so far
    issued: usize,
    /// Index of the first child read from
    initial_idx: usize,
    dbi: DivBufInaccessible,
    /// Spacemap index to read
    smidx: u32,
    #[pin]
    fut: VdevBlockFut,
}
impl Future for ReadSpacemap {
    type Output = Result<()>;

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
    {
        let nchildren = self.children.len();
        Poll::Ready(loop {
            let pinned = self.as_mut().project();
            let r = futures::ready!(pinned.fut.poll(cx));
            if r.is_ok() {
                break Ok(());
            }
            if *pinned.issued >= nchildren {
                // Out of children.  Fail the Read.
                break r;
            }
            // Try a different child.
            let buf = pinned.dbi.try_mut().unwrap();
            let idx = (*pinned.initial_idx + *pinned.issued) % nchildren;
            let new_fut = pinned.children[idx].read_spacemap(buf,
                *pinned.smidx);
            self.set(ReadSpacemap {
                children: self.children.clone(),
                issued: self.issued + 1,
                initial_idx: self.initial_idx,
                dbi: self.dbi.clone(),
                smidx: self.smidx,
                fut: new_fut
            });
        })
    }
}

/// Future type of [`Mirror::readv_at`]
#[pin_project]
pub struct ReadvAt {
    children: Arc<Box<[Child]>>,
    /// Reads already issued so far
    issued: usize,
    /// Index of the first child read from
    initial_idx: usize,
    dbis: Vec<DivBufInaccessible>,
    /// Address to read from the lower devices
    lba: LbaT,
    #[pin]
    fut: VdevBlockFut,
}
impl Future for ReadvAt {
    type Output = Result<()>;

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
    {
        let nchildren = self.children.len();
        Poll::Ready(loop {
            let pinned = self.as_mut().project();
            let r = futures::ready!(pinned.fut.poll(cx));
            if r.is_ok() {
                break Ok(());
            }
            if *pinned.issued >= nchildren {
                // Out of children.  Fail the Read.
                break r;
            }
            // Try a different child.
            let bufs = pinned.dbis.iter()
                .map(|dbi| dbi.try_mut().unwrap())
                .collect::<Vec<_>>();
            let idx = (*pinned.initial_idx + *pinned.issued) % nchildren;
            let new_fut = pinned.children[idx].readv_at(bufs, *pinned.lba);
            self.set(ReadvAt {
                children: self.children.clone(),
                issued: self.issued + 1,
                initial_idx: self.initial_idx,
                dbis: self.dbis.clone(),
                lba: self.lba,
                fut: new_fut
            });
        })
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
        pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn open(uuid: Option<Uuid>, combined: Vec<(VdevBlock, LabelReader)>)
            -> (Self, LabelReader);
        pub fn open_zone(&self, start: LbaT) -> BoxVdevFut;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        pub fn status(&self) -> Status;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
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
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {
    use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
    use divbuf::DivBufShared;
    use futures::{FutureExt, future};
    use itertools::Itertools;
    use mockall::predicate::*;
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.erase_zone(3, 31).now_or_never().unwrap().unwrap();
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.finish_zone(3, 31).now_or_never().unwrap().unwrap();
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(uuid, children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let r = mirror.read_at(buf, 3).now_or_never().unwrap();
            assert!(r == Err(Error::EIO) || r == Err(Error::ENXIO));
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.readv_at(sglist, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn degraded() {
            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let buf = dbs.try_mut().unwrap();
            let sglist = vec![buf];
            let total_reads = Arc::new(AtomicU32::new(0));

            let bd0 = mock(1, Ok(()), total_reads.clone());
            let children = vec![
                Child::present(bd0),
                Child::missing(Uuid::new_v4())
            ];
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            mirror.readv_at(sglist, 3).now_or_never().unwrap().unwrap();
            assert_eq!(total_reads.load(Ordering::Relaxed), 1);
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.open_zone(0).now_or_never().unwrap().unwrap();
            let r = mirror.readv_at(sglist, 3).now_or_never().unwrap();
            assert!(r == Err(Error::EIO) || r == Err(Error::ENXIO));
            assert_eq!(total_reads.load(Ordering::Relaxed), 2);
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
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
            let mirror = Mirror::new(Uuid::new_v4(), vec![bd0, bd1].into());
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
            let mirror = Mirror::new(Uuid::new_v4(), children.into());
            mirror.write_spacemap(sgl, 1, 2).now_or_never().unwrap().unwrap();
        }
    }
}
// LCOV_EXCL_STOP
