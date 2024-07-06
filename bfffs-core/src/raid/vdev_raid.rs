// vim: tw=80

use async_trait::async_trait;
use crate::{
    label::*,
    types::*,
    util::*,
    vdev::*,
    mirror
};
use cfg_if::cfg_if;
use divbuf::{DivBuf, DivBufInaccessible, DivBufMut, DivBufShared};
use fixedbitset::FixedBitSet;
use futures::{
    Future,
    FutureExt,
    StreamExt,
    TryFutureExt,
    TryStreamExt,
    future,
    stream::{FuturesOrdered, FuturesUnordered},
    task::{Context, Poll}
};
use itertools::{Itertools, multizip};
use mockall_double::double;
use pin_project::pin_project;
use std::{
    collections::BTreeMap,
    cmp,
    fmt,
    mem,
    num::{NonZeroU8, NonZeroU64},
    pin::Pin,
    ptr,
    sync::{Arc, RwLock}
};
use serde_derive::{Deserialize, Serialize};
use super::{
    LocatorImpl,
    Status,
    codec::*,
    declust::*,
    prime_s::*,
    sgcursor::*,
    vdev_raid_api::*,
};

#[double]
use crate::mirror::Mirror;

#[cfg(test)]
mod tests;

/// RAID placement algorithm.
///
/// This algorithm maps RAID chunks to specific disks and offsets.  It does not
/// encode or decode parity.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum LayoutAlgorithm {
    /// A good declustered algorithm for any prime number of disks
    PrimeS,
}

impl fmt::Display for LayoutAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimeS => "PrimeS".fmt(f)
        }
    }
}

/// In-memory cache of data that has not yet been flushed the Mirror devices.
///
/// Typically there will be one of these for each open zone.
struct StripeBuffer {
    /// Cache of `IoVec`s that haven't yet been flushed
    buf: SGList,

    /// The LBA of the beginning of the cached stripe
    lba: LbaT,

    /// Amount of data in a full stripe, in bytes
    stripesize: usize,
}

impl StripeBuffer {
    /// Store more data into this `StripeBuffer`.
    ///
    /// Don't overflow one row.  Do zero-pad up to the next full LBA.  Return
    /// the unused part of the `IoVec`
    pub fn fill(&mut self, mut iovec: IoVec) -> IoVec {
        let want_bytes = self.stripesize - self.len();
        let have_bytes = iovec.len();
        let get_bytes = cmp::min(want_bytes, have_bytes);
        if get_bytes > 0 {
            self.buf.push(iovec.split_to(get_bytes));
            let partial = get_bytes % BYTES_PER_LBA;
            if partial != 0 {
                debug_assert_eq!(iovec.len(), 0);
                let remainder = BYTES_PER_LBA - partial;
                let zbuf = ZERO_REGION.try_const().unwrap().slice_to(remainder);
                self.buf.push(zbuf);
            }
        }
        iovec
    }

    /// Is the stripe buffer full?
    pub fn is_full(&self) -> bool {
        debug_assert!(self.len() <= self.stripesize);
        self.len() == self.stripesize
    }

    /// The usual `is_empty` function
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The LBA of the beginning of the stripe
    pub fn lba(&self) -> LbaT {
        self.lba
    }

    /// Number of bytes worth of data contained in the buffer
    fn len(&self) -> usize {
        let bytes: usize = self.buf.iter().map(DivBuf::len).sum();
        bytes
    }

    pub fn new(lba: LbaT, stripe_lbas: LbaT) -> Self {
        assert!(lba % stripe_lbas == 0,
                "Can't create a non-stripe-aligned StripeBuffer at lba {lba:?}"
        );
        let stripesize = stripe_lbas as usize * BYTES_PER_LBA;
        StripeBuffer{ buf: SGList::new(), lba, stripesize}
    }

    /// Return the value of the next LBA should be written into this buffer
    pub fn next_lba(&self) -> LbaT {
        debug_assert_eq!(self.len() % BYTES_PER_LBA, 0);
        self.lba + (self.len() / BYTES_PER_LBA) as LbaT
    }

    /// Fill the `StripeBuffer` with zeros and return the number of LBAs worth
    /// of padding.
    pub fn pad(&mut self) -> LbaT {
        let padlen = self.stripesize - self.len();
        let zero_region_len = ZERO_REGION.len();
        let zero_bufs = div_roundup(padlen, zero_region_len);
        for _ in 0..(zero_bufs - 1) {
            self.fill(ZERO_REGION.try_const().unwrap());
        }
        self.fill(ZERO_REGION.try_const().unwrap().slice_to(
                padlen - (zero_bufs - 1) * zero_region_len));
        debug_assert!(self.is_full());
        debug_assert_eq!(padlen % BYTES_PER_LBA, 0);
        (padlen / BYTES_PER_LBA) as LbaT
    }

    /// Get a reference to the data contained by the `StripeBuffer`
    ///
    /// This can be used to read out the currently buffered data
    pub fn peek(&self) -> &SGList {
        &self.buf
    }

    /// Extract all data from the `StripeBuffer`
    pub fn pop(&mut self) -> SGList {
        let new_sglist = SGList::new();
        self.lba = self.next_lba();
        mem::replace(&mut self.buf, new_sglist)
    }

    /// Reset an empty `StripeBuffer` to point to a new stripe.
    ///
    /// It is illegal to call this method on a non-empty `StripeBuffer`
    pub fn reset(&mut self, lba: LbaT) {
        assert!(self.is_empty(), "A StripeBuffer with data cannot be moved");
        self.lba = lba;
    }
}

/// A child of a VdevRaid.  Probably either a Mirror or a missing disk.
enum Child {
    Present(Mirror),
    Missing(Uuid),
    /// Administratively faulted
    Faulted(Mirror)
}

cfg_if! {
    if #[cfg(test)] {
        type ChildReadSpacemap = BoxVdevFut;
    } else {
        type ChildReadSpacemap = mirror::ReadSpacemap;
    }
}

impl Child {
    fn faulted(vdev: Mirror) -> Self {
        Child::Faulted(vdev)
    }

    fn present(vdev: Mirror) -> Self {
        Child::Present(vdev)
    }

    fn missing(uuid: Uuid) -> Self {
        Child::Missing(uuid)
    }

    fn as_present(&self) -> Option<&Mirror> {
        if let Child::Present(vb) = self {
            Some(vb)
        } else {
            None
        }
    }

    fn as_present_mut(&mut self) -> Option<&mut Mirror> {
        if let Child::Present(vb) = self {
            Some(vb)
        } else {
            None
        }
    }

    fn erase_zone(&self, start: LbaT, end: LbaT) -> Option<BoxVdevFut> {
        self.as_present().map(|m| m.erase_zone(start, end))
    }

    /// Mark this child as faulted.  If its status changed from Present to
    /// Faulted, return 1.
    fn fault(&mut self) -> i16 {
        let old = mem::replace(self, Child::Missing(Uuid::default()));
        let r = if matches!(old, Child::Present(_)) {
            1
        } else {
            0
        };
        *self = match old {
            Child::Present(m) => Child::Faulted(m),
            Child::Faulted(m) => Child::Faulted(m),
            Child::Missing(uuid) => Child::Missing(uuid),
        };
        r
    }

    fn finish_zone(&self, start: LbaT, end: LbaT) -> Option<BoxVdevFut> {
        self.as_present().map(|m| m.finish_zone(start, end))
    }

    fn is_present(&self) -> bool {
        matches!(self, Child::Present(_))
    }

    fn open_zone(&self, start: LbaT) -> BoxVdevFut {
        self.as_present().unwrap().open_zone(start)
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        if let Child::Present(c) = self {
            Box::pin(c.read_at(buf, lba)) as BoxVdevFut
        } else {
            Box::pin(future::err(Error::ENXIO)) as BoxVdevFut
        }
    }

    fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> ChildReadSpacemap {
        self.as_present().unwrap().read_spacemap(buf, smidx)
    }

    fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut {
        if let Child::Present(c) = self {
            Box::pin(c.readv_at(bufs, lba)) as BoxVdevFut
        } else {
            Box::pin(future::err(Error::ENXIO)) as BoxVdevFut
        }
    }

    fn status(&self) -> Option<mirror::Status> {
        match self {
            Child::Present(m) => Some(m.status()),
            Child::Faulted(m) => {
                let mut status = m.status();
                status.health = Health::Faulted(FaultedReason::User);
                Some(status)
            },
            Child::Missing(_) => None
        }
    }


    fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut {
        if let Child::Present(c) = self {
            Box::pin(c.write_at(buf, lba)) as BoxVdevFut
        } else {
            Box::pin(future::err(Error::ENXIO)) as BoxVdevFut
        }
    }

    fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut {
        self.as_present().unwrap().write_label(labeller)
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  BoxVdevFut
    {
        self.as_present().unwrap().write_spacemap(sglist, idx, block)
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut {
        if let Child::Present(c) = self {
            Box::pin(c.writev_at(bufs, lba)) as BoxVdevFut
        } else {
            Box::pin(future::err(Error::ENXIO)) as BoxVdevFut
        }
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.as_present().unwrap().lba2zone(lba)
    }

    fn optimum_queue_depth(&self) -> Option<u32> {
        self.as_present().map(|m| m.optimum_queue_depth())
    }

    fn size(&self) -> Option<LbaT> {
        self.as_present().map(|m| m.size())
    }

    fn sync_all(&self) -> Option<BoxVdevFut> {
        self.as_present().map(|m| m.sync_all())
    }

    fn uuid(&self) -> Uuid {
        match self {
            Child::Present(vb) => vb.uuid(),
            Child::Faulted(vb) => vb.uuid(),
            Child::Missing(uuid) => *uuid,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    pub uuid:           Uuid,
    chunksize:          LbaT,
    disks_per_stripe:   i16,
    redundancy:         i16,
    layout_algorithm:   LayoutAlgorithm,
    pub children:       Vec<Uuid>
}

/// Future type of [`VdevRaid::read_spacemap`]
#[pin_project]
pub struct ReadSpacemap {
    inner: Arc<Inner>,
    /// Index of the next child read from
    next: usize,
    dbi: DivBufInaccessible,
    /// Spacemap index to read
    smidx: u32,
    #[cfg(not(test))]
    #[pin]
    fut: Option<crate::mirror::ReadSpacemap>,
    #[cfg(test)]
    #[pin]
    fut: Option<BoxVdevFut>,
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
                    if *pinned.next >= pinned.inner.children.len() {
                        // Out of children.  Fail the Read.
                        return Poll::Ready(Err(e));
                    }
                }
            }
            // Try a different child.
            let buf = pinned.dbi.try_mut().unwrap();
            let child = &pinned.inner.children[*pinned.next];
            *pinned.next += 1;
            let new_fut = child.read_spacemap(buf, *pinned.smidx);
            pinned.fut.replace(new_fut);
        }
    }
}

/// Parts of the [`VdevRaid`] structure that must be Arc'd
struct Inner {
    /// Locator, declustering or otherwise
    locator: LocatorImpl,

    /// Underlying mirror devices.  Order is important!
    children: Box<[Child]>,

    /// RAID codec
    codec: Codec,
}

/// `VdevRaid`: Virtual Device for the RAID transform
///
/// This Vdev implements the RAID I/O path, for all types of RAID encodings and
/// layout algorithms.
pub struct VdevRaid {
    /// Size of RAID chunks in LBAs
    ///
    /// A chunk, aka stripe unit, is the amount of data that will be written in
    /// a contiguous stretch to one disk before the `VdevRaid` switches to the
    /// next disk
    chunksize: LbaT,

    /// Cache of the number of faulted children.
    faulted_children: i16,

    /// RAID placement algorithm.
    layout_algorithm: LayoutAlgorithm,

    /// Best number of queued commands for the whole `VdevRaid`
    optimum_queue_depth: u32,

    /// Usable size of the vdev.
    size: LbaT,

    /// In memory cache of data that has not yet been flushed to the mirror
    /// devices.
    ///
    /// We cache up to one stripe per zone before flushing it.  Cacheing entire
    /// stripes uses fewer resources than only cacheing the parity information.
    stripe_buffers: RwLock<BTreeMap<ZoneT, StripeBuffer>>,

    uuid: Uuid,

    inner: Arc<Inner>,
}

/// Convenience macro for `VdevRaid` I/O methods
///
/// # Examples
///
/// ```ignore
/// let v = Vec::<IoVec>::with_capacity(4);
/// let lba = 0;
/// let fut = issue_1stripe_writes!(self, v, lba, false, write_at);
/// ```
macro_rules! issue_1stripe_writes {
    ( $self:ident, $buf:expr, $lba:expr, $parity:expr, $func:ident) => {
        {
            let (start, end) = if $parity {
                let m = $self.inner.codec.stripesize() -
                    $self.inner.codec.protection();
              (ChunkId::Parity($lba / $self.chunksize, 0),
                 ChunkId::Data($lba / $self.chunksize + m as u64))
            } else {
                (ChunkId::Data($lba / $self.chunksize),
                 ChunkId::Parity($lba / $self.chunksize, 0))
            };
            let mut iter = $self.inner.locator.iter(start, end);
            let mut first = true;
            $buf.into_iter()
            .map(|d| {
                let (_, loc) = iter.next().unwrap();
                let disk_lba = if first {
                    first = false;
                    // The op may begin mid-chunk
                    let chunk_offset = $lba % $self.chunksize;
                    loc.offset * $self.chunksize + chunk_offset
                } else {
                    loc.offset * $self.chunksize
                };
                $self.inner.children[loc.disk as usize].$func(d, disk_lba)
            }).collect::<FuturesOrdered<_>>()
            .collect::<Vec<Result<()>>>()
        }
    }
}

impl VdevRaid {
    const DEFAULT_CHUNKSIZE: LbaT = 16;

    /// Choose the best declustering layout for the requirements given.
    fn choose_layout(num_disks: i16, _disks_per_stripe: i16, _redundancy: i16,
                     chunksize: Option<NonZeroU64>)
        -> (LayoutAlgorithm, LbaT)
    {
        debug_assert!(num_disks > 1);
        let chunksize = chunksize.map(NonZeroU64::get)
            .unwrap_or(VdevRaid::DEFAULT_CHUNKSIZE);
        (LayoutAlgorithm::PrimeS, chunksize)
    }

    /// Create a new VdevRaid from unused files or devices
    ///
    /// * `chunksize`:          RAID chunksize in LBAs, if specified.  This is
    ///                         the largest amount of data that will be
    ///                         read/written to a single device before the
    ///                         `Locator` switches to the next device.
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than
    ///                         or equal to the number of disks in `paths`.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `mirrors`:            Already labeled Mirror devices
    // Hide from docs.  The public API should just be raid::create, but this
    // function technically needs to be public for testing purposes.
    #[doc(hidden)]
    pub fn create(chunksize: Option<NonZeroU64>, disks_per_stripe: i16,
        redundancy: i16, mirrors: Vec<Mirror>)
        -> Self
    {
        let num_disks = mirrors.len() as i16;
        let (layout, chunksize) = VdevRaid::choose_layout(num_disks,
            disks_per_stripe, redundancy, chunksize);
        let uuid = Uuid::new_v4();
        let children = mirrors.into_iter()
            .map(Child::present)
            .collect();
        VdevRaid::new(chunksize, disks_per_stripe, redundancy, uuid,
                      layout, children)
    }

    fn faulted_children(&self) -> i16 {
        debug_assert_eq!(self.faulted_children as usize,
                         self.inner.children.iter()
                         .filter(|c| !c.is_present())
                         .count());
        self.faulted_children
    }

    /// Return a reference to the first child that isn't missing
    fn first_healthy_child(&self) -> &Mirror {
        for c in self.inner.children.iter() {
            if let Some(m) = c.as_present() {
                return m;
            }
        }
        panic!("No children at all?");
    }

    fn new(chunksize: LbaT,
           disks_per_stripe: i16,
           redundancy: i16,
           uuid: Uuid,
           layout_algorithm: LayoutAlgorithm,
           children: Box<[Child]>) -> Self
    {
        let mut faulted_children = 0;
        let num_disks = children.len() as i16;
        let codec = Codec::new(disks_per_stripe as u32, redundancy as u32);
        let locator: LocatorImpl = match layout_algorithm {
            LayoutAlgorithm::PrimeS => 
                PrimeS::new(num_disks, disks_per_stripe, redundancy).into()
        };

        // XXX The vdev size or child size should be recorded in the label.
        // Otherwise it could change when a pool is imported with missing
        // devices.
        let child_size = children.iter()
            .filter_map(Child::size)
            .min()
            .unwrap();
        let zl0 = children.iter()
            .filter_map(Child::as_present)
            .map(|m| m.zone_limits(0))
            .next()
            .unwrap();

        for child in children.iter() {
            if let Some(m) = child.as_present() {
                // All children must be the same size
                assert_eq!(child_size, m.size());

                // All children must have the same zone boundaries
                // XXX this check assumes fixed-size zones
                assert_eq!(zl0, m.zone_limits(0));
            } else {
                faulted_children += 1;
            }
        }

        // NB: the optimum queue depth should actually be a little higher for
        // healthy reads than for writes or degraded reads.  This calculation
        // computes the optimum for writes and degraded reads.
        let optimum_queue_depth = children.iter()
        .filter_map(Child::optimum_queue_depth)
        .sum::<u32>() / (codec.stripesize() as u32);

        let disk_size_in_chunks = child_size / chunksize;
        let size = disk_size_in_chunks * locator.datachunks() *
            chunksize / LbaT::from(locator.depth());

        let inner = Arc::new(Inner {
            locator,
            children,
            codec
        });

        VdevRaid {
            chunksize,
            layout_algorithm,
            faulted_children,
            optimum_queue_depth,
            size,
            stripe_buffers: RwLock::new(BTreeMap::new()),
            uuid,
            inner
        }
    }

    /// Open an existing `VdevRaid` from its component devices
    ///
    /// # Parameters
    ///
    /// * `label`:      The `VdevRaid`'s label, taken from any child.
    /// * `mirrors`:    A map of all the present children `Mirror`s, indexed by
    ///                 UUID.
    pub(super) fn open(label: Label, mut mirrors: BTreeMap<Uuid, Mirror>)
        -> Self
    {
        let children = label.children.iter().map(|uuid| {
            if let Some(m) = mirrors.remove(uuid) {
                if m.status().health.is_faulted() {
                    Child::faulted(m)
                } else {
                    Child::present(m)
                }
            } else {
                Child::missing(*uuid)
            }
        }).collect::<Vec<_>>();
        assert!(children.len() as i16 >= label.disks_per_stripe - label.redundancy,
            "Insufficient RAID members");
        VdevRaid::new(label.chunksize,
                      label.disks_per_stripe,
                      label.redundancy,
                      label.uuid,
                      label.layout_algorithm,
                      children.into_boxed_slice())
    }

    /// Asynchronously open a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:              The target zone ID
    /// - `already_allocated`: The amount of data that was previously allocated
    ///                        in this zone, if the zone is being reopened.
    // Create a new StripeBuffer, and zero fill leading wasted space
    fn open_zone_priv(&self, zone: ZoneT, already_allocated: LbaT) -> BoxVdevFut
    {
        let f = self.inner.codec.protection();
        let m = (self.inner.codec.stripesize() - f) as LbaT;
        let stripe_lbas = m * self.chunksize as LbaT;
        let (start_lba, _) = self.zone_limits(zone);
        let sb = StripeBuffer::new(start_lba + already_allocated, stripe_lbas);
        assert!(self.stripe_buffers.write().unwrap().insert(zone, sb).is_none());

        let (first_disk_lba, _) = self.first_healthy_child().zone_limits(zone);
        let start_disk_chunk = div_roundup(first_disk_lba, self.chunksize);
        let futs = FuturesUnordered::<BoxVdevFut>::new();
        for (idx, mirrordev) in self.inner.children.iter().enumerate() {
            if !mirrordev.is_present() {
                continue;
            }

            // Find the first LBA of this disk that's within our zone
            let mut first_usable_disk_lba = 0;
            for chunk in start_disk_chunk.. {
                let loc = Chunkloc::new(idx as i16, chunk);
                let chunk_id = self.inner.locator.loc2id(loc);
                let chunk_lba = chunk_id.address() * self.chunksize;
                if chunk_lba >= start_lba {
                    first_usable_disk_lba = chunk * self.chunksize;
                    break;
                }
            }
            futs.push(Box::pin(mirrordev.open_zone(first_disk_lba)));
            if first_usable_disk_lba > first_disk_lba && already_allocated == 0
            {
                // Zero-fill leading wasted space so as not to cause a
                // write pointer violation on SMR disks.
                let zero_lbas = first_usable_disk_lba - first_disk_lba;
                let zero_len = zero_lbas as usize * BYTES_PER_LBA;
                let sglist = zero_sglist(zero_len);
                futs.push(Box::pin(
                    mirrordev.writev_at(sglist, first_disk_lba)) as BoxVdevFut
                );
            }
        }

        Box::pin(
            futs.try_collect::<Vec<_>>().map_ok(drop)
        )
    }

    /// Read more than one whole stripe.  Optimized for reading many stripes.
    // Assemble a vectorized read operation for each mirror child that includes
    // all data chunks needed to reassemble the requested LBA range.  But
    // skipping a Chunk would require issueing additional IOPs to the disk, and
    // that slows down disk firmware.  So even though only Data chunks are
    // needed, we will also read Parity chunks if they're bookended by Data
    // chunks.  And we just might need them for reconstruction, if there's an
    // EIO.
    fn read_at_multi(
        &self,
        mut buf: IoVecMut,
        // First LBA the user wants to read
        ulba: LbaT
    ) -> impl Future<Output=Result<()>> + Send + Sync
    {
        const SENTINEL : LbaT = LbaT::MAX;

        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let n = self.inner.children.len();
        let f = self.inner.codec.protection();
        let k = self.inner.codec.stripesize();
        let chunksize = self.chunksize;
        let inner = self.inner.clone();
        let m = k - f;
        let lbas_per_stripe = chunksize * m as LbaT;
        let faulted_children = self.faulted_children();

        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);
        let unlbas = (buf.len() / BYTES_PER_LBA) as LbaT;
        let uchunks = div_roundup(buf.len(), col_len);
        let start_stripe = ulba / (chunksize * m as LbaT);
        // First LBA that the user does not want to read
        let uelba = ulba + unlbas;
        // The last stripe that the user wants to read from
        let last_stripe = (uelba - 1) / (chunksize * m as LbaT);
        // The number of stripes to read, at least partially
        let nstripes = (last_stripe - start_stripe + 1) as usize;

        let stripe_lba = ulba - ulba % lbas_per_stripe;
        // First LBA that we'll read from disk
        let rlba = if faulted_children > 0 {
            stripe_lba
        } else {
            ulba
        };

        // Allocate extra buffers for parity and possibly for normal data too.
        // The normal data may be needed for reconstruction during reads of less
        // than a full stripe.  The parity may be needed for reconstruction, and
        // may also be opportunistically read just to reduce disk IOPs.
        let mut exbufs = Vec::with_capacity(nstripes);
        exbufs.resize_with(nstripes, || Vec::with_capacity(f as usize));

        // Store DivBufInaccessible for use during error recovery
        let dbi = buf.clone_inaccessible();

        // Create an SGList for each disk.
        let mut sglists = Vec::<SGListMut>::with_capacity(n);
        // Create additional SGLists to store Parity buffers, if we need them.
        let mut psglists = Vec::<SGListMut>::new();
        psglists.resize_with(n, Default::default);
        // The disk LBAs (not RAID LBAs) that each disk read starts from
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let max_chunks_per_disk = inner.locator.parallel_read_count(uchunks);
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            sglists.push(SGListMut::with_capacity(max_chunks_per_disk));
        }
        // Build the SGLists, one chunk at a time
        let start_chunk_addr = rlba / chunksize;
        let start_cid = ChunkId::Data(start_chunk_addr);
        let at_end_of_stripe = uelba % lbas_per_stripe >
            lbas_per_stripe - chunksize;
        let end_cid = if faulted_children > 0 || at_end_of_stripe {
            if faulted_children < f {
                ChunkId::Parity(last_stripe * m as u64, faulted_children)
            } else {
                ChunkId::Data((last_stripe + 1) * m as u64)
            }
        } else {
            ChunkId::Data(div_roundup(uelba, chunksize))
        };
        let mut starting = true;
        for (cid, loc) in inner.locator.iter(start_cid, end_cid) {
            let mut disk_lba = loc.offset * chunksize;
            let disk = loc.disk as usize;
            let cid_lba = cid.address() * chunksize;
            let cid_end_lba = cid_lba + chunksize;
            let stripe = cid.address().saturating_sub(start_chunk_addr) as usize / m as usize;
            match cid {
                ChunkId::Data(_did) => {
                    let col = if cid_end_lba <= ulba {
                        // This chunk is only needed for parity reconstruction
                        debug_assert!(faulted_children > 0);
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs[stripe].push(dbs);
                        dbm
                    } else if cid_lba < ulba && faulted_children > 0 {
                        // This chunk is needed partly by the caller and partly
                        // for parity reconstruction.
                        debug_assert!(psglists[disk].is_empty());
                        let chunk_offset = ulba % chunksize;
                        let datalbas = chunksize - chunk_offset;
                        let datasize = cmp::min(datalbas as usize * BYTES_PER_LBA,
                                                  buf.len());
                        let exsize = col_len - datasize;
                        let datacol = buf.split_to(datasize);
                        let dbs = DivBufShared::uninitialized(exsize);
                        let excol = dbs.try_mut().unwrap();
                        if start_lbas[disk] == SENTINEL {
                            start_lbas[disk] = disk_lba;
                        }
                        exbufs[stripe].push(dbs);
                        sglists[disk].append(&mut psglists[disk]);
                        sglists[disk].push(excol);
                        datacol
                    } else if cid_lba < ulba {
                        // Part of this chunk is needed by the caller
                        debug_assert!(starting);
                        let chunk_offset = ulba % chunksize;
                        let chunk0lbas = chunksize - chunk_offset;
                        let chunk0size = cmp::min(chunk0lbas as usize * BYTES_PER_LBA,
                                                  buf.len());
                        let col0 = buf.split_to(chunk0size);
                        disk_lba += chunk_offset;
                        col0
                    } else if cid_end_lba <= uelba {
                        // This chunk is needed by the caller
                        buf.split_to(col_len)
                    } else if cid_lba < uelba && faulted_children == 0 {
                        // Part of this chunk is needed by the caller
                        // TODO: add a DivBufMut::take method
                        buf.split_to(buf.len())
                    } else if cid_lba < uelba {
                        // This chunk is needed partly by the caller and partly
                        // for parity reconstruction.
                        let exlen = col_len - buf.len();
                        let datacol = buf.split_to(buf.len());
                        let dbs = DivBufShared::uninitialized(exlen);
                        let excol = dbs.try_mut().unwrap();
                        exbufs[stripe].push(dbs);
                        if start_lbas[disk] == SENTINEL {
                            start_lbas[disk] = disk_lba;
                        }
                        sglists[disk].append(&mut psglists[disk]);
                        sglists[disk].push(datacol);
                        excol
                    } else {
                        debug_assert!(cid_lba >= uelba);
                        // This chunk is only needed for parity reconstruction
                        debug_assert!(faulted_children > 0);
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs[stripe].push(dbs);
                        dbm
                    };
                    // Read any parity chunks that came before this data one
                    sglists[disk].append(&mut psglists[disk]);
                    sglists[disk].push(col);
                    if start_lbas[disk] == SENTINEL {
                        start_lbas[disk] = disk_lba;
                    }
                    starting = false;
                }
                ChunkId::Parity(_did, pid) => {
                    if faulted_children > pid {
                        // Read this parity chunk for reconstruction
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs[stripe].push(dbs);
                        sglists[disk].append(&mut psglists[disk]);
                        sglists[disk].push(dbm);
                        if start_lbas[disk] == SENTINEL {
                            start_lbas[disk] = disk_lba;
                        }
                    } else if !sglists[disk].is_empty() {
                        // Read this parity chunk if we're going to read data
                        // chunks to either side of it anyway.
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs[stripe].push(dbs);
                        psglists[disk].push(dbm);
                    } else {
                        // Skip this uneeded chunk.
                    }
                }
            }
        }

        let rfut = multizip((inner.children.iter(),
                              sglists.into_iter(),
                              start_lbas.into_iter()))
        .map(|(mirrordev, sglist, lba)| {
            if lba != SENTINEL {
                Box::pin(mirrordev.readv_at(sglist, lba)) as BoxVdevFut
            } else {
                Box::pin(future::ok(())) as BoxVdevFut
            }
        })
        // TODO: on error, record error statistics, possibly fault a drive,
        // and request the faulty drive's zone to be rebuilt.
        .collect::<FuturesOrdered::<_>>();

        async move {
            let dv = rfut.collect::<Vec<_>>().await;

            // TODO: on error, issue extra parity reads.

            if dv.iter().all(Result::is_ok) {
                Ok(())
            } else {
                // Split the range into single stripes and perform error
                // recovery on each.  It isn't the most efficient, but this
                // should be a rare case.
                drop(buf);
                let mut rbuf = dbi.try_mut().unwrap();
                let lbas_per_stripe = m as LbaT * chunksize as LbaT;
                let mut exbufs = exbufs.into_iter();
                (start_stripe..=last_stripe).map(move |s| {
                    // Start of user data within this stripe
                    let usslba = ulba.max(s * lbas_per_stripe);
                    // End of user data within this stripe
                    let uselba = uelba.min((s + 1) * lbas_per_stripe);
                    let sbytes = (uselba - usslba) as usize * BYTES_PER_LBA;
                    let data = rbuf.split_to(sbytes);
                    let exbuf = exbufs.next().unwrap();
                    let start_cid = ChunkId::Data(s * m as u64);
                    let end_cid = if faulted_children < f {
                        ChunkId::Parity(s * m as u64, faulted_children)
                    } else {
                        ChunkId::Data((s + 1) * m as u64)
                    };
                    let lociter = inner.locator.iter(start_cid, end_cid);
                    let dvs = lociter
                        .map(|(_cid, loc)| dv[loc.disk as usize])
                        .collect::<Vec<_>>();
                    Self::read_at_reconstruction(&inner.codec, chunksize, data,
                                           usslba, exbuf, dvs)
                }).reduce(|acc, e| acc.and(e))
                .unwrap()
            }
        }
    }

    /// Read a (possibly improper) subset of one stripe
    fn read_at_one(
        &self,
        mut buf: IoVecMut,
        ulba: LbaT
    ) -> impl Future<Output=Result<()>> + Send + Sync
    {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let chunksize = self.chunksize;
        let f = self.inner.codec.protection();
        let m = (self.inner.codec.stripesize() - f) as usize;
        let inner = self.inner.clone();
        let lbas_per_stripe = chunksize * m as LbaT;
        let dbi = buf.clone_inaccessible();
        let faulted_children = self.faulted_children();

        let unlbas = (buf.len() / BYTES_PER_LBA) as LbaT;
        // First LBA that the user does not want to read
        let uelba = ulba + unlbas;
        let stripe_lba = ulba - ulba % lbas_per_stripe;
        let end_of_stripe_lba = stripe_lba + lbas_per_stripe;
        let (start_lba, end_lba) = if faulted_children > 0 {
            (stripe_lba, end_of_stripe_lba)
        } else {
            (ulba, ulba + (buf.len() / BYTES_PER_LBA) as LbaT)
        };

        // If the array is degraded, allocate extra buffers for parity and
        // possibly for normal data too.  The normal data may be needed for
        // reconstruction during reads of less than a full stripe.
        let mut exbufs = if faulted_children > 0 {
            Some(Vec::new())
        } else {
            None
        };

        let mut start_chunk_addr = start_lba / chunksize;
        let start_cid = ChunkId::Data(start_chunk_addr);
        let end_lba_remainder = end_lba % lbas_per_stripe;
        let end_cid = if faulted_children > 0 && faulted_children < f {
            ChunkId::Parity(stripe_lba / chunksize, faulted_children)
        } else if faulted_children > 0 {
            ChunkId::Data(end_lba / chunksize)
        } else if end_lba_remainder == 0 || (lbas_per_stripe - end_lba_remainder) < chunksize {
            ChunkId::Parity(stripe_lba / chunksize, 0)
        } else {
            ChunkId::Data(div_roundup(end_lba, chunksize))
        };

        let mut first = true;
        let rfut = inner.locator.iter(start_cid, end_cid)
        .map(|(cid, loc)| {
            let cid_lba = cid.address() * chunksize;
            let cid_end_lba = cid_lba + chunksize;
            let mut disk_lba = loc.offset * chunksize;
            if !cid.is_data() || cid_end_lba <= ulba {
                // This is either a parity chunk or a chunk before the start of
                // the data that the user wants to read.  In either case, it's
                // needed only for parity reconstruction.
                debug_assert!(faulted_children > 0);
                let dbs = DivBufShared::uninitialized(col_len);
                let dbm = dbs.try_mut().unwrap();
                exbufs.as_mut().unwrap().push(dbs);
                inner.children[loc.disk as usize].read_at(dbm, disk_lba)
            } else if cid_lba < ulba && faulted_children > 0 {
                // This chunk is needed partly by the caller and partly for
                // parity reconstruction
                let chunk_offset = ulba % chunksize;
                let datalbas = chunksize - chunk_offset;
                let datasize = cmp::min(datalbas as usize * BYTES_PER_LBA,
                                        buf.len());
                let exsize0 = (ulba - cid_lba) as usize * BYTES_PER_LBA;
                let datacol = buf.split_to(datasize);
                let dbs = DivBufShared::uninitialized(exsize0);
                let excol = dbs.try_mut().unwrap();
                exbufs.as_mut().unwrap().push(dbs);
                let mut sglist = vec![excol, datacol];
                let exsize1 = col_len - datasize - exsize0;
                if exsize1 > 0 {
                    let dbs = DivBufShared::uninitialized(exsize1);
                    let excol = dbs.try_mut().unwrap();
                    exbufs.as_mut().unwrap().push(dbs);
                    sglist.push(excol);
                }
                inner.children[loc.disk as usize].readv_at(sglist, disk_lba)
            } else if cid_lba < ulba {
                // Part of this chunk is needed by the caller
                debug_assert!(first);
                first = false;
                let chunk_offset = ulba % chunksize;
                let chunk0lbas = chunksize - chunk_offset;
                let chunk0size = cmp::min(chunk0lbas as usize * BYTES_PER_LBA,
                                          buf.len());
                let col0 = buf.split_to(chunk0size);
                disk_lba += chunk_offset;
                inner.children[loc.disk as usize].read_at(col0, disk_lba)
            } else if cid_end_lba <= uelba {
                // This chunk is needed by the caller
                let dbm = buf.split_to(col_len.min(buf.len()));
                inner.children[loc.disk as usize].read_at(dbm, disk_lba)
            } else if cid_lba < uelba && faulted_children == 0 {
                // Part of this chunk is needed by the caller
                // TODO: add a DivBufMut::take method
                let dbm = buf.split_to(buf.len());
                inner.children[loc.disk as usize].read_at(dbm, disk_lba)
            } else if cid_lba < uelba {
                // This chunk is needed partly by the caller and partly for
                // parity reconstruction
                let exlen = col_len - buf.len();
                let datacol = buf.split_to(buf.len());
                let dbs = DivBufShared::uninitialized(exlen);
                let excol = dbs.try_mut().unwrap();
                exbufs.as_mut().unwrap().push(dbs);
                let sglist = vec![datacol, excol];
                inner.children[loc.disk as usize].readv_at(sglist, disk_lba)
            } else {
                debug_assert!(cid_lba >= uelba);
                // This chunk is only needed for parity reconstruction
                debug_assert!(faulted_children > 0);
                let dbs = DivBufShared::uninitialized(col_len);
                let dbm = dbs.try_mut().unwrap();
                exbufs.as_mut().unwrap().push(dbs);
                inner.children[loc.disk as usize].read_at(dbm, disk_lba)
            }
        }).collect::<FuturesOrdered<_>>();

        async move {
            let mut nerrs;
            let mut dv = rfut.collect::<Vec<Result<()>>>().await;

            let mut old_nerrs = faulted_children;
            loop {
                nerrs = dv.iter().filter(|x| x.is_err()).count() as i16;
                if nerrs <= old_nerrs {
                    break;
                }
                debug_assert!(dv.len() - (nerrs as usize) < m);
                // Some disk must've had an unexpected error.  Read enough
                // parity for reconstruction.
                if nerrs > f {
                    // Too many errors.  No point in attempting recovery. :(
                    break
                }
                if exbufs.is_none() {
                    exbufs = Some(Vec::with_capacity(nerrs as usize));
                }
                let start_cid = ChunkId::Data(stripe_lba / chunksize);
                let end_cid = if nerrs > 0 && nerrs < f {
                    ChunkId::Parity(stripe_lba / chunksize, nerrs)
                } else {
                    ChunkId::Data(end_of_stripe_lba / chunksize)
                };
                let rfut = inner.locator.iter(start_cid, end_cid)
                .map(|(cid, loc)| {
                    if let ChunkId::Parity(_, pid) = cid {
                        debug_assert!(pid < nerrs);
                        if pid < old_nerrs {
                            // Already read it
                            let dvidx = m + pid as usize;
                            Box::pin(future::ready(dv[dvidx])) as BoxVdevFut
                        } else {
                            let dbs = DivBufShared::uninitialized(col_len);
                            let dbm = dbs.try_mut().unwrap();
                            exbufs.as_mut().unwrap().push(dbs);
                            let disk_lba = loc.offset * chunksize;
                            Box::pin(inner.children[loc.disk as usize].read_at(dbm, disk_lba)) as BoxVdevFut
                        }
                    } else if cid.address() < start_chunk_addr {
                        // Need to read this data chunk for parity
                        // reconstruction
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs.as_mut().unwrap().push(dbs);
                        let disk_lba = loc.offset * chunksize;
                        Box::pin(inner.children[loc.disk as usize].read_at(dbm, disk_lba)) as BoxVdevFut
                    } else if cid.address() - start_chunk_addr < dv.len() as u64
                    {
                        // should've already read it
                        let dvidx = (cid.address() - start_chunk_addr) as usize;
                        Box::pin(future::ready(dv[dvidx])) as BoxVdevFut
                    } else {
                        // Need to read this data chunk for parity
                        // reconstruction
                        let dbs = DivBufShared::uninitialized(col_len);
                        let dbm = dbs.try_mut().unwrap();
                        exbufs.as_mut().unwrap().push(dbs);
                        let disk_lba = loc.offset * chunksize;
                        Box::pin(inner.children[loc.disk as usize].read_at(dbm, disk_lba)) as BoxVdevFut
                    }
                }).collect::<FuturesOrdered<_>>();
                start_chunk_addr = stripe_lba / chunksize;
                old_nerrs = nerrs;
                dv = rfut.collect::<Vec<Result<()>>>().await;
            }

            if nerrs > f {
                // Reconstruction is impossible :(
                dv.into_iter()
                    .find(Result::is_err)
                    .unwrap()
            } else if nerrs > 0 {
                // We must reconstruct the data
                drop(buf);
                let data = dbi.try_mut().unwrap();
                Self::read_at_reconstruction(&inner.codec, chunksize, data,
                                             ulba, exbufs.unwrap(), dv)
            } else  {
                Ok(())
            }
        }
        // TODO: on error, record error statistics, possibly fault a drive,
        // and request the faulty drive's zone to be rebuilt.
    }

    fn read_long_multi(&self, unlbas: LbaT, ulba: LbaT)
        -> impl Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send
    {
        let f = self.inner.codec.protection();
        let k = self.inner.codec.stripesize();
        let m = self.inner.codec.stripesize() - f;
        let n = self.inner.children.len();
        let chunksize = self.chunksize;
        let inner = self.inner.clone();
        let lbas_per_stripe = chunksize * m as LbaT;
        let start_stripe = ulba / (chunksize * m as LbaT);
        let col_len = chunksize as usize * BYTES_PER_LBA;
        // end_lba is inclusive.  The highest LBA from which data will be read
        let end_lba = ulba + unlbas - 1;
        let end_stripe = end_lba / (chunksize as LbaT * m as LbaT);
        let stripes = end_stripe - start_stripe + 1;

        // Assemble the list of read operations.  Unlink read_at_multi, don't
        // use scatter/gather operations.  Instead, read each disks's data into
        // a single contiguous buffer.  We'll chop it up in the next step.
        let mut buffers = Vec::with_capacity(self.inner.children.len());
        for _ in 0..self.inner.children.len() {
            buffers.push(None);
        }
        let mut args_per_disk = vec![None; self.inner.children.len()];

        let stripe_lba = ulba - ulba % lbas_per_stripe;
        let start_lba = stripe_lba;
        let start_chunk_addr = start_lba / chunksize;
        let start_cid = ChunkId::Data(start_chunk_addr);
        let end_cid = ChunkId::Data(start_chunk_addr + stripes * m as LbaT);
        for (_cid, loc) in inner.locator.iter(start_cid, end_cid) {
            match args_per_disk[loc.disk as usize] {
                None => args_per_disk[loc.disk as usize] =
                    Some((loc.offset, loc.offset + 1)),
                Some((_s, ref mut e)) => *e = loc.offset + 1
            };
        }
        args_per_disk.iter()
        .enumerate()
        .filter_map(|(disk_idx, x)| x.map(|(sofs, eofs)| (disk_idx, sofs, eofs)))
        .map(|(disk_idx, sofs, eofs)| {
            let disk_lba = sofs * chunksize;
            let len = ((eofs - sofs) * chunksize) as usize * BYTES_PER_LBA;
            let dbs = DivBufShared::from(vec![0u8; len]);
            let dbm = dbs.try_mut().unwrap();
            buffers[disk_idx] = Some(dbs);
            self.inner.children[disk_idx].read_at(dbm, disk_lba)
        }).collect::<FuturesOrdered<_>>()
        .collect::<Vec<Result<()>>>()
        .map(move |dv| {
            let mut failures = FixedBitSet::with_capacity(n);
            for (i, v) in dv.iter().enumerate() {
                if v.is_err() {
                    failures.set(i, true);
                }
            }
            // Now iterate over all combinations of disk failures
            let iterator = (0..n).combinations(f as usize)
            .filter_map(move |it| {
                let mut disk_erasures = FixedBitSet::with_capacity(n);
                for i in it {
                    disk_erasures.set(i, true);
                }
                if failures.difference(&disk_erasures).count() != 0 {
                    return None;
                }
                // Allocate storage for the reconstructed stripes
                let sdbs = DivBufShared::from(
                    vec![0u8; stripes as usize * m as usize * col_len]
                );
                let sdbm = sdbs.try_mut().unwrap();
                let mut sdbmi = sdbm.into_chunks(col_len);

                // Now iterate stripe by stripe, reconstructing the result
                // Allocate storage for the entire stripe.
                for s in start_stripe..=end_stripe {
                    let mut erasures = FixedBitSet::with_capacity(k as usize);
                    let mut surviving = Vec::with_capacity(m as usize);
                    let mut vdbm = Vec::with_capacity(f as usize);
                    let mut vdb = Vec::with_capacity(m as usize);
                    let mut missing = Vec::with_capacity(f as usize);
                    let start_chunk_addr = s * m as LbaT;
                    let past_chunk_addr = (s + 1) * m as LbaT;
                    let start_cid = ChunkId::Data(start_chunk_addr);
                    let end_cid = ChunkId::Data(past_chunk_addr);
                    for (chunkpos, (cid, loc)) in inner.locator
                        .iter(start_cid, end_cid)
                        .enumerate()
                    {
                        let disk = loc.disk as usize;
                        let args = args_per_disk[disk].as_ref().unwrap();
                        if disk_erasures.contains(disk) && cid.is_data() {
                            erasures.set(chunkpos, true);
                            let mut dbm = sdbmi.next().unwrap();
                            missing.push(dbm.as_mut_ptr());
                            vdbm.push(dbm);
                        } else {
                            let dbs = buffers[disk].as_mut().unwrap();
                            let mut db = dbs.try_const().unwrap();
                            db.split_to((loc.offset - args.0) as usize * col_len);
                            db.split_off(col_len);
                            if cid.is_data() {
                                sdbmi.next().unwrap().copy_from_slice(&db[..]);
                            }
                            surviving.push(db.as_ptr());
                            vdb.push(db);
                        }
                        if surviving.len() >= m as usize {
                            break;
                        }
                    }
                    // Safe because all columns are still owned by vdb and vdbm
                    // and they all have the same length.
                    if !missing.is_empty() {
                        unsafe {
                            inner.codec.decode(col_len, &surviving[..],
                                          &mut missing[..], &erasures);
                        }
                    }
                }
                debug_assert!(sdbmi.next().is_none());
                drop(sdbmi);
                // Now copy just the part that the user wants
                let udbs = DivBufShared::from(
                    vec![0u8; unlbas as usize * BYTES_PER_LBA]
                );
                let mut udbm = udbs.try_mut().unwrap();
                let sdb = sdbs.try_const().unwrap();
                let uo = (ulba % lbas_per_stripe) as usize * BYTES_PER_LBA;
                let ul = unlbas as usize * BYTES_PER_LBA;
                udbm.copy_from_slice(&sdb[uo..uo + ul]);

                Some(udbs)
            });
            Ok(Box::new(iterator) as Box<dyn Iterator<Item=DivBufShared> + Send>)
        })
    }

    // NB: In a cluster that uses raid on top of mirrors, this function won't
    // consult the mirrors' read_long methods.  Instead, it assumes that the
    // raid layer itself contains enough redundancy to rebuild the corrupt data.
    // This limitation could be lifted, but I don't expect it to come up very
    // often.
    fn read_long_one(&self, unlbas: LbaT, ulba: LbaT)
        -> impl Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send
    {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let k = self.inner.codec.stripesize() as usize;
        let f = self.inner.codec.protection();
        let m = (self.inner.codec.stripesize() - f) as usize;
        let n = self.inner.children.len();
        let lbas_per_stripe = self.chunksize * m as LbaT;
        let inner = self.inner.clone();

        let stripe_lba = ulba - ulba % lbas_per_stripe;
        let end_of_stripe_lba = stripe_lba + lbas_per_stripe;
        let start_lba = stripe_lba;
        let end_lba = end_of_stripe_lba;

        let mut exbufs = Vec::with_capacity(n);

        let start_chunk_addr = start_lba / self.chunksize;
        let start_cid = ChunkId::Data(start_chunk_addr);
        let end_cid = ChunkId::Data(end_lba / self.chunksize);

        let rfut = self.inner.locator.iter(start_cid, end_cid)
        .map(|(_cid, loc)| {
            let disk_lba = loc.offset * self.chunksize;
            let dbs = DivBufShared::from(vec![0u8; col_len]);
            let dbm = dbs.try_mut().unwrap();
            exbufs.push(dbs);
            self.inner.children[loc.disk as usize].read_at(dbm, disk_lba)
        }).collect::<FuturesOrdered<_>>();
        rfut.collect::<Vec<Result<()>>>()
        .map(move |dv| {
            let mut failures = FixedBitSet::with_capacity(k);
            for (i, v) in dv.iter().enumerate() {
                if v.is_err() {
                    failures.set(i, true);
                }
            }
            let iterator = (0..k).combinations(f as usize).filter_map(move |it| {
                let mut erasures = FixedBitSet::with_capacity(k);
                for i in it {
                    erasures.set(i, true);
                }
                if failures.difference(&erasures).count() != 0 {
                    None
                } else {
                    // Reconstruct the entire stripe.
                    // NB: this could be more efficient if the user only wants a
                    // few LBAs in the middle of a chunk.  In that case we don't
                    // need to reconstruct the entire stripe, just a thin
                    // substripe of it.  But read_long is not optimized for
                    // performance.
                    let dbs = DivBufShared::from(vec![0u8; m * col_len]);
                    let dbm = dbs.try_mut().unwrap();
                    let mut vdb = Vec::with_capacity(m);
                    let mut surviving = Vec::with_capacity(m);
                    let mut vdbm = Vec::with_capacity(f as usize);
                    let mut missing = Vec::with_capacity(f as usize);
                    for (i, mut dbm) in dbm.into_chunks(col_len).enumerate() {
                        if erasures.contains(i) {
                            missing.push(dbm.as_mut_ptr());
                            vdbm.push(dbm);
                        } else if !erasures.contains(i) {
                            debug_assert!(dv[i].is_ok());
                            let db = exbufs[i].try_const().unwrap();
                            dbm[..].copy_from_slice(&db[..]);
                            surviving.push(db.as_ptr());
                            vdb.push(db);
                        }
                    }
                    for i in m..k {
                        if !erasures.contains(i) {
                            debug_assert!(dv[i].is_ok());
                            let db = exbufs[i].try_const().unwrap();
                            surviving.push(db.as_ptr());
                            vdb.push(db);
                        }
                        if surviving.len() >= m {
                            break;
                        }
                    }
                    // Safe because all columns are still owned by vdb and vdbm
                    // and they all have the same length.
                    if !missing.is_empty() {
                        unsafe {
                            inner.codec.decode(col_len, &surviving[..],
                                          &mut missing[..], &erasures);
                        }
                    }
                    drop(vdbm);
                    drop(vdb);
                    if unlbas == lbas_per_stripe {
                        Some(dbs)
                    } else {
                        // Copy just the part that the user wants
                        // NB: this could be more efficient.  The data copy
                        // could be eliminated by using more exbufs for stuff
                        // the user doesn't want, and reading or reconstructing
                        // directly into udbs.  But that would be complicated,
                        // and read_long isn't optimized for performance.
                        let uo = (ulba % lbas_per_stripe) as usize * BYTES_PER_LBA;
                        let ul = unlbas as usize * BYTES_PER_LBA;
                        let udbs = DivBufShared::from(vec![0u8; ul]);
                        let mut udbm = udbs.try_mut().unwrap();
                        udbm.copy_from_slice(&dbs.try_const().unwrap()[uo..uo + ul]);
                        Some(udbs)
                    }
                }
            });
            Ok(Box::new(iterator) as Box<dyn Iterator<Item=DivBufShared> + Send>)
        })
    }

    /// Reconstruct a stripe of data, given surviving columns and parity
    fn read_at_reconstruction(
        codec: &Codec,
        chunksize: LbaT,
        // Handle to the original buffer.  Must live on as a DivBufInaccessible
        // in the caller.
        mut data: DivBufMut,
        // LBA where the original operation began
        lba: LbaT,
        // Buffers of extra chunks needed only for reconstruction.  They are
        // expected to be in the order of their arrangement in the stripe, and
        // exclude successfully read chunks.
        exbufs: Vec<DivBufShared>,
        // The results of the read attempts for each disk in the stripe.
        v: Vec<Result<()>>
    ) -> Result<()>
    {
        let f = codec.protection() as usize;
        let k = codec.stripesize() as usize;
        let m = k - f;
        // How many disks, counting from start of stripe, are needed in order to
        // reconstruct the missing ones?
        let disks_needed = match v.iter()
            .enumerate()
            .filter(|(_i, r)| r.is_ok())
            .nth(m - 1)
            .map(|(i, _r)| i)
        {
            Some(i) => 1 + i,
            None => return *v.iter()
                    .find(|r| r.is_err())
                    .unwrap()
        };

        if disks_needed <= m {
            // On this stripe, no data chunks were missing.
            return Ok(());
        }

        let col_len = chunksize as usize * BYTES_PER_LBA;
        let end_lba = lba + (data.len() / BYTES_PER_LBA) as LbaT;

        let mut surviving = [Vec::with_capacity(k), Vec::with_capacity(k),
            Vec::with_capacity(k)];
        let mut missing = [Vec::with_capacity(k), Vec::with_capacity(k),
            Vec::with_capacity(k)];
        let mut erasures = FixedBitSet::with_capacity(k);
        let mut excols = exbufs.iter()
            .map(|dbs| dbs.try_mut().unwrap());
        let stripe_offset = lba % (m as LbaT * chunksize);

        let pre_lbas = if end_lba % chunksize > 0 {
            (lba % chunksize).min(end_lba % chunksize)
        } else {
            lba % chunksize
        };
        let post_lbas = if end_lba % chunksize == 0 {
            0
        } else if end_lba % chunksize > lba % chunksize {
            chunksize - end_lba % chunksize
        } else {
            chunksize - lba % chunksize
        };

        let mut codecbytes = [
            pre_lbas as usize * BYTES_PER_LBA,
            0,
            post_lbas as usize * BYTES_PER_LBA,
        ];
        codecbytes[1] = col_len - codecbytes[0] - codecbytes[2];

        // Place buffer pointers into the correct locations for the codec to
        // reassemble.
        let mut append_cols = |
            i,
            r: &Result<()>,
            cols: &mut [DivBufMut]|
        {
            let mut cols = cols.iter_mut();

            let mut col = cols.next().unwrap();
            if codecbytes[0] > 0 {
                let mut precol = col.split_to(codecbytes[0]);
                if r.is_ok() {
                    surviving[0].push(precol.as_ptr());
                } else if i < m {
                    missing[0].push(precol.as_mut_ptr());
                }
            }
            if col.is_empty() {
                col = cols.next().unwrap();
            }
            let mut midcol = col.split_to(codecbytes[1]);
            if r.is_ok() {
                surviving[1].push(midcol.as_ptr());
            } else if i < m {
                missing[1].push(midcol.as_mut_ptr());
            }
            if codecbytes[2] > 0 {
                if col.is_empty() {
                    col = cols.next().unwrap();
                }
                if r.is_ok() {
                    surviving[2].push(col.as_ptr());
                } else if i < m {
                    missing[2].push(col.as_mut_ptr());
                }
            }
        };

        for (i, r) in v.iter().enumerate().take(disks_needed) {
            let chunk_lba = lba - stripe_offset + i as LbaT * chunksize;
            debug_assert_eq!(chunk_lba % chunksize, 0);
            let chunk_elba = chunk_lba + chunksize;
            if chunk_elba <= lba {
                // This chunk is needed for reconstruction only
                let excol = excols.next().unwrap();
                append_cols(i, r, &mut [excol][..])
            } else if chunk_lba < lba {
                // This chunk is partially needed by the user, and partially
                // needed for reconstruction.  We should've read it partially
                // into the user buffer and partially into parity.
                assert!(codecbytes[0] > 0 );
                let excol = excols.next().unwrap();
                let datalbas = (end_lba - lba).min(chunk_elba - lba);
                let datasize = datalbas as usize * BYTES_PER_LBA;
                let dcol = data.split_to(datasize);
                let exsize1 = col_len - datasize - excol.len();
                if exsize1 > 0 {
                    append_cols(i, r, &mut [excol, dcol, excols.next().unwrap()][..]);
                } else {
                    append_cols(i, r, &mut [excol, dcol][..]);
                }
            } else if chunk_elba <= end_lba {
                // This whole chunk is needed by the user
                let dcol = data.split_to(col_len);
                append_cols(i, r, &mut [dcol][..])
            } else if chunk_lba < end_lba {
                // This chunk is partially needed by the user, and partially
                // needed for reconstruction.  We should've read it into the
                // parity buffer; now we must partially copy that back to the
                // user buffer.
                assert!(codecbytes[2] > 0);
                let excol = excols.next().unwrap();
                let l = data.len();
                let dcol = data.split_to(l);
                debug_assert_eq!(l, (end_lba - chunk_lba) as usize * BYTES_PER_LBA);
                debug_assert!(l < chunksize as usize * BYTES_PER_LBA);
                append_cols(i, r, &mut [dcol, excol][..])
            } else {
                // This is either beyond what the user wants, or is parity
                debug_assert!(chunk_lba >= end_lba);
                let excol = excols.next().unwrap();
                append_cols(i, r, &mut [excol][..])
            };
            if r.is_err() {
                erasures.set(i, true);
            }
        }
        for i in 0..3 {
            if !missing[i].is_empty() && codecbytes[i] > 0 {
                debug_assert!(surviving[i].len() >= m);
                // Safe because all columns are still owned (by the caller for
                // data, and by exbufs for parity) and they all have the same
                // length.
                unsafe {
                    codec.decode(codecbytes[i], &surviving[i], &mut missing[i],
                                      &erasures);
                }
            }
        }
        Ok(())
    }

    /// Write two or more whole stripes
    #[allow(clippy::needless_range_loop)]
    fn write_at_multi(&self, mut buf: IoVec, lba: LbaT) -> BoxVdevFut {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.inner.codec.protection() as usize;
        let k = self.inner.codec.stripesize() as usize;
        let m = k - f;
        let n = self.inner.children.len();
        let chunks = buf.len() / col_len;
        let stripes = chunks / m;

        // Allocate storage for parity for the entire operation
        let mut parity = (0..f)
            .map(|_| Vec::<u8>::with_capacity(stripes * col_len))
            .collect::<Vec<_>>();

        // Calculate parity.  We must do it separately for each stripe
        let mut data_refs : Vec<*const u8> = vec![ptr::null(); m];
        let mut parity_refs : Vec<*mut u8> = vec![ptr::null_mut(); f];
        for s in 0..stripes {
            for i in 0..m {
                let chunk = s * m + i;
                let begin = chunk * col_len;
                let end = (chunk + 1) * col_len;
                let col = buf.slice(begin, end);
                data_refs[i] = col.as_ptr();
            }
            for p in 0..f {
                let begin = s * col_len;
                debug_assert!(begin + col_len <= parity[p].capacity());
                // Safe because the assertion passed
                unsafe {
                    parity_refs[p] = parity[p].as_mut_ptr().add(begin);
                }
            }
            // Safe because the above assertion passed
            unsafe {
                self.inner.codec.encode(col_len, &data_refs, &mut parity_refs);
            }
        }

        let mut pw = parity.into_iter()
            .map(|mut v| {
                // Safe because codec.encode filled the columns
                unsafe { v.set_len(stripes * col_len);}
                let dbs = DivBufShared::from(v);
                dbs.try_const().unwrap()
            }).collect::<Vec<_>>();

        // Create an SGList for each disk.
        let mut sglists = Vec::<SGList>::with_capacity(n);
        const SENTINEL : LbaT = LbaT::MAX;
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let max_chunks_per_disk = self.inner.locator.parallel_read_count(chunks);
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            sglists.push(SGList::with_capacity(max_chunks_per_disk));
        }
        // Build the SGLists, one chunk at a time
        let start = ChunkId::Data(lba / self.chunksize);
        let end = ChunkId::Data((lba + (buf.len() / BYTES_PER_LBA) as LbaT) /
                                self.chunksize);
        for (chunk_id, loc) in self.inner.locator.iter(start, end) {
            let col = match chunk_id {
                ChunkId::Data(_) => buf.split_to(col_len),
                ChunkId::Parity(_, i) => pw[i as usize].split_to(col_len)
            };
            let disk_lba = loc.offset * self.chunksize;
            if start_lbas[loc.disk as usize] == SENTINEL {
                start_lbas[loc.disk as usize] = disk_lba;
            } else {
                debug_assert!(start_lbas[loc.disk as usize] < disk_lba);
            }
            sglists[loc.disk as usize].push(col);
        }

        let bi = self.inner.children.iter();
        let sgi = sglists.into_iter();
        let li = start_lbas.into_iter();
        let fut = multizip((bi, sgi, li))
        .filter(|&(_, _, lba)| lba != SENTINEL)
        .map(|(mirrordev, sglist, lba)| mirrordev.writev_at(sglist, lba))
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .map(move |dv| {
            let mut r = Ok(());
            let mut nerrs = 0;
            for e in dv.into_iter().filter(Result::is_err) {
                // As long as we wrote enough disks to make the data
                // recoverable, consider it successful.
                nerrs += 1;
                if nerrs > f {
                    r = e;
                    break
                }
            }
            r
        });
        Box::pin(fut)
        // TODO: on error, record error statistics, possibly fault a drive,
        // request the faulty drive's zone to be rebuilt, and read parity to
        // reconstruct the data.
    }

    /// Write exactly one stripe
    fn write_at_one(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.inner.codec.protection() as usize;
        let m = self.inner.codec.stripesize() as usize - f;

        let mut parity = (0..f)
            .map(|_| Vec::<u8>::with_capacity(col_len))
            .collect::<Vec<_>>();

        let dcols : Vec<IoVec> = buf.into_chunks(col_len).collect();
        {
            let drefs: Vec<*const u8> = dcols.iter()
                .map(|d| d.as_ptr())
                .collect();
            debug_assert_eq!(dcols.len(), m);

            let mut prefs = parity.iter_mut()
                .map(|v| v.as_mut_ptr())
                .collect::<Vec<_>>();

            // Safe because each parity column is sized for `col_len`
            unsafe {
                self.inner.codec.encode(col_len, &drefs, &mut prefs);
            }
        }
        let pw = parity.into_iter()
            .map(|mut v| {
                // Safe because codec::encode filled the column
                unsafe { v.set_len(col_len); }
                let dbs = DivBufShared::from(v);
                dbs.try_const().unwrap()
            });

        let data_fut = issue_1stripe_writes!(self, dcols, lba, false, write_at);
        let parity_fut = issue_1stripe_writes!(self, pw, lba, true, write_at);
        // TODO: on error, record error statistics, and possibly fault a drive.
        Box::pin(
            future::join(data_fut, parity_fut)
            .map(move |(dv, pv)| {
                let mut r = Ok(());
                let mut nerrs = 0;
                let dpvi = dv.into_iter().chain(pv);
                for e in dpvi.filter(Result::is_err) {
                    // As long as we wrote enough disks to make the data
                    // recoverable, consider it successful.
                    nerrs += 1;
                    if nerrs > f {
                        r = e;
                        break
                    }
                }
                r
            })
        )
    }

    /// Write exactly one stripe, with SGLists.
    ///
    /// This is mostly useful internally, for writing from the stripe buffer.
    /// It should not be used publicly.
    #[doc(hidden)]
    pub fn writev_at_one(&self, buf: &[IoVec], lba: LbaT)
        -> BoxVdevFut
    {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.inner.codec.protection() as usize;
        let m = self.inner.codec.stripesize() as usize - f;

        let mut dcols = Vec::<SGList>::with_capacity(m);
        let mut dcursor = SGCursor::from(&buf);
        for _ in 0..m {
            let mut l = 0;
            let mut col = SGList::new();
            while l < col_len {
                let split = dcursor.next(col_len - l).unwrap();
                l += split.len();
                col.push(split);
            }
            dcols.push(col);
        }
        debug_assert_eq!(dcursor.peek_len(), 0);

        let mut parity = (0..f)
            .map(|_| Vec::<u8>::with_capacity(col_len))
            .collect::<Vec<_>>();
        let mut prefs = parity.iter_mut()
            .map(|v| v.as_mut_ptr())
            .collect::<Vec<_>>();

        // Safe because each parity column is sized for `col_len`
        unsafe {
            self.inner.codec.encodev(col_len, &dcols, &mut prefs);
        }
        let pw = parity.into_iter()
            .map(|mut v| {
                // Safe because codec.encode filled the columns
                unsafe { v.set_len(col_len);}
                let dbs = DivBufShared::from(v);
                dbs.try_const().unwrap()
            });

        let data_fut = issue_1stripe_writes!(self, dcols, lba, false, writev_at);
        let parity_fut = issue_1stripe_writes!(self, pw, lba, true, write_at);
        // TODO: on error, record error statistics, and possibly fault a drive.
        Box::pin(
            future::join(data_fut, parity_fut)
            .map(move |(dv, pv)| {
                let mut r = Ok(());
                let mut nerrs = 0;
                let dpvi = dv.into_iter().chain(pv);
                for e in dpvi.filter(Result::is_err) {
                    // As long as we wrote enough disks to make the data
                    // recoverable, consider it successful.
                    nerrs += 1;
                    if nerrs > f {
                        r = e;
                        break
                    }
                }
                r
            })
        )
    }
}

/// Helper function that returns both the minimum and the maximum element of the
/// iterable.
fn min_max<I>(iterable: I) -> Option<(I::Item, I::Item)>
    where I: Iterator, I::Item: Ord + Copy {
    iterable.fold(None, |acc, i| {
        if let Some(accv) = acc {
            Some((cmp::min(accv.0, i), cmp::max(accv.1, i)))
        } else {
            Some((i, i))
        }
    })
}

impl Vdev for VdevRaid {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        let loc = self.inner.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        let child_idx = (0..self.inner.children.len())
            .map(|i| (loc.disk as usize + i) % self.inner.children.len())
            .find(|child_idx| self.inner.children[*child_idx].is_present())
            .unwrap();
        let tentative = self.inner.children[child_idx].lba2zone(disk_lba);
        tentative?;
        // NB: this call to zone_limits is slow, but unfortunately necessary.
        let limits = self.zone_limits(tentative.unwrap());
        if lba >= limits.0 && lba < limits.1 {
            tentative
        } else {
            None
        }
    }

    fn optimum_queue_depth(&self) -> u32 {
        self.optimum_queue_depth
    }

    fn size(&self) -> LbaT {
        self.size
    }

    // Zones don't necessarily line up with repetition boundaries.  So we don't
    // know the disk where a given zone begins.  Worse, declustered RAID is
    // usually not monotonic across all disks.  That is, RAID LBA X may
    // translate to disk 0 LBA Y, while RAID LBA X+1 may translate to disk 1 LBA
    // Y-1.  That is a problem if Y is the first LBA of one of the disks' zones.
    //
    // So we must exclude any stripe that crosses two of the underlying disks'
    // zones.  We must also exclude any row whose chunks cross the underlying
    // zone boundary.
    //
    // Outline:
    // 1) Determine the disks' zone limits.  This will be the same for all
    //    disks.
    // 2) Find both the lowest and the highest stripe that are fully contained
    //    within those limits, on any disk.
    // 3) Determine whether any of those stripes also include a chunk from
    //    the previous zone.
    // 4) Return the first LBA of the lowest stripe after all stripes that
    //    do span the previous zone.
    // 5) Repeat steps 2-4, in mirror image, for the end of the zone.
    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        let m = (self.inner.codec.stripesize()
                 - self.inner.codec.protection()) as LbaT;

        // 1) All children must have the same zone map, so we only need to do
        //    the zone_limits call once.
        let (disk_lba_b, disk_lba_e) = self.first_healthy_child().zone_limits(zone);
        let disk_chunk_b = div_roundup(disk_lba_b, self.chunksize);
        let disk_chunk_e = disk_lba_e / self.chunksize - 1; //inclusive endpoint

        let endpoint_lba = |boundary_chunk, is_highend| {
            // 2) Find the lowest and highest stripe
            let stripes = (0..self.inner.children.len()).map(|i| {
                let cid = self.inner.locator.loc2id(Chunkloc::new(i as i16,
                                                            boundary_chunk));
                cid.address() / m
            });
            let (min_stripe, max_stripe) = min_max(stripes).unwrap();

            // 3,4) Find stripes that cross zones.  Return the innermost that
            // doesn't
            let mut innermost_stripe = None;
            'stripe_loop: for stripe in min_stripe..=max_stripe {
                let minchunk = ChunkId::Data(stripe * m);
                let maxchunk = ChunkId::Data((stripe + 1) * m);
                let chunk_iter = self.inner.locator.iter(minchunk, maxchunk);
                for (_, loc) in chunk_iter {
                    if is_highend && (loc.offset > boundary_chunk) {
                        continue 'stripe_loop;
                    }
                    if !is_highend && (loc.offset < boundary_chunk) {
                        innermost_stripe = None;
                        continue 'stripe_loop;
                    }
                }
                if innermost_stripe.is_none() || is_highend {
                    innermost_stripe = Some(stripe);
                }
            };
            assert!(innermost_stripe.is_some(),
                "No stripes found that don't cross zones.  fix the algorithm");
            let limit_stripe = if is_highend {
                innermost_stripe.unwrap() + 1  // The high limit is exclusive
            } else {
                innermost_stripe.unwrap()
            };
            limit_stripe * m * self.chunksize
        };

        // 5)
        (endpoint_lba(disk_chunk_b, false),
         endpoint_lba(disk_chunk_e, true))
    }

    // The RAID transform does not increase the number of zones; it just makes
    // them bigger
    fn zones(&self) -> ZoneT {
        self.first_healthy_child().zones()
    }
}

#[async_trait]
impl VdevRaidApi for VdevRaid {
    fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut {
        assert!(!self.stripe_buffers.read().unwrap().contains_key(&zone),
            "Tried to erase an open zone");
        let (start, end) = self.first_healthy_child().zone_limits(zone);
        let fut = self.inner.children.iter().filter_map(|mirrordev| {
            mirrordev.erase_zone(start, end - 1)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    /// Mark a child device as faulted.
    fn fault(&mut self, uuid: Uuid) -> Result<()> {
        if uuid == self.uuid {
            return Err(Error::EINVAL);
        }

        let inner = if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner
        } else {
            return Err(Error::EAGAIN);
        };
        for child in inner.children.iter_mut() {
            if uuid == child.uuid() {
                self.faulted_children += child.fault();
                return Ok(());
            } else if let Some(mirror) = child.as_present_mut() {
                match mirror.fault(uuid) {
                    Ok(()) => {
                        let status = mirror.status();
                        if status.health.is_faulted() {
                            self.faulted_children += child.fault();
                        }
                        return Ok(());
                    }
                    Err(Error::ENOENT) => continue,
                    Err(e) => panic!("Unexpected error from Mirror::fault: {:?}", e),
                }
            }
        }

        Err(Error::ENOENT)
    }

    // Zero-fill the current StripeBuffer and write it out.  Then drop the
    // StripeBuffer.
    fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut {
        let (start, end) = self.first_healthy_child().zone_limits(zone);
        let mut futs = FuturesUnordered::new();
        let mut sbs = self.stripe_buffers.write().unwrap();
        let sb = sbs.get_mut(&zone).expect("Can't finish a closed zone");
        if ! sb.is_empty() {
            sb.pad();
            let lba = sb.lba();
            let sgl = sb.pop();
            futs.push(self.writev_at_one(&sgl, lba))
        }
        futs.extend(
            self.inner.children.iter()
            .filter_map(|mirrordev|
                 mirrordev.finish_zone(start, end - 1)
                 .map(|fut| Box::pin(fut) as BoxVdevFut)
            )
        );

        assert!(sbs.remove(&zone).is_some());
        Box::pin(
            futs.try_collect::<Vec<_>>()
            .map_ok(drop)
        )
    }

    fn flush_zone(&self, zone: ZoneT) -> (LbaT, BoxVdevFut) {
        // Flushing a partially written zone to disk requires zero-filling the
        // StripeBuffer so the parity will be correct
        let mut sb_ref = self.stripe_buffers.write().unwrap();
        let sb_opt = sb_ref.get_mut(&zone);
        match sb_opt {
            None => {
                // This zone isn't open, or the buffer is empty.  Nothing to do!
                (0, Box::pin(future::ok(())))
            },
            Some(sb) => {
                if sb.is_empty() {
                    //Nothing to do!
                    (0, Box::pin(future::ok::<(), Error>(())))
                } else {
                    let pad_lbas = sb.pad();
                    let lba = sb.lba();
                    let sgl = sb.pop();
                    drop(sb_ref);
                    (pad_lbas, Box::pin(self.writev_at_one(&sgl, lba)))
                }
            }
        }
    }

    fn open_zone(&self, zone: ZoneT) -> BoxVdevFut {
        self.open_zone_priv(zone, 0)
    }

    fn read_at(&self, mut buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        let f = self.inner.codec.protection();
        let m = (self.inner.codec.stripesize() - f) as LbaT;
        assert_eq!(buf.len() % BYTES_PER_LBA, 0, "reads must be LBA-aligned");

        let mut justreturn = false;
        // end_lba is inclusive.  The highest LBA from which data will be read
        let mut end_lba = lba + ((buf.len() - 1) / BYTES_PER_LBA) as LbaT;
        let buf2 = {
            let sb_ref = self.stripe_buffers.read().unwrap();

            // Look for a StripeBuffer that contains part of the requested
            // range.  There should only be a few zones open at any one time, so
            // it's ok to search through them all.
            let stripe_buffer = sb_ref.iter()
                .map(|(_, sb)| sb)
                .find(|&stripe_buffer| {
                    !stripe_buffer.is_empty() &&
                    lba < stripe_buffer.next_lba() &&
                    end_lba >= stripe_buffer.lba()
                });

            match stripe_buffer {
                Some(sb) if !sb.is_empty() && end_lba >= sb.lba() => {
                    // We need to service part of the read from the StripeBuffer
                    let mut cursor = SGCursor::from(sb.peek());
                    let direct_len = if sb.lba() > lba {
                        (sb.lba() - lba) as usize * BYTES_PER_LBA
                    } else {
                        // Seek to the LBA of interest
                        let mut skipped = 0;
                        let to_skip = (lba - sb.lba()) as usize * BYTES_PER_LBA;
                        while skipped < to_skip {
                            let iovec = cursor.next(to_skip - skipped);
                            skipped += iovec.unwrap().len();
                        }
                        0
                    };
                    let mut sb_buf = buf.split_off(direct_len);
                    // Copy from StripeBuffer into sb_buf
                    while !sb_buf.is_empty() {
                        let iovec = cursor.next(sb_buf.len()).expect(
                            "Read beyond the stripe buffer into unallocated space");
                        sb_buf.split_to(iovec.len())[..]
                            .copy_from_slice(&iovec[..]);
                    }
                    if direct_len == 0 {
                        // Read was fully serviced by StripeBuffer.  No need to
                        // go to disks.
                        justreturn = true;
                    } else {
                        // Service the first part of the read from the disks
                        end_lba = sb.lba() - 1;
                    }
                    buf
                },
                _ => buf        // Don't involve the StripeBuffer
            }
        };
        if justreturn {
            return Box::pin(future::ok(()))
        }
        let start_stripe = lba / (self.chunksize * m as LbaT);
        let end_stripe = end_lba / (self.chunksize * m);
        if start_stripe == end_stripe {
            Box::pin(self.read_at_one(buf2, lba))
        } else {
            Box::pin(self.read_at_multi(buf2, lba))
        }
    }

    fn read_long(&self, len: LbaT, ulba: LbaT)
        -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>
    {
        let f = self.inner.codec.protection();
        let m = self.inner.codec.stripesize() - f;

        if self.faulted_children() >= f {
            return future::err(Error::EINTEGRITY).boxed();
        }

        // Ignore the stripe buffer.  The fact that read_long got called meant
        // that, even if the data is available in the stripe buffer , it must be
        // wrong.

        let start_stripe = ulba / (self.chunksize * m as LbaT);
        // end_lba is inclusive.  The highest LBA from which data will be read
        let end_lba = ulba + len - 1;
        let end_stripe = end_lba / (self.chunksize as LbaT * m as LbaT);
        if start_stripe == end_stripe {
            Box::pin(self.read_long_one(len, ulba))
        } else {
            Box::pin(self.read_long_multi(len, ulba))
        }
    }

    fn read_spacemap(&self, buf: IoVecMut, smidx: u32) -> BoxVdevFut
    {
        let dbi = buf.clone_inaccessible();
        let inner = self.inner.clone();
        let fut = inner.children.first().map(|child| child.read_spacemap(buf, smidx));

        Box::pin(ReadSpacemap {
            inner,
            next: 1,
            dbi,
            smidx,
            fut
        })
    }

    fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut
    {
        self.open_zone_priv(zone, allocated)
    }

    fn status(&self) -> Status {
        let n = self.inner.children.len();
        let k = self.inner.codec.stripesize();
        let f = self.inner.codec.protection();
        let codec = format!("{}-{},{},{}", self.layout_algorithm, n, k, f);
        let mut mirrors = Vec::with_capacity(self.inner.children.len());
        for child in self.inner.children.iter() {
            let cs = child.status().unwrap_or(mirror::Status {
                health: Health::Faulted(FaultedReason::Removed),
                uuid: child.uuid(),
                leaves: Vec::new()
            });
            mirrors.push(cs);
        }
        let d = mirrors.iter().map(|m| {
            match m.health {
                Health::Online => 0,
                Health::Degraded(n) => u8::from(n),
                _ => {
                    if m.leaves.is_empty() {
                        // XXX in the case of a missing mirror child, we don't
                        // know how many children it's supposed to have.  TODO:
                        // store the target size of a mirror in its label.
                        1
                    } else {
                        m.leaves.len() as u8
                    }
                }
            }
        }).sum();
        let health = if self.faulted_children() > self.inner.codec.protection()
        {
            Health::Faulted(FaultedReason::InsufficientRedundancy)
        } else {
            NonZeroU8::new(d)
                .map(Health::Degraded)
                .unwrap_or(Health::Online)
        };
        Status {
            health,
            codec,
            mirrors,
            uuid: self.uuid
        }
    }

    fn sync_all(&self) -> BoxVdevFut {
        // Don't flush zones ourselves; the Cluster layer must be in charge of
        // that, so it can update the spacemap.
        debug_assert!(
            self.stripe_buffers.read().unwrap()
            .values()
            .all(StripeBuffer::is_empty),
            "Must call flush_zone before sync_all"
        );
        // TODO: handle errors on some devices
        let fut = self.inner.children.iter()
        .filter_map(|bd| bd.sync_all())
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
    }

    fn write_at(&self, buf: IoVec, zone: ZoneT, mut lba: LbaT) -> BoxVdevFut
    {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.inner.codec.protection() as usize;
        let m = self.inner.codec.stripesize() as usize - f;
        let stripe_len = col_len * m;
        debug_assert_eq!(zone, self.lba2zone(lba).unwrap(),
            "Write to wrong zone");
        debug_assert_eq!(zone, self.lba2zone(lba +
                ((buf.len() - 1) / BYTES_PER_LBA) as LbaT).unwrap(),
            "Write spanned a zone boundary");
        let futs = FuturesUnordered::<BoxVdevFut>::new();
        {
            let mut sb_ref = self.stripe_buffers.write().unwrap();
            let stripe_buffer = sb_ref.get_mut(&zone)
                                      .expect("Can't write to a closed zone");
            assert_eq!(stripe_buffer.next_lba(), lba);

            let mut buf3 = if !stripe_buffer.is_empty() ||
                buf.len() < stripe_len {

                let buflen = buf.len();
                let buf2 = stripe_buffer.fill(buf);
                if stripe_buffer.is_full() {
                    let stripe_lba = stripe_buffer.lba();
                    let sglist = stripe_buffer.pop();
                    lba += ((buflen - buf2.len()) / BYTES_PER_LBA) as LbaT;
                    futs.push(self.writev_at_one(&sglist, stripe_lba));
                }
                buf2
            } else {
                buf
            };
            if !buf3.is_empty() {
                debug_assert!(stripe_buffer.is_empty());
                let nstripes = buf3.len() / stripe_len;
                let writable_buf = buf3.split_to(nstripes * stripe_len);
                stripe_buffer.reset(lba + (nstripes * m) as LbaT * self.chunksize);
                if ! buf3.is_empty() {
                    let buf4 = stripe_buffer.fill(buf3);
                    if stripe_buffer.is_full() {
                        // We can only get here if buf was within < 1 LBA of
                        // completing a stripe
                        let slba = stripe_buffer.lba();
                        let sglist = stripe_buffer.pop();
                        futs.push(self.writev_at_one(&sglist, slba));
                    }
                    debug_assert!(!stripe_buffer.is_full());
                    debug_assert!(buf4.is_empty());
                }
                futs.push(if nstripes == 1 {
                    self.write_at_one(writable_buf, lba)
                } else {
                    self.write_at_multi(writable_buf, lba)
                });
            }
        }
        Box::pin(futs.try_collect::<Vec<_>>().map_ok(drop))
    }

    fn write_label(&self, mut labeller: LabelWriter) -> BoxVdevFut
    {
        let children_uuids = self.inner.children.iter().map(|bd| bd.uuid())
            .collect::<Vec<_>>();
        let raid_label = Label {
            uuid: self.uuid,
            chunksize: self.chunksize,
            disks_per_stripe: self.inner.locator.stripesize(),
            redundancy: self.inner.locator.protection(),
            layout_algorithm: self.layout_algorithm,
            children: children_uuids
        };
        let label = super::Label::Raid(raid_label);
        labeller.serialize(&label).unwrap();
        let fut = self.inner.children.iter().map(|bd| {
           bd.write_label(labeller.clone())
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> BoxVdevFut
    {
        let fut = self.inner.children.iter().map(|bd| {
            bd.write_spacemap(sglist.clone(), idx, block)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

}
