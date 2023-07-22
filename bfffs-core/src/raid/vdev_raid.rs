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
use itertools::multizip;
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
// We optimize for the large case (Present) which is both more important and
// more common than the small case.
#[allow(clippy::large_enum_variant)]
enum Child {
    Present(Mirror),
    Missing(Uuid)
}

cfg_if! {
    if #[cfg(test)] {
        type ChildReadSpacemap = BoxVdevFut;
    } else {
        type ChildReadSpacemap = mirror::ReadSpacemap;
    }
}

impl Child {
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

    fn erase_zone(&self, start: LbaT, end: LbaT) -> Option<BoxVdevFut> {
        self.as_present().map(|m| m.erase_zone(start, end))
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
        self.as_present().map(Mirror::status)
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
        self.as_present().map(Mirror::optimum_queue_depth)
    }

    fn size(&self) -> Option<LbaT> {
        self.as_present().map(Mirror::size)
    }

    fn sync_all(&self) -> Option<BoxVdevFut> {
        self.as_present().map(Mirror::sync_all)
    }

    fn uuid(&self) -> Uuid {
        match self {
            Child::Present(vb) => vb.uuid(),
            Child::Missing(uuid) => *uuid
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
    vdev: Arc<VdevRaid>,
    /// Reads already issued so far
    issued: usize,
    /// Index of the first child read from
    initial_idx: usize,
    dbi: DivBufInaccessible,
    /// Spacemap index to read
    smidx: u32,
    #[cfg(not(test))]
    #[pin]
    fut: crate::mirror::ReadSpacemap,
    #[cfg(test)]
    #[pin]
    fut: BoxVdevFut,
}
impl Future for ReadSpacemap {
    type Output = Result<()>;

    fn poll<'a>(mut self: Pin<&mut Self>, cx: &mut Context<'a>) -> Poll<Self::Output>
    {
        let nchildren = self.vdev.children.len();
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
            let new_fut = pinned.vdev.children[idx].read_spacemap(buf,
                *pinned.smidx);
            self.set(ReadSpacemap {
                vdev: self.vdev.clone(),
                issued: self.issued + 1,
                initial_idx: self.initial_idx,
                dbi: self.dbi.clone(),
                smidx: self.smidx,
                fut: new_fut
            });
        })
    }
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

    /// RAID codec
    codec: Codec,

    /// Locator, declustering or otherwise
    locator: Box<dyn Locator>,

    /// Underlying mirror devices.  Order is important!
    children: Box<[Child]>,

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
}

/// Convenience macro for `VdevRaid` I/O methods
///
/// # Examples
///
/// ```no_run
/// let v = Vec::<IoVec>::with_capacity(4);
/// let lba = 0;
/// let fut = issue_1stripe_ops!(self, v, lba, false, write_at)
/// ```
macro_rules! issue_1stripe_ops {
    ( $self:ident, $buf:expr, $lba:expr, $parity:expr, $func:ident) => {
        {
            let (start, end) = if $parity {
                let m = $self.codec.stripesize() - $self.codec.protection();
              (ChunkId::Parity($lba / $self.chunksize, 0),
                 ChunkId::Data($lba / $self.chunksize + m as u64))
            } else {
                (ChunkId::Data($lba / $self.chunksize),
                 ChunkId::Parity($lba / $self.chunksize, 0))
            };
            let mut iter = $self.locator.iter(start, end);
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
                $self.children[loc.disk as usize].$func(d, disk_lba)
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

    /// Return a reference to the first child that isn't missing
    fn first_healthy_child(&self) -> &Mirror {
        for c in self.children.iter() {
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
        let num_disks = children.len() as i16;
        let codec = Codec::new(disks_per_stripe as u32, redundancy as u32);
        let locator: Box<dyn Locator> = match layout_algorithm {
            LayoutAlgorithm::PrimeS => Box::new(
                PrimeS::new(num_disks, disks_per_stripe, redundancy))
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

        VdevRaid { chunksize, codec, locator, children, layout_algorithm,
                   optimum_queue_depth,
                   size,
                   stripe_buffers: RwLock::new(BTreeMap::new()),
                   uuid}
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
                Child::present(m)
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
        let f = self.codec.protection();
        let m = (self.codec.stripesize() - f) as LbaT;
        let stripe_lbas = m * self.chunksize as LbaT;
        let (start_lba, _) = self.zone_limits(zone);
        let sb = StripeBuffer::new(start_lba + already_allocated, stripe_lbas);
        assert!(self.stripe_buffers.write().unwrap().insert(zone, sb).is_none());

        let (first_disk_lba, _) = self.first_healthy_child().zone_limits(zone);
        let start_disk_chunk = div_roundup(first_disk_lba, self.chunksize);
        let futs = FuturesUnordered::<BoxVdevFut>::new();
        for (idx, mirrordev) in self.children.iter().enumerate() {
            if !mirrordev.is_present() {
                continue;
            }

            // Find the first LBA of this disk that's within our zone
            let mut first_usable_disk_lba = 0;
            for chunk in start_disk_chunk.. {
                let loc = Chunkloc::new(idx as i16, chunk);
                let chunk_id = self.locator.loc2id(loc);
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
    fn read_at_multi(self: Arc<Self>, mut buf: IoVecMut, lba: LbaT) -> impl Future<Output=Result<()>>
    {
        const SENTINEL : LbaT = LbaT::max_value();

        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let n = self.children.len();
        let f = self.codec.protection();
        let k = self.codec.stripesize();
        let m = k - f;
        let lbas_per_stripe = self.chunksize * m as LbaT;
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);
        let lbas = (buf.len() / BYTES_PER_LBA) as LbaT;
        let chunks = div_roundup(buf.len(), col_len);
        let stripes = div_roundup(chunks, m as usize);

        // Allocate storage for parity, which we might need to read
        let mut pbufs = (0..f)
            .map(|_| DivBufShared::uninitialized(stripes * col_len))
            .collect::<Vec<_>>();
        let mut pmuts = pbufs.iter_mut()
            .map(|dbs| dbs.try_mut().unwrap())
            .collect::<Vec<_>>();

        // Store DivBufInaccessible for use during error recovery
        let dbi = buf.clone_inaccessible();

        // Create an SGList for each disk.
        let mut sglists = Vec::<SGListMut>::with_capacity(n);
        // Create additional SGLists to store Parity buffers, if we need them.
        let mut psglists = Vec::<SGListMut>::new();
        psglists.resize_with(n, Default::default);
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let max_chunks_per_disk = self.locator.parallel_read_count(chunks);
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            sglists.push(SGListMut::with_capacity(max_chunks_per_disk));
        }
        // Build the SGLists, one chunk at a time
        let start = ChunkId::Data(lba / self.chunksize);
        let end_lba = lba + lbas;   // First LBA past end of the operation
        let at_end_of_stripe = end_lba % lbas_per_stripe >
            lbas_per_stripe - self.chunksize;
        let end = if at_end_of_stripe {
            let end_chunk = end_lba / self.chunksize;
            ChunkId::Parity(end_chunk - end_chunk % m as LbaT, 0)
        } else {
            ChunkId::Data(div_roundup(lba + lbas, self.chunksize))
        };
        let mut starting = true;
        for (cid, loc) in self.locator.iter(start, end) {
            let mut disk_lba = loc.offset * self.chunksize;
            let disk = loc.disk as usize;
            match cid {
                ChunkId::Data(_did) => {
                    let col = if starting && lba % self.chunksize != 0 {
                        // The operation begins mid-chunk
                        let lbas_into_chunk = lba % self.chunksize;
                        let chunk0lbas = self.chunksize - lbas_into_chunk;
                        let chunk0size = chunk0lbas as usize * BYTES_PER_LBA;
                        disk_lba += lbas_into_chunk;
                        buf.split_to(chunk0size)
                    } else {
                        let chunklen = cmp::min(buf.len(), col_len);
                        buf.split_to(chunklen)
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
                    if sglists[disk].is_empty() {
                        // Skip this chunk if we aren't already reading a Data
                        // chunk from an earlier LBA of this disk.
                        let _ = pmuts[pid as usize].split_to(col_len);
                    } else {
                        let col = pmuts[pid as usize].split_to(col_len);
                        psglists[disk].push(col);
                    }
                }
            }
        }

        multizip((self.children.iter(),
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
        .collect::<FuturesOrdered::<_>>()
        .collect::<Vec<_>>()
        .then(move |dv| {
            if dv.iter().all(Result::is_ok) {
                Box::pin(future::ok(())) as BoxVdevFut
            } else {
                // Split the range into single stripes and perform error
                // recovery on each.  It isn't the most efficient, but this
                // should be a rare case.
                let mut wbuf = dbi.try_mut().unwrap();
                let start_stripe = lba / (self.chunksize * m as LbaT);
                let end_lba = lba + ((wbuf.len() - 1) / BYTES_PER_LBA) as LbaT;
                let past_lba = lba + (wbuf.len() / BYTES_PER_LBA) as LbaT;
                let end_stripe = end_lba / (self.chunksize * m as LbaT);
                let lbas_per_stripe = m as LbaT * self.chunksize as LbaT;
                let fut = (start_stripe..=end_stripe).map(move |s| {
                    let slba = lba.max(s * lbas_per_stripe);
                    let elba = past_lba.min((s + 1) * lbas_per_stripe);
                    let sbytes = (elba - slba) as usize * BYTES_PER_LBA;
                    let start = ChunkId::Data(slba / self.chunksize);
                    let end = ChunkId::Data(elba / self.chunksize);
                    let lociter = self.locator.iter_data(start, end);
                    let dvs = lociter
                        .map(|(_cid, loc)| Some(dv[loc.disk as usize]))
                        .collect::<Vec<_>>();
                    // TODO: make use of any parity chunks that we
                    // opportunistically read.
                    self.clone()
                        .read_at_recovery(wbuf.split_to(sbytes), slba, dvs)
                }).collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
                .map_ok(drop);
                Box::pin(fut) as BoxVdevFut
            }
        })
    }

    /// Read a (possibly improper) subset of one stripe
    fn read_at_one(self: Arc<Self>, mut buf: IoVecMut, lba: LbaT) -> impl Future<Output=Result<()>>{
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let m = (self.codec.stripesize() - self.codec.protection()) as usize;
        let dbi = buf.clone_inaccessible();

        let data: Vec<IoVecMut> = if lba % self.chunksize == 0 {
            buf.into_chunks(col_len).collect()
        } else {
            let lbas_into_chunk = lba % self.chunksize;
            let chunk0lbas = self.chunksize - lbas_into_chunk;
            let chunk0size = cmp::min(chunk0lbas as usize * BYTES_PER_LBA,
                                      buf.len());
            let col0 = buf.split_to(chunk0size);
            let rest = buf.into_chunks(col_len);
            Some(col0).into_iter().chain(rest).collect()
        };
        debug_assert!(data.len() <= m);

        issue_1stripe_ops!(self, data, lba, false, read_at)
        .then(move |dv| {
            if dv.iter().all(Result::is_ok) {
                Box::pin(future::ok(())) as BoxVdevFut
            } else {
                let dvs = dv.into_iter().map(Some).collect::<Vec<_>>();
                let data = dbi.try_mut().unwrap();
                Box::pin(self.read_at_recovery(data, lba, dvs))
            }
        })
        // TODO: on error, record error statistics, possibly fault a drive,
        // and request the faulty drive's zone to be rebuilt.
    }

    /// Reconstruct a stripe of data, given surviving columns and parity
    fn read_at_reconstruction(
        &self,
        // Handle to the original buffer
        data: DivBufMut,
        // LBA where the original operation began
        lba: LbaT,
        exbufs: Vec<DivBufShared>,
        // The results of the read attempts for each disk in the stripe.
        v: Vec<Result<()>>
    ) -> Result<()>
    {
        let f = self.codec.protection() as usize;
        let k = self.codec.stripesize() as usize;
        let m = k - f;
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let end_lba = lba + ((data.len() - 1) / BYTES_PER_LBA) as LbaT;

        let mut surviving = Vec::with_capacity(k);
        let mut missing = Vec::with_capacity(f);
        let mut erasures = FixedBitSet::with_capacity(k);
        let mut container = Vec::with_capacity(k);
        let mut excols = exbufs.into_iter()
            .map(|dbs| dbs.try_mut().unwrap());
        let mut dcols = data.into_chunks(col_len);
        for (i, r) in v.iter().enumerate().take(k) {
            let stripe_offset = lba % (m as LbaT * self.chunksize);
            let recovery_lba = lba - stripe_offset + i as LbaT * self.chunksize;
            let mut col = if lba <= recovery_lba && recovery_lba <= end_lba
            {
                dcols.next().unwrap()
            } else {
                excols.next().unwrap()
            };
            if r.is_ok() {
                surviving.push(col.as_ptr());
            } else if i < m {
                missing.push(col.as_mut_ptr());
                erasures.set(i, true);
            } else {
                // Don't try to reconstruct missing parity columns
                erasures.set(i, true);
            }
            container.push(col);
            if surviving.len() >= m {
                break;
            }
        }
        if missing.is_empty() {
            // Nothing to do!
            Ok(())
        } else if surviving.len() >= m {
            // Safe because all columns are held in `container` and
            // they all have the same length.
            unsafe {
                self.codec.decode(col_len, &surviving, &mut missing,
                                  &erasures);
            }
            Ok(())
            // TODO: re-write the offending sector for READ UNRECOVERABLE
        } else {
            let r = v.iter()
                .find(|r| matches!(r, Err(_)))
                .unwrap();
            Err(r.unwrap_err())
        }
    }

    /// Error-recovery path for [`read_at`], for at most one RAID stripe at a
    /// time.
    fn read_at_recovery(
        self: Arc<Self>,
        // Handle to the original buffer
        data: DivBufMut,
        // LBA where the original operation began
        lba: LbaT,
        // Results of the original read attempts, if any.
        dv: Vec<Option<Result<()>>>) -> BoxVdevFut
    {
        let f = self.codec.protection() as usize;
        if dv.iter()
            .filter(|r| r.as_ref().map(Result::is_err).unwrap_or(false))
            .count() > f
        {
            // Too many errors.  No point in attempting recovery. :(
            let r = dv.iter()
                .find(|r| matches!(r, Some(Err(_))))
                .unwrap()
                .unwrap();
            Box::pin(future::err(r.unwrap_err())) as BoxVdevFut
        } else {
            let k = self.codec.stripesize() as usize;
            let m = k - f;
            let col_len = self.chunksize as usize * BYTES_PER_LBA;

            let start_stripe = lba / (self.chunksize * m as LbaT);
            let end_lba = lba + ((data.len() - 1) / BYTES_PER_LBA) as LbaT;
            let end_stripe = end_lba / (self.chunksize * m as LbaT);
            debug_assert_eq!(start_stripe, end_stripe);

            // Allocate space to read parity chunks, and possibly other data
            // chunks that the user did not request but are nevertheless needed
            // for reconstruction.
            let mut exbufs = (0..(k - dv.len()))
                .map(|_| DivBufShared::uninitialized(col_len))
                .collect::<Vec<_>>();
            let mut exmuts = exbufs.iter_mut()
                .map(|dbs| dbs.try_mut().unwrap());
            let mut recovery_lba = lba - (lba % (m as LbaT * self.chunksize));
            let start = ChunkId::Data(recovery_lba / self.chunksize);
            let end = ChunkId::Data(recovery_lba / self.chunksize + m as LbaT);
            let mut lociter = self.locator.iter(start, end);
            let mut did = 0;
            let fut = (0..k).map(|_| {
                let (_, loc) = lociter.next().unwrap();
                let fut = if lba <= recovery_lba && recovery_lba <= end_lba {
                    did += 1;
                    let or = dv[did - 1];
                    if let Some(r) = or {
                        // Already attempted to read this chunk.  No need to
                        // reread.
                        Box::pin(future::ready(r)) as BoxVdevFut
                    } else {
                        // TODO: at this point, issue reads to data that we
                        // don't have, when doing a degraded mode read.
                        todo!()
                    }
                } else {
                    // Read extra data or parity chunks for reconstruction
                    let buf = exmuts.next().unwrap();
                    Box::pin(self.children[loc.disk as usize].read_at(buf, loc.offset * self.chunksize))
                };
                recovery_lba += self.chunksize;
                fut
            }).collect::<FuturesOrdered<_>>()
            .collect::<Vec<Result<()>>>()
            .map(move |v| {
                self.read_at_reconstruction(data, lba, exbufs, v)
                // TODO: record error statistics
            });
            Box::pin(fut) as BoxVdevFut
        }
    }


    /// Write two or more whole stripes
    #[allow(clippy::needless_range_loop)]
    fn write_at_multi(&self, mut buf: IoVec, lba: LbaT) -> BoxVdevFut {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let k = self.codec.stripesize() as usize;
        let m = k - f;
        let n = self.children.len();
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
                self.codec.encode(col_len, &data_refs, &mut parity_refs);
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
        const SENTINEL : LbaT = LbaT::max_value();
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let max_chunks_per_disk = self.locator.parallel_read_count(chunks);
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            sglists.push(SGList::with_capacity(max_chunks_per_disk));
        }
        // Build the SGLists, one chunk at a time
        let start = ChunkId::Data(lba / self.chunksize);
        let end = ChunkId::Data((lba + (buf.len() / BYTES_PER_LBA) as LbaT) /
                                self.chunksize);
        for (chunk_id, loc) in self.locator.iter(start, end) {
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

        let bi = self.children.iter();
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
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f;

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
                self.codec.encode(col_len, &drefs, &mut prefs);
            }
        }
        let pw = parity.into_iter()
            .map(|mut v| {
                // Safe because codec::encode filled the column
                unsafe { v.set_len(col_len); }
                let dbs = DivBufShared::from(v);
                dbs.try_const().unwrap()
            });

        let data_fut = issue_1stripe_ops!(self, dcols, lba, false, write_at);
        let parity_fut = issue_1stripe_ops!(self, pw, lba, true, write_at);
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
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f;

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
            self.codec.encodev(col_len, &dcols, &mut prefs);
        }
        let pw = parity.into_iter()
            .map(|mut v| {
                // Safe because codec.encode filled the columns
                unsafe { v.set_len(col_len);}
                let dbs = DivBufShared::from(v);
                dbs.try_const().unwrap()
            });

        let data_fut = issue_1stripe_ops!(self, dcols, lba, false, writev_at);
        let parity_fut = issue_1stripe_ops!(self, pw, lba, true, write_at);
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
        let loc = self.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        let child_idx = (0..self.children.len())
            .map(|i| (loc.disk as usize + i) % self.children.len())
            .find(|child_idx| self.children[*child_idx].is_present())
            .unwrap();
        let tentative = self.children[child_idx].lba2zone(disk_lba);
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
        let fut = self.children.iter()
        .filter_map(|bd| bd.sync_all())
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn uuid(&self) -> Uuid {
        self.uuid
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
        let m = (self.codec.stripesize() - self.codec.protection()) as LbaT;

        // 1) All children must have the same zone map, so we only need to do
        //    the zone_limits call once.
        let (disk_lba_b, disk_lba_e) = self.first_healthy_child().zone_limits(zone);
        let disk_chunk_b = div_roundup(disk_lba_b, self.chunksize);
        let disk_chunk_e = disk_lba_e / self.chunksize - 1; //inclusive endpoint

        let endpoint_lba = |boundary_chunk, is_highend| {
            // 2) Find the lowest and highest stripe
            let stripes = (0..self.children.len()).map(|i| {
                let cid = self.locator.loc2id(Chunkloc::new(i as i16,
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
                let chunk_iter = self.locator.iter(minchunk, maxchunk);
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
        let fut = self.children.iter().filter_map(|mirrordev| {
            mirrordev.erase_zone(start, end - 1)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
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
            self.children.iter()
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

    fn read_at(self: Arc<Self>, mut buf: IoVecMut, lba: LbaT) -> BoxVdevFut {
        let f = self.codec.protection();
        let m = (self.codec.stripesize() - f) as LbaT;
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

    fn read_spacemap(self: Arc<Self>, buf: IoVecMut, smidx: u32) -> BoxVdevFut
    {
        let dbi = buf.clone_inaccessible();
        let fut = self.first_healthy_child().read_spacemap(buf, smidx);
        let fut = ReadSpacemap {
            vdev: self,
            issued: 1,
            initial_idx: 0,
            dbi,
            smidx,
            fut
        };
        Box::pin(fut)
    }

    fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut
    {
        self.open_zone_priv(zone, allocated)
    }

    fn status(&self) -> Status {
        let n = self.children.len();
        let k = self.codec.stripesize();
        let f = self.codec.protection();
        let codec = format!("{}-{},{},{}", self.layout_algorithm, n, k, f);
        let mut mirrors = Vec::with_capacity(self.children.len());
        for child in self.children.iter() {
            let cs = child.status().unwrap_or(mirror::Status {
                health: Health::Faulted,
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
        let health = NonZeroU8::new(d)
            .map(Health::Degraded)
            .unwrap_or(Health::Online);
        Status {
            health,
            codec,
            mirrors,
        }
    }

    fn write_at(&self, buf: IoVec, zone: ZoneT, mut lba: LbaT) -> BoxVdevFut
    {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f;
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
        let children_uuids = self.children.iter().map(|bd| bd.uuid())
            .collect::<Vec<_>>();
        let raid_label = Label {
            uuid: self.uuid,
            chunksize: self.chunksize,
            disks_per_stripe: self.locator.stripesize(),
            redundancy: self.locator.protection(),
            layout_algorithm: self.layout_algorithm,
            children: children_uuids
        };
        let label = super::Label::Raid(raid_label);
        labeller.serialize(&label).unwrap();
        let fut = self.children.iter().map(|bd| {
           bd.write_label(labeller.clone())
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> BoxVdevFut
    {
        let fut = self.children.iter().map(|bd| {
            bd.write_spacemap(sglist.clone(), idx, block)
        }).collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .map_ok(drop);
        Box::pin(fut)
    }

}
