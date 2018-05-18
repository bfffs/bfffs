// vim: tw=80

use common::{*, declust::*, label::*, vdev::*, raid::*};
#[cfg(not(test))] use common::vdev_block::*;
#[cfg(any(not(test), feature = "mocks"))]
use common::{null_raid::*, prime_s::*};
use divbuf::DivBufShared;
use futures::{Future, future};
#[cfg(not(test))]
use itertools::Itertools;
use itertools::multizip;
use nix::Error;
use std::{cell::RefCell, cmp, mem, ptr};
use std::collections::BTreeMap;
#[cfg(not(test))] use std::path::Path;
use tokio::reactor::Handle;
use uuid::Uuid;

#[cfg(test)]
/// Only exists so mockers can replace VdevBlock
pub trait VdevBlockTrait : Vdev {
    fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut>;
    fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut>;
    fn open_zone(&self, lba: LbaT) -> Box<VdevFut>;
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevFut>;
    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
    fn write_label(&self, labeller: LabelWriter) -> Box<VdevFut>;
    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevFut>;
}
#[cfg(test)]
pub type VdevBlockLike = Box<VdevBlockTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type VdevBlockLike = VdevBlock;

/// RAID placement algorithm.
///
/// This algorithm maps RAID chunks to specific disks and offsets.  It does not
/// encode or decode parity.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum LayoutAlgorithm {
    /// The trivial, nonredundant placement algorithm
    NullRaid,
    /// A good declustered algorithm for any prime number of disks
    PrimeS,
}

/// In-memory cache of data that has not yet been flushed the Block devices.
///
/// Typically there will be one of these for each open zone.
struct StripeBuffer {
    /// Cache of `IoVec`s that haven't yet been flushed
    buf: SGList,

    /// The LBA of the beginning of the cached stripe
    lba: LbaT,

    /// Amount of data in a full stripe, in LBAs
    stripe_lbas: LbaT,
}

impl StripeBuffer {
    /// Store more data into this `StripeBuffer`, but don't overflow one row.
    ///
    /// Return the unused part of the `IoVec`
    pub fn fill(&mut self, mut iovec: IoVec) -> IoVec {
        let want_lbas = self.stripe_lbas - self.len();
        let want_bytes = want_lbas as usize * BYTES_PER_LBA as usize;
        let have_bytes = iovec.len();
        let get_bytes = cmp::min(want_bytes, have_bytes);
        if get_bytes > 0 {
            self.buf.push(iovec.split_to(get_bytes));
        }
        iovec
    }

    /// Is the stripe buffer full?
    pub fn is_full(&self) -> bool {
        debug_assert!(self.len() <= self.stripe_lbas);
        self.len() == self.stripe_lbas
    }

    /// The usual `is_empty` function
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The LBA of the beginning of the stripe
    pub fn lba(&self) -> LbaT {
        self.lba
    }

    /// Number of LBAs worth of data contained in the buffer
    fn len(&self) -> LbaT {
        let bytes: usize = self.buf.iter().map(|iovec| iovec.len()).sum();
        (bytes / BYTES_PER_LBA as usize) as LbaT
    }

    pub fn new(lba: LbaT, stripe_lbas: LbaT) -> Self {
        StripeBuffer{ buf: SGList::new(), lba, stripe_lbas}
    }

    /// Return the value of the next LBA should be written into this buffer
    pub fn next_lba(&self) -> LbaT {
        self.lba + self.len()
    }

    /// Fill the `StripeBuffer` with zeros and return the number of LBAs worth
    /// of padding.
    pub fn pad(&mut self) -> LbaT {
        let pad_lbas = self.stripe_lbas - self.len();
        let padlen = pad_lbas as usize * BYTES_PER_LBA;
        let zero_region_len = ZERO_REGION.len();
        let zero_bufs = div_roundup(padlen, zero_region_len);
        for _ in 0..(zero_bufs - 1) {
            self.fill(ZERO_REGION.try().unwrap());
        }
        self.fill(ZERO_REGION.try().unwrap().slice_to(
                padlen - (zero_bufs - 1) * zero_region_len));
        debug_assert!(self.is_full());
        pad_lbas
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

// LCOV_EXCL_START
#[derive(Serialize, Deserialize, Debug)]
struct Label {
    /// Vdev UUID, fixed at format time
    uuid:               Uuid,
    chunksize:          LbaT,
    disks_per_stripe:   i16,
    redundancy:         i16,
    layout_algorithm:   LayoutAlgorithm,
    children:           Vec<Uuid>
}
// LCOV_EXCL_STOP

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

    /// Tokio reactor handle
    handle: Handle,

    /// Locator, declustering or otherwise
    locator: Box<Locator>,

    /// Underlying block devices.  Order is important!
    blockdevs: Box<[VdevBlockLike]>,

    /// RAID placement algorithm.
    layout_algorithm: LayoutAlgorithm,

    /// Best number of queued commands for the whole `VdevRaid`
    optimum_queue_depth: u32,

    /// In memory cache of data that has not yet been flushed to the block
    /// devices.
    ///
    /// We cache up to one stripe per zone before flushing it.  Cacheing entire
    /// stripes uses fewer resources than only cacheing the parity information.
    stripe_buffers: RefCell<BTreeMap<ZoneT, StripeBuffer>>,

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
            let futs : Vec<_> = $buf
            .into_iter()
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
                $self.blockdevs[loc.disk as usize].$func(d, disk_lba)
            })
            .collect();
            future::join_all(futs)
        }
    }
}

impl VdevRaid {
    /// Choose the best declustering layout for the requirements given.
    #[cfg(not(test))]
    fn choose_layout(num_disks: i16, _disks_per_stripe: i16,
                     _redundancy: i16) -> LayoutAlgorithm {

        if num_disks == 1 {
            LayoutAlgorithm::NullRaid
        } else {
            LayoutAlgorithm::PrimeS
        }
    }

    /// Create a new VdevRaid from unused files or devices
    ///
    /// * `chunksize`:          RAID chunksize in LBAs.  This is the largest
    ///                         amount of data that will be read/written to a
    ///                         single device before the `Locator` switches to
    ///                         the next device.
    /// * `num_disks`:          Total number of disks in the array
    /// * `disks_per_stripe`:   Number of data plus parity chunks in each
    ///                         self-contained RAID stripe.  Must be less than
    ///                         or equal to `num_disks`.
    /// * `redundancy`:         Degree of RAID redundancy.  Up to this many
    ///                         disks may fail before the array becomes
    ///                         inoperable.
    /// * `paths`:              Slice of pathnames of files and/or devices
    /// * `handle`:             Handle to the Tokio reactor that will be used to
    ///                         service this vdev.
    #[cfg(not(test))]
    pub fn create<P: AsRef<Path>>(chunksize: LbaT,
                                  num_disks: i16,
                                  disks_per_stripe: i16,
                                  redundancy: i16,
                                  paths: &[P],
                                  handle: Handle) -> Self {
        let layout_algo = VdevRaid::choose_layout(num_disks, disks_per_stripe,
                                                  redundancy);
        let uuid = Uuid::new_v4();
        let blockdevs = paths.iter().map(|path| {
            VdevBlock::create(path, handle.clone()).unwrap()
        }).collect::<Vec<_>>();
        VdevRaid::new(chunksize, disks_per_stripe, redundancy, uuid,
                      layout_algo, blockdevs.into_boxed_slice(), handle)
    }

    #[cfg(any(not(test), feature = "mocks"))]
    fn new(chunksize: LbaT,
           disks_per_stripe: i16,
           redundancy: i16,
           uuid: Uuid,
           layout_algorithm: LayoutAlgorithm,
           blockdevs: Box<[VdevBlockLike]>,
           handle: Handle) -> Self {

        let num_disks = blockdevs.len() as i16;
        let codec = Codec::new(disks_per_stripe as u32, redundancy as u32);
        let locator: Box<Locator> = match layout_algorithm {
            LayoutAlgorithm::NullRaid => Box::new(
                NullRaid::new(num_disks, disks_per_stripe, redundancy)),
            LayoutAlgorithm::PrimeS => Box::new(
                PrimeS::new(num_disks, disks_per_stripe, redundancy))
        };
        for i in 1..blockdevs.len() {
            // All blockdevs must be the same size
            assert_eq!(blockdevs[0].size(), blockdevs[i].size());

            // All blockdevs must have the same zone boundaries
            // XXX this check assumes fixed-size zones
            assert_eq!(blockdevs[0].zone_limits(0),
                       blockdevs[i].zone_limits(0));
        }

        // NB: the optimum queue depth should actually be a little higher for
        // healthy reads than for writes or degraded reads.  This calculation
        // computes the optimum for writes and degraded reads.
        let optimum_queue_depth = blockdevs.iter().map(|bd| {
            bd.optimum_queue_depth()
        }).sum::<u32>() / (codec.stripesize() as u32);

        VdevRaid { chunksize, codec, locator, blockdevs, layout_algorithm,
                   optimum_queue_depth,
                   stripe_buffers: RefCell::new(BTreeMap::new()),
                   uuid, handle }   // LCOV_EXCL_LINE   kcov false negative
    }

    /// Open all existing `VdevRaid`s found in `paths`.
    ///
    /// Returns a vector of new `VdevRaid` objects and `LabelReader`s that may
    /// be used to construct other vdevs stacked on top of these.
    ///
    /// * `paths`:  Pathnames to search for the `VdevRaid`s.  All child devices
    ///             must be present.
    /// * `h`:      Handle to the Tokio reactor that will be used to service
    ///             this vdev.
    #[cfg(not(test))]
    pub fn open_all<P>(paths: Vec<P>, handle: Handle)
        -> Box<Future<Item=Vec<(Self, LabelReader)>, Error=Error>>
        where P: AsRef<Path> + 'static {

        let handle2 = handle.clone();
        // TODO: error handling for devices that don't exist or have no VdevFile
        // label
        Box::new(future::join_all(paths.into_iter().map(move |path| {
            VdevBlock::open(path, handle.clone())
        })).and_then(move |blockdevs| {
            let mut all_blockdevs = blockdevs.into_iter().map(|(bd, reader)| {
                (bd.uuid(), (bd, Some(reader)))
            }).collect::<BTreeMap<Uuid, (VdevBlock, Option<LabelReader>)>>();

            let all_raid_labels = all_blockdevs.iter_mut().map(|(_, v)| {
                let mut label_reader = v.1.take().unwrap();
                let label: Label = label_reader.deserialize().unwrap();
                (label, label_reader)
            }).unique_by(|v| v.0.uuid)
            .collect::<Vec<_>>();

            Ok(all_raid_labels.into_iter().filter_map(|(rlabel, label_reader)| {
                let mut blockdevs = Vec::with_capacity(rlabel.children.len());
                let num_disks = rlabel.children.len() as i16;
                for child_uuid in rlabel.children {
                    match all_blockdevs.remove(&child_uuid) {
                        Some(bd) => blockdevs.push(bd.0),
                        None => break,
                    }
                }
                if blockdevs.len() == num_disks as usize {
                    Some((VdevRaid::new(rlabel.chunksize,
                                      rlabel.disks_per_stripe,
                                      rlabel.redundancy,
                                      rlabel.uuid,
                                      rlabel.layout_algorithm,
                                      blockdevs.into_boxed_slice(),
                                      handle2.clone()),
                       label_reader))
                } else {
                    // Some block devices weren't found
                    None
                }
            }).collect::<Vec<_>>())
        }))
    }

    /// Asynchronously erase a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    pub fn erase_zone(&self, zone: ZoneT) -> Box<VdevFut> {
        assert!(!self.stripe_buffers.borrow().contains_key(&zone),
            "Tried to erase an open zone");
        let (start, end) = self.blockdevs[0].zone_limits(zone);
        let futs : Vec<_> = self.blockdevs.iter().map(|blockdev| {
            blockdev.erase_zone(start, end - 1)
        }).collect();
        Box::new(future::join_all(futs).map(|_| ()))
    }

    /// Asynchronously finish a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    // Zero-fill the current StripeBuffer and write it out.  Then drop the
    // StripeBuffer.
    pub fn finish_zone(&self, zone: ZoneT) -> Box<VdevFut> {
        let mut sbs = self.stripe_buffers.borrow_mut();
        let nfuts = self.blockdevs.len() + 1;
        let mut futs: Vec<Box<VdevFut>> = Vec::with_capacity(nfuts);
        {
            let sb = sbs.get_mut(&zone).expect("Can't finish a closed zone");
            if ! sb.is_empty() {
                sb.pad();
                let lba = sb.lba();
                let sgl = sb.pop();
                futs.push(self.writev_at_one(&sgl, lba));
            }   // LCOV_EXCL_LINE   kcov false negative
            let (start, end) = self.blockdevs[0].zone_limits(zone);
            futs.extend(
                Box::new(
                    self.blockdevs.iter()
                    .map(|blockdev| {
                        blockdev.finish_zone(start, end - 1)
                    })
                )
            );
        }

        assert!(sbs.remove(&zone).is_some());

        Box::new(future::join_all(futs).map(|_| ()))
    }

    /// Asynchronously flush the `StripeBuffer` for the given zone.
    ///
    /// # Returns
    /// The number of LBAs that were zero-filled, and `Future` that will
    /// complete when the zone's contents are fully written
    pub fn flush_zone(&self, zone: ZoneT) -> (LbaT, Box<VdevFut>) {
        // Flushing a partially written zone to disk requires zero-filling the
        // StripeBuffer so the parity be correct
        let mut sb_ref = self.stripe_buffers.borrow_mut();
        let sb_opt = sb_ref.get_mut(&zone);
        match sb_opt {
            None => {
                // This zone isn't open, or the buffer is empty.  Nothing to do!
                (0, Box::new(future::ok::<(), Error>(())))
            },
            Some(sb) => {
                if sb.is_empty() {
                    //Nothing to do!
                    (0, Box::new(future::ok::<(), Error>(())))
                } else {
                    let pad_lbas = sb.pad();
                    let lba = sb.lba();
                    let sgl = sb.pop();
                    (pad_lbas, self.writev_at_one(&sgl, lba))
                }
            }
        }
    }

    /// Asynchronously open a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    // Create a new StripeBuffer, and zero fill and leading wasted space
    pub fn open_zone(&self, zone: ZoneT) -> Box<VdevFut> {
        let f = self.codec.protection();
        let m = (self.codec.stripesize() - f) as LbaT;
        let stripe_lbas = m * self.chunksize as LbaT;
        let (start_lba, _) = self.zone_limits(zone);
        let sb = StripeBuffer::new(start_lba, stripe_lbas);
        assert!(self.stripe_buffers.borrow_mut().insert(zone, sb).is_none());

        let (first_disk_lba, _) = self.blockdevs[0].zone_limits(zone);
        let start_disk_chunk = div_roundup(first_disk_lba, self.chunksize);
        let futs: Vec<_> = self.blockdevs.iter()
            .enumerate()
            .map(|(idx, blockdev)| {
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
                let zero_fut = if first_usable_disk_lba > first_disk_lba {
                    // Zero-fill leading wasted space so as not to cause a
                    // write pointer violation on SMR disks.
                    let zero_lbas = first_usable_disk_lba - first_disk_lba;
                    let zero_len = zero_lbas as usize * BYTES_PER_LBA;
                    let sglist = zero_sglist(zero_len);
                    blockdev.writev_at(sglist, first_disk_lba)
                } else {
                    Box::new(future::ok::<(), Error>(()))
                };

                blockdev.open_zone(first_disk_lba).join(zero_fut)
            }).collect();

        Box::new(future::join_all(futs).map(|_| ()))
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Returns `()` on success, or an error on failure
    pub fn read_at(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let f = self.codec.protection();
        let m = (self.codec.stripesize() - f) as LbaT;
        assert_eq!(buf.len() % BYTES_PER_LBA, 0, "reads must be LBA-aligned");

        // end_lba is inclusive.  The highest LBA from which data will be read
        let mut end_lba = lba + ((buf.len() - 1) / BYTES_PER_LBA) as LbaT;
        let sb_ref = self.stripe_buffers.borrow();

        // Look for a StripeBuffer that contains part of the requested range.
        // There should only be a few zones open at any one time, so it's ok to
        // search through them all.
        let stripe_buffer = sb_ref.iter()
            .map(|(_, sb)| sb)
            .filter(|&stripe_buffer| {
                !stripe_buffer.is_empty() &&
                lba < stripe_buffer.next_lba() &&
                end_lba >= stripe_buffer.lba()
            }).nth(0);

        let buf2 = if stripe_buffer.is_some() &&
            !stripe_buffer.unwrap().is_empty() &&
            end_lba >= stripe_buffer.unwrap().lba() {

            // We need to service part of the read from the StripeBuffer
            let mut cursor = SGCursor::from(stripe_buffer.unwrap().peek());
            let direct_len = if stripe_buffer.unwrap().lba() > lba {
                let direct_len = (stripe_buffer.unwrap().lba() - lba) as usize *
                                 BYTES_PER_LBA;
                direct_len
            } else {
                // Seek to the LBA of interest
                let mut skipped = 0;
                let to_skip = (lba - stripe_buffer.unwrap().lba()) as usize *
                    BYTES_PER_LBA;
                while skipped < to_skip {
                    let iovec = cursor.next(to_skip - skipped);
                    skipped += iovec.unwrap().len();
                }
                0
            };
            let mut sb_buf = buf.split_off(direct_len);
            // Copy from StripeBuffer into sb_buf
            while sb_buf.len() > 0 {
                let iovec = cursor.next(sb_buf.len()).expect(
                    "Read beyond the stripe buffer into unallocated space");
                sb_buf.split_to(iovec.len())[..].copy_from_slice(&iovec[..]);
            }
            if direct_len == 0 {
                // Read was fully serviced by StripeBuffer.  No need to go to
                // disks.
                return Box::new(future::ok(()));
            } else {
                // Service the first part of the read from the disks
                end_lba = stripe_buffer.unwrap().lba() - 1;
            }
            buf
        } else {
            // Don't involve the StripeBuffer
            buf
        };
        let start_stripe = lba / (self.chunksize * m as LbaT);
        let end_stripe = end_lba / (self.chunksize * m);
        if start_stripe == end_stripe {
            self.read_at_one(buf2, lba)
        } else {
            self.read_at_multi(buf2, lba)
        }
    }

    /// Read more than one whole stripe
    fn read_at_multi(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let n = self.blockdevs.len();
        debug_assert_eq!(buf.len() % BYTES_PER_LBA, 0);
        let lbas = (buf.len() / BYTES_PER_LBA) as LbaT;
        let chunks = div_roundup(buf.len(), col_len);

        // Create an SGList for each disk.
        let mut sglists = Vec::<SGListMut>::with_capacity(n);
        const SENTINEL : LbaT = LbaT::max_value();
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let mut next_lbas : Vec<LbaT> = vec![SENTINEL; n];
        let max_chunks_per_disk = self.locator.parallel_read_count(chunks);
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            sglists.push(SGListMut::with_capacity(max_chunks_per_disk));
        }
        // Build the SGLists, one chunk at a time
        let max_futs = n * max_chunks_per_disk;
        let mut futs: Vec<Box<VdevFut>> = Vec::with_capacity(max_futs);
        let start = ChunkId::Data(lba / self.chunksize);
        let end = ChunkId::Data(div_roundup(lba + lbas, self.chunksize));
        let mut starting = true;
        for (_, loc) in self.locator.iter_data(start, end) {
            let (col, disk_lba) = if starting && lba % self.chunksize != 0 {
                // The operation begins mid-chunk
                starting = false;
                let lbas_into_chunk = lba % self.chunksize;
                let chunk0lbas = self.chunksize - lbas_into_chunk;
                let chunk0size = chunk0lbas as usize * BYTES_PER_LBA;
                let col = buf.split_to(chunk0size);
                let disk_lba = loc.offset * self.chunksize + lbas_into_chunk;
                (col, disk_lba)
            } else {    // LCOV_EXCL_LINE   kcov false negative
                let chunklen = cmp::min(buf.len(), col_len);
                let col = buf.split_to(chunklen);
                let disk_lba = loc.offset * self.chunksize;
                (col, disk_lba)
            };  // LCOV_EXCL_LINE   kcov false negative
            let disk = loc.disk as usize;
            if start_lbas[disk as usize] == SENTINEL {
                // First chunk assigned to this disk
                start_lbas[disk as usize] = disk_lba;
            } else if next_lbas[disk as usize] < disk_lba {
                // There must've been a parity chunk on this disk, which we
                // skipped.  Fire off a readv_at and keep going
                let new = SGListMut::with_capacity(max_chunks_per_disk - 1);
                let old = mem::replace(&mut sglists[disk], new);
                let lba = start_lbas[disk];
                futs.push(self.blockdevs[disk].readv_at(old, lba));
                start_lbas[disk] = disk_lba;
            }
            sglists[disk as usize].push(col);
            next_lbas[disk as usize] = disk_lba + self.chunksize;
        }

        futs.extend(multizip((self.blockdevs.iter(),
                              sglists.into_iter(),
                              start_lbas.into_iter()))  // LCOV_EXCL_LINE   kcov false neg
            .filter(|&(_, _, lba)| lba != SENTINEL)
            .map(|(blockdev, sglist, lba)| blockdev.readv_at(sglist, lba)));
        let fut = future::join_all(futs);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, possibly fault a drive,
        // request the faulty drive's zone to be rebuilt, and read parity to
        // reconstruct the data.
        Box::new(fut.map(|_| { () }))
    }

    /// Read a (possibly improper) subset of one stripe
    fn read_at_one(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;

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

        let fut = issue_1stripe_ops!(self, data, lba, false, read_at);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, possibly fault a drive,
        // request the faulty drive's zone to be rebuilt, and read parity to
        // reconstruct the data.
        Box::new(fut.map(|_| { () }))
    }

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns `()` on success, or an error on failure
    pub fn write_at(&self, buf: IoVec, zone: ZoneT,
                    mut lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;
        let stripe_len = col_len * m;
        assert_eq!(buf.len() % BYTES_PER_LBA, 0, "Writes must be LBA-aligned");
        debug_assert_eq!(zone, self.lba2zone(lba).unwrap());
        let mut sb_ref = self.stripe_buffers.borrow_mut();
        let stripe_buffer = sb_ref.get_mut(&zone)
                                  .expect("Can't write to a closed zone");
        assert_eq!(stripe_buffer.next_lba(), lba);

        let mut sb_fut = None;
        let mut buf3 = if !stripe_buffer.is_empty() ||
            buf.len() < stripe_len {

            let buflen = buf.len();
            let buf2 = stripe_buffer.fill(buf);
            if stripe_buffer.is_full() {
                let stripe_lba = stripe_buffer.lba();
                let sglist = stripe_buffer.pop();
                lba += ((buflen - buf2.len()) / BYTES_PER_LBA) as LbaT;
                sb_fut = Some(self.writev_at_one(&sglist, stripe_lba));
                if buf2.is_empty() {
                    //Special case: if we fully consumed buf, then return
                    //immediately
                    return Box::new(sb_fut.unwrap());
                } else {
                    buf2
                }
            } else {
                // We didn't have enough data to fill the StripeBuffer, so
                // return early
                return Box::new(future::ok(()));
            }
        } else {
            buf
        };
        debug_assert!(stripe_buffer.is_empty());
        let nstripes = buf3.len() / stripe_len;
        let writable_buf = buf3.split_to(nstripes * stripe_len);
        stripe_buffer.reset(lba + (nstripes * m) as LbaT * self.chunksize);
        if ! buf3.is_empty() {
            let buf4 = stripe_buffer.fill(buf3);
            debug_assert!(!stripe_buffer.is_full());
            debug_assert!(buf4.is_empty());
        }
        let fut = if nstripes == 1 {
            self.write_at_one(writable_buf, lba)
        } else {
            self.write_at_multi(writable_buf, lba)
        };
        if sb_fut.is_some() {
            Box::new(sb_fut.unwrap().join(fut).map(|_| ()))
        } else {
            Box::new(fut)
        }
    }

    /// Write two or more whole stripes
    fn write_at_multi(&self, mut buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let k = self.codec.stripesize() as usize;
        let m = k - f as usize;
        let n = self.blockdevs.len();
        let chunks = buf.len() / col_len;
        let stripes = chunks / m;

        // Allocate storage for parity for the entire operation
        let mut parity = Vec::<IoVecMut>::with_capacity(f);
        let mut parity_dbses = Vec::<DivBufShared>::with_capacity(f);
        for _ in 0..f {
            let mut v = Vec::<u8>::with_capacity(stripes * col_len);
            //codec::encode will actually fill the column
            unsafe { v.set_len(stripes * col_len) };
            let col = DivBufShared::from(v);
            let mut dbm = col.try_mut().unwrap();
            parity.push(dbm);
            parity_dbses.push(col);
        }

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
                for p in 0..f {
                    let begin = s * col_len;
                    let end = (s + 1) * col_len;
                    parity_refs[p] = parity[p][begin..end].as_mut_ptr();
                }
            }
            self.codec.encode(col_len, &data_refs, &parity_refs);
        }

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
                ChunkId::Parity(_, ref i) =>
                    parity[*i as usize].split_to(col_len).freeze()
            };
            let disk_lba = loc.offset * self.chunksize;
            if start_lbas[loc.disk as usize] == SENTINEL {
                start_lbas[loc.disk as usize] = disk_lba;
            } else {
                debug_assert!(start_lbas[loc.disk as usize] < disk_lba);
            }
            sglists[loc.disk as usize].push(col);
        }

        let bi = self.blockdevs.iter();
        let sgi = sglists.into_iter();
        let li = start_lbas.into_iter();
        let futs : Vec<Box<VdevFut>> = multizip((bi, sgi, li))
            .filter(|&(_, _, lba)| lba != SENTINEL)
            .map(|(blockdev, sglist, lba)| blockdev.writev_at(sglist, lba))
            .collect();
        let fut = future::join_all(futs);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, possibly fault a drive,
        // request the faulty drive's zone to be rebuilt, and read parity to
        // reconstruct the data.
        Box::new(fut.map(move |_| {
            let _ = parity_dbses;   // Needs to live this long
            ()
        }))
    }

    /// Write exactly one stripe
    fn write_at_one(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;

        let dcols : Vec<IoVec> = buf.into_chunks(col_len).collect();
        let drefs : Vec<*const u8> = dcols.iter().map(|d| d.as_ptr()).collect();
        debug_assert_eq!(dcols.len(), m);

        let mut parity = Vec::<IoVecMut>::with_capacity(f);
        let mut prefs = Vec::<*mut u8>::with_capacity(f);
        let mut parity_dbses = Vec::<DivBufShared>::with_capacity(f);
        for _ in 0..f {
            let mut v = Vec::<u8>::with_capacity(col_len);
            //codec::encode will actually fill the column
            unsafe { v.set_len(col_len) };
            let col = DivBufShared::from(v);
            let mut dbm = col.try_mut().unwrap();
            prefs.push(dbm.as_mut_ptr());
            parity.push(dbm);
            parity_dbses.push(col);
        }

        self.codec.encode(col_len, &drefs, &prefs);
        let pw = parity.into_iter().map(|p| p.freeze());

        let data_fut = issue_1stripe_ops!(self, dcols, lba, false, write_at);
        let parity_fut = issue_1stripe_ops!(self, pw, lba, true, write_at);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, and possibly fault a drive.
        Box::new(data_fut.join(parity_fut).map(move |_| {
            let _ = parity_dbses;   // Needs to live this long
        }))
    }

    /// Asynchronously write this Vdev's label to all component devices
    pub fn write_label(&self, mut labeller: LabelWriter) -> Box<VdevFut> {
        let children_uuids = self.blockdevs.iter().map(|bd| bd.uuid())
            .collect::<Vec<_>>();
        let label = Label {
            uuid: self.uuid,
            chunksize: self.chunksize,
            disks_per_stripe: self.locator.stripesize(),
            redundancy: self.locator.protection(),
            layout_algorithm: self.layout_algorithm,
            children: children_uuids
        };
        let dbs = labeller.serialize(label);
        let futs = self.blockdevs.iter().map(|bd| {
            bd.write_label(labeller.clone())
        }).collect::<Vec<_>>();
        Box::new(future::join_all(futs).map(move |_| {
            let _ = dbs;    // needs to live this long
        }))
    }

    /// Write exactly one stripe, with SGLists.
    ///
    /// This is mostly useful internally, for writing from the stripe buffer.
    /// It should not be used publicly.
    #[doc(hidden)]
    pub fn writev_at_one(&self, buf: &SGList, lba: LbaT) -> Box<VdevFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;

        let mut dcols = Vec::<SGList>::with_capacity(m);
        let mut dcursor = SGCursor::from(buf);
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
        debug_assert!(dcursor.next(usize::max_value()).is_none());

        let mut pcols = Vec::<IoVecMut>::with_capacity(f);
        let mut prefs = Vec::<*mut u8>::with_capacity(f);
        let mut parity_dbses = Vec::<DivBufShared>::with_capacity(f);
        for _ in 0..f {
            let mut v = Vec::<u8>::with_capacity(col_len);
            //codec::encode will actually fill the column
            unsafe { v.set_len(col_len) };
            let col = DivBufShared::from(v);
            let mut dbm = col.try_mut().unwrap();
            prefs.push(dbm.as_mut_ptr());
            pcols.push(dbm);
            parity_dbses.push(col);
        }

        self.codec.encodev(col_len, &dcols, &mut pcols);
        let pw = pcols.into_iter().map(|p| p.freeze());

        let data_fut = issue_1stripe_ops!(self, dcols, lba, false, writev_at);
        let parity_fut = issue_1stripe_ops!(self, pw, lba, true, write_at);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, and possibly fault a drive.
        Box::new(data_fut.join(parity_fut).map(move |_| {
            let _ = parity_dbses;   // Needs to live this long
            ()
        }))
    }
}

/// Helper function that returns both the minimum and the maximum element of the
/// iterable.
fn min_max<I>(iterable: I) -> Option<(I::Item, I::Item)>
    where I: Iterator, I::Item: Ord + Copy {
    iterable.fold(None, |acc, i| {
        if acc.is_none() {
            Some((i, i))
        } else {
            Some((cmp::min(acc.unwrap().0, i), cmp::max(acc.unwrap().1, i)))
        }
    })
}

impl Vdev for VdevRaid {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        let loc = self.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        let tentative = self.blockdevs[loc.disk as usize].lba2zone(disk_lba);
        if tentative.is_none() {
            return None;
        }
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
        let disk_size_in_chunks = self.blockdevs[0].size() / self.chunksize;
        disk_size_in_chunks * self.locator.datachunks() *
            self.chunksize / (self.locator.depth() as LbaT)
    }

    fn sync_all(&self) -> Box<Future<Item = (), Error = Error>> {
        #[cfg(debug_assertions)]
        // Don't flush zones ourselves; the Cluster layer must be in charge of
        // that, so it can update the spacemap.
        {
            for sb in self.stripe_buffers.borrow().values() {
                assert!(sb.is_empty(), "Must call flush_zone before sync_all");
            }
        }
        Box::new(
            future::join_all(
                self.blockdevs.iter()
                .map(|bd| bd.sync_all())
                .collect::<Vec<_>>()
            ).map(|_| ())
        )
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

        // 1) All blockdevs must have the same zone map, so we only need to do
        //    the zone_limits call once.
        let (disk_lba_b, disk_lba_e) = self.blockdevs[0].zone_limits(zone);
        let disk_chunk_b = div_roundup(disk_lba_b, self.chunksize);
        let disk_chunk_e = disk_lba_e / self.chunksize - 1; //inclusive endpoint

        let endpoint_lba = |boundary_chunk, is_highend| {
            // 2) Find the lowest and highest stripe
            let stripes = (0..self.blockdevs.len()).map(|i| {
                let cid = self.locator.loc2id(Chunkloc::new(i as i16,
                                                            boundary_chunk));
                let stripe = cid.address() / m;
                stripe
            });
            let (min_stripe, max_stripe) = min_max(stripes).unwrap();

            // 3,4) Find stripes that cross zones.  Return the innermost that
            // doesn't
            let mut innermost_stripe = None;
            'stripe_loop: for stripe in min_stripe..max_stripe + 1 {
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
        self.blockdevs[0].zones()
    }
}


// LCOV_EXCL_START
#[test]
fn test_min_max() {
    let empty: Vec<u8> = Vec::with_capacity(0);
    assert_eq!(min_max(empty.iter()), None);
    assert_eq!(min_max(vec![42u8].iter()), Some((&42, &42)));
    assert_eq!(min_max(vec![1u32, 2u32, 3u32].iter()), Some((&1, &3)));
    assert_eq!(min_max(vec![0i8, -9i8, 18i8, 1i8].iter()), Some((&-9, &18)));
}

#[test]
fn stripe_buffer_empty() {
    let mut sb = StripeBuffer::new(99, 6);
    assert!(!sb.is_full());
    assert!(sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 99);
    assert_eq!(sb.len(), 0);
    assert!(sb.peek().is_empty());
    let sglist = sb.pop();
    assert!(sglist.is_empty());
    // Adding an empty iovec should change nothing, but add a useless sender
    let dbs = DivBufShared::from(vec![0; 4096]);
    let db = dbs.try().unwrap();
    let db0 = db.slice(0, 0);
    assert!(sb.fill(db0).is_empty());
    assert!(!sb.is_full());
    assert!(sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 99);
    assert_eq!(sb.len(), 0);
    assert!(sb.peek().is_empty());
    let sglist = sb.pop();
    assert!(sglist.is_empty());
}

#[test]
fn stripe_buffer_fill_when_full() {
    let dbs0 = DivBufShared::from(vec![0; 24576]);
    let db0 = dbs0.try().unwrap();
    let dbs1 = DivBufShared::from(vec![1; 4096]);
    let db1 = dbs1.try().unwrap();
    {
        let mut sb = StripeBuffer::new(99, 6);
        assert!(sb.fill(db0).is_empty());
        assert_eq!(sb.fill(db1).len(), 4096);
        assert!(sb.is_full());
        assert_eq!(sb.lba(), 99);
        assert_eq!(sb.next_lba(), 105);
        assert_eq!(sb.len(), 6);
    }
}

#[test]
fn stripe_buffer_one_iovec() {
    let mut sb = StripeBuffer::new(99, 6);
    let dbs = DivBufShared::from(vec![0; 4096]);
    let db = dbs.try().unwrap();
    assert!(sb.fill(db).is_empty());
    assert!(!sb.is_full());
    assert!(!sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 100);
    assert_eq!(sb.len(), 1);
    {
        let sglist = sb.peek();
        assert_eq!(sglist.len(), 1);
        assert_eq!(&sglist[0][..], &vec![0; 4096][..]);
    }
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 1);
    assert_eq!(&sglist[0][..], &vec![0; 4096][..]);
}

// Pad a StripeBuffer that is larger than the ZERO_REGION
#[test]
fn stripe_buffer_pad() {
    let zero_region_lbas = (ZERO_REGION.len() / BYTES_PER_LBA) as LbaT;
    let stripesize = 2 * zero_region_lbas + 1;
    let mut sb = StripeBuffer::new(99, stripesize);
    let dbs = DivBufShared::from(vec![0; BYTES_PER_LBA]);
    let db = dbs.try().unwrap();
    assert!(sb.fill(db).is_empty());
    assert!(sb.pad() == stripesize - 1);
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 3);
    assert_eq!(sglist.iter().map(|v| v.len()).sum::<usize>(),
               stripesize as usize * BYTES_PER_LBA);
}

#[test]
fn stripe_buffer_reset() {
    let mut sb = StripeBuffer::new(99, 6);
    assert_eq!(sb.lba(), 99);
    sb.reset(111);
    assert_eq!(sb.lba(), 111);
}

#[test]
#[should_panic(expected = "A StripeBuffer with data cannot be moved")]
fn stripe_buffer_reset_nonempty() {
    let mut sb = StripeBuffer::new(99, 6);
    let dbs = DivBufShared::from(vec![0; 4096]);
    let db = dbs.try().unwrap();
    let _ = sb.fill(db);
    sb.reset(111);
}

#[test]
fn stripe_buffer_two_iovecs() {
    let mut sb = StripeBuffer::new(99, 6);
    let dbs0 = DivBufShared::from(vec![0; 8192]);
    let db0 = dbs0.try().unwrap();
    assert!(sb.fill(db0).is_empty());
    let dbs1 = DivBufShared::from(vec![1; 4096]);
    let db1 = dbs1.try().unwrap();
    assert!(sb.fill(db1).is_empty());
    assert!(!sb.is_full());
    assert!(!sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 102);
    assert_eq!(sb.len(), 3);
    {
        let sglist = sb.peek();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &vec![0; 8192][..]);
        assert_eq!(&sglist[1][..], &vec![1; 4096][..]);
    }
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 2);
    assert_eq!(&sglist[0][..], &vec![0; 8192][..]);
    assert_eq!(&sglist[1][..], &vec![1; 4096][..]);
}

#[test]
fn stripe_buffer_two_iovecs_overflow() {
    let mut sb = StripeBuffer::new(99, 6);
    let dbs0 = DivBufShared::from(vec![0; 16384]);
    let db0 = dbs0.try().unwrap();
    assert!(sb.fill(db0).is_empty());
    let dbs1 = DivBufShared::from(vec![1; 16384]);
    let db1 = dbs1.try().unwrap();
    assert_eq!(sb.fill(db1).len(), 8192);
    assert!(sb.is_full());
    assert!(!sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 105);
    assert_eq!(sb.len(), 6);
    {
        let sglist = sb.peek();
        assert_eq!(sglist.len(), 2);
        assert_eq!(&sglist[0][..], &vec![0; 16384][..]);
        assert_eq!(&sglist[1][..], &vec![1; 8192][..]);
    }
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 2);
    assert_eq!(&sglist[0][..], &vec![0; 16384][..]);
    assert_eq!(&sglist[1][..], &vec![1; 8192][..]);
}

#[cfg(feature = "mocks")]
#[cfg(test)]
mod t {

use super::*;
use futures::future;
use mockers::Scenario;

mock!{
    MockVdevBlock,
    vdev,
    trait Vdev {
        fn handle(&self) -> Handle;
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> Box<Future<Item = (), Error = Error>>;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    },
    self,
    trait VdevBlockTrait{
        fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut> ;
        fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevFut> ;
        fn open_zone(&self, lba: LbaT) -> Box<VdevFut> ;
        fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
        fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevFut>;
        fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
        fn write_label(&self, labeller: LabelWriter) -> Box<VdevFut>;
        fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
    }
}

test_suite! {
    // Test basic layout properties
    name basic;

    use super::super::*;
    use super::MockVdevBlock;
    use mockers::{matchers, Scenario};

    fixture!( mocks(n: i16, k: i16, f:i16, chunksize: LbaT)
              -> (Scenario, VdevRaid) {

        setup(&mut self) {
            let s = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..*self.n {
                let mock = Box::new(s.create_mock::<MockVdevBlock>());
                s.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                s.expect(mock.lba2zone_call(0)
                                    .and_return_clone(None)
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(1..65536))
                                    .and_return_clone(Some(0))
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(Some(1))
                                    .times(..));
                s.expect(mock.optimum_queue_depth_call()
                                    .and_return_clone(10)
                                    .times(..));
                s.expect(mock.zone_limits_call(0)
                                    .and_return_clone((1, 65536))
                                    .times(..));
                s.expect(mock.zone_limits_call(1)
                                    // 64k LBAs/zone
                                    .and_return_clone((65536, 131072))
                                    .times(..));

                blockdevs.push(mock);
            }

            let vdev_raid = VdevRaid::new(*self.chunksize, *self.k,
                                          *self.f, Uuid::new_v4(),
                                          LayoutAlgorithm::PrimeS,
                                          blockdevs.into_boxed_slice(),
                                          Handle::default());
            (s, vdev_raid)
        }
    });

    test small(mocks((5, 4, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), None);
        assert_eq!(mocks.val.1.lba2zone(95), None);
        assert_eq!(mocks.val.1.lba2zone(96), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245759), Some(0));
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245760), Some(1));

        assert_eq!(mocks.val.1.optimum_queue_depth(), 12);

        assert_eq!(mocks.val.1.size(), 983040);

        assert_eq!(mocks.val.1.zone_limits(0), (96, 245760));
        assert_eq!(mocks.val.1.zone_limits(1), (245760, 491520));
    }

    test medium(mocks((7, 4, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), None);
        assert_eq!(mocks.val.1.lba2zone(95), None);
        assert_eq!(mocks.val.1.lba2zone(96), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(344063), Some(0));
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(344064), Some(1));

        assert_eq!(mocks.val.1.optimum_queue_depth(), 17);

        assert_eq!(mocks.val.1.size(), 1376256);

        assert_eq!(mocks.val.1.zone_limits(0), (96, 344064));
        assert_eq!(mocks.val.1.zone_limits(1), (344064, 688128));
    }

    // A layout whose depth does not evenly divide the zone size.  The zone size
    // is not even a multiple of this layout's iterations.  So, it has a gap of
    // unused LBAs between zones
    test has_gap(mocks((7, 5, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), None);
        assert_eq!(mocks.val.1.lba2zone(127), None);
        assert_eq!(mocks.val.1.lba2zone(128), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(366975), Some(0));
        // An LBA in between zones 0 and 1
        assert_eq!(mocks.val.1.lba2zone(366976), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(367040), Some(1));

        assert_eq!(mocks.val.1.optimum_queue_depth(), 14);

        assert_eq!(mocks.val.1.size(), 1468006);

        assert_eq!(mocks.val.1.zone_limits(0), (128, 366976));
        assert_eq!(mocks.val.1.zone_limits(1), (367040, 733952));
    }

    // A layout whose depth does not evenly divide the zone size and has
    // multiple whole stripes per row.  So, it has a gap of multiple stripes
    // between zones.
    test has_multistripe_gap(mocks((11, 3, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), None);
        assert_eq!(mocks.val.1.lba2zone(159), None);
        assert_eq!(mocks.val.1.lba2zone(160), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(480511), Some(0));
        // LBAs in between zones 0 and 1
        assert_eq!(mocks.val.1.lba2zone(480512), None);
        assert_eq!(mocks.val.1.lba2zone(480639), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(480640), Some(1));

        assert_eq!(mocks.val.1.size(), 1922389);

        assert_eq!(mocks.val.1.zone_limits(0), (160, 480512));
        assert_eq!(mocks.val.1.zone_limits(1), (480640, 961152));
    }

    // A layout whose chunksize does not evenly divide the zone size.  One or
    // more entire rows must be skipped
    test misaligned_chunksize(mocks((5, 4, 1, 5))) {
        assert_eq!(mocks.val.1.lba2zone(0), None);
        assert_eq!(mocks.val.1.lba2zone(29), None);
        assert_eq!(mocks.val.1.lba2zone(30), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245744), Some(0));
        // LBAs in the zone 0-1 gap
        assert_eq!(mocks.val.1.lba2zone(245745), None);
        assert_eq!(mocks.val.1.lba2zone(245774), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245775), Some(1));

        assert_eq!(mocks.val.1.size(), 983025);

        assert_eq!(mocks.val.1.zone_limits(0), (30, 245745));
        assert_eq!(mocks.val.1.zone_limits(1), (245775, 491505));
    }
}

// Use mock VdevBlock objects to test that RAID reads hit the right LBAs from
// the individual disks.  Ignore the actual data values, since we don't have
// real VdevBlocks.  Functional testing will verify the data.
#[test]
fn read_at_one_stripe() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let m0 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m0.size_call().and_return_clone(262144).times(..));
        s.expect(m0.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m0.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m0.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m0.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m1.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m1.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m1.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));
        s.expect(m1.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 65536)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m2.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m2.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m2.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));
        s.expect(m2.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 65536)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );
        blockdevs.push(m2);

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      blockdevs.into_boxed_slice(),
                                      Handle::default());
        vdev_raid.open_zone(1);
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let rbuf = dbs.try_mut().unwrap();
        vdev_raid.read_at(rbuf, 131072);
}

#[test]
fn sync_all() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.sync_all_call()
                 .and_return(Box::new(future::ok::<(), Error>(())))
        );
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    let bd1 = bd();
    let bd2 = bd();

    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());
    vdev_raid.sync_all();
}

// It's illegal to sync a VdevRaid without flushing its zones first
#[test]
#[should_panic(expected = "Must call flush_zone before sync_all")]
fn sync_all_unflushed() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);
    let zl1 = (60_000, 120_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.lba2zone_call(60_000).and_return_clone(Some(1)).times(..));
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
        s.expect(bd.open_zone_call(60_000).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(bd.sync_all_call()
                 .and_return(Box::new(future::ok::<(), Error>(())))
        );
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    let bd1 = bd();
    let bd2 = bd();

    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());

    vdev_raid.open_zone(1);
    let dbs = DivBufShared::from(vec![1u8; 4096]);
    let wbuf = dbs.try().unwrap();
    vdev_raid.write_at(wbuf, 1, 120_000);
    // Don't flush zone 1 before syncing.  Syncing should panic
    vdev_raid.sync_all();
}

// Use mock VdevBlock objects to test that RAID writes hit the right LBAs from
// the individual disks.  Ignore the actual data values, since we don't have
// real VdevBlocks.  Functional testing will verify the data.
#[test]
fn write_at_one_stripe() {
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let m0 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m0.size_call().and_return_clone(262144).times(..));
        s.expect(m0.lba2zone_call(65536).and_return_clone(Some(1)).times(..));
        s.expect(m0.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m0.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m0.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m0.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));
        s.expect(m0.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 65536)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.lba2zone_call(65536).and_return_clone(Some(1)).times(..));
        s.expect(m1.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m1.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m1.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m1.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));
        s.expect(m1.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 65536)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.lba2zone_call(65536).and_return_clone(Some(1)).times(..));
        s.expect(m2.open_zone_call(65536).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m2.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        s.expect(m2.zone_limits_call(0).and_return_clone((1, 65536)).times(..));
        s.expect(m2.zone_limits_call(1).and_return_clone((65536, 131072))
                 .times(..));
        s.expect(m2.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 65536)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );
        blockdevs.push(m2);

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      blockdevs.into_boxed_slice(),
                                      Handle::default());
        vdev_raid.open_zone(1);
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let wbuf = dbs.try().unwrap();
        vdev_raid.write_at(wbuf, 1, 131072);
}

// Partially written stripes should be flushed by flush_zone
#[test]
fn write_at_and_flush_zone() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);
    let zl1 = (60_000, 120_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.lba2zone_call(60_000).and_return_clone(Some(1)).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
        s.expect(bd.open_zone_call(60_000)
                 .and_return(Box::new(future::ok::<(), Error>(())))
        );
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    s.expect(bd0.writev_at_call(check!(|buf: &SGList| {
        // The first segment is user data
        &buf[0][..] == &vec![1u8; BYTES_PER_LBA][..] &&
        // Later segments are zero-fill from flush_zone
        &buf[1][..] == &vec![0u8; BYTES_PER_LBA][..]
    }), 60_000)
        .and_return( Box::new( future::ok::<(), Error>(())))
    );

    let bd1 = bd();
    // This write is from the zero-fill
    s.expect(bd1.writev_at_call(check!(|buf: &SGList| {
        &buf[0][..] == &vec![0u8; BYTES_PER_LBA][..] &&
        &buf[1][..] == &vec![0u8; BYTES_PER_LBA][..]
    }), 60_000)
        .and_return( Box::new( future::ok::<(), Error>(())))
    );

    // This write is generated parity
    let bd2 = bd();
    s.expect(bd2.write_at_call(check!(|buf: &IoVec| {
        // single disk parity is a simple XOR
        &buf[0..4096] == &vec![1u8; BYTES_PER_LBA][..] &&
        &buf[4096..8192] == &vec![0u8; BYTES_PER_LBA][..]
    }), 60_000)
        .and_return( Box::new( future::ok::<(), Error>(())))
    );

    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());
    vdev_raid.open_zone(1);
    let dbs = DivBufShared::from(vec![1u8; 4096]);
    let wbuf = dbs.try().unwrap();
    vdev_raid.write_at(wbuf, 1, 120_000);
    vdev_raid.flush_zone(1);
}

// Erase a zone.  VdevRaid doesn't care whether it still has allocated data;
// that's Cluster's job.  And VdevRaid doesn't care whether the zone is closed
// or empty; that's the VdevLeaf's job.
#[test]
fn erase_zone() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);
    let zl1 = (60_000, 120_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.lba2zone_call(60_000).and_return_clone(Some(1)).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
        s.expect(bd.erase_zone_call(1, 59_999)
                 .and_return(Box::new(future::ok::<(), Error>(())))
        );
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    let bd1 = bd();
    let bd2 = bd();
    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());
    vdev_raid.erase_zone(0);
}

// Flushing a closed zone is a no-op
#[test]
fn flush_zone_closed() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.lba2zone_call(60_000).and_return_clone(Some(1)).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    let bd1 = bd();
    let bd2 = bd();
    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());
    vdev_raid.flush_zone(0);
}

// Flushing an open zone is a no-op if the stripe buffer is empty
#[test]
fn flush_zone_empty_stripe_buffer() {
    let k = 3;
    let f = 1;
    const CHUNKSIZE: LbaT = 2;
    let zl0 = (1, 60_000);
    let zl1 = (60_000, 120_000);

    let s = Scenario::new();
    let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

    let bd = || {
        let bd = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(bd.size_call().and_return_clone(262144).times(..));
        s.expect(bd.lba2zone_call(60_000).and_return_clone(Some(1)).times(..));
        s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
        s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
        s.expect(bd.open_zone_call(60_000)
                 .and_return(Box::new(future::ok::<(), Error>(())))
        );
        s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                 .times(..));
        bd
    };

    let bd0 = bd();
    let bd1 = bd();
    let bd2 = bd();
    blockdevs.push(bd0);
    blockdevs.push(bd1);
    blockdevs.push(bd2);

    let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                  Uuid::new_v4(),
                                  LayoutAlgorithm::PrimeS,
                                  blockdevs.into_boxed_slice(),
                                  Handle::default());
    vdev_raid.open_zone(1);
    vdev_raid.flush_zone(1);
}

// Open a zone that has wasted leading space due to a chunksize misaligned with
// the zone size.
// Use highly unrealistic disks with 32 LBAs per zone
#[test]
fn open_zone_zero_fill_wasted_chunks() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 5;
        let zl0 = (1, 32);
        let zl1 = (32, 64);

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

        let bd = || {
            let bd = Box::new(s.create_mock::<MockVdevBlock>());
            s.expect(bd.size_call().and_return_clone(262144).times(..));
            s.expect(bd.lba2zone_call(1).and_return_clone(Some(0)).times(..));
            s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
            s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
            s.expect(bd.open_zone_call(32)
                     .and_return(Box::new(future::ok::<(), Error>(())))
            );
            s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                     .times(..));
            s.expect(bd.writev_at_call(check!(|sglist: &SGList| {
                let len = sglist.iter().map(|b| b.len()).sum::<usize>();
                len == 3 * BYTES_PER_LBA
            }), 32)
                .and_return( Box::new( future::ok::<(), Error>(())))
            );
            bd
        };

        blockdevs.push(bd());    //disk 0
        blockdevs.push(bd());    //disk 1
        blockdevs.push(bd());    //disk 2
        blockdevs.push(bd());    //disk 3
        blockdevs.push(bd());    //disk 4

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      blockdevs.into_boxed_slice(),
                                      Handle::default());
        vdev_raid.open_zone(1);
}

// Open a zone that has some leading wasted space.  Use mock VdevBlock objects
// to verify that the leading wasted space gets zero-filled.
// Use highly unrealistic disks with 32 LBAs per zone
#[test]
fn open_zone_zero_fill_wasted_stripes() {
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 1;
        let zl0 = (1, 32);
        let zl1 = (32, 64);

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

        let bd = |gap_chunks: LbaT| {
            let bd = Box::new(s.create_mock::<MockVdevBlock>());
            s.expect(bd.size_call().and_return_clone(262144).times(..));
            s.expect(bd.lba2zone_call(1).and_return_clone(Some(0)).times(..));
            s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
            s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
            s.expect(bd.open_zone_call(32)
                     .and_return(Box::new(future::ok::<(), Error>(())))
            );
            s.expect(bd.optimum_queue_depth_call().and_return_clone(10)
                     .times(..));
            if gap_chunks > 0 {
                s.expect(bd.writev_at_call(check!(move |sglist: &SGList| {
                    let gap_lbas = gap_chunks * CHUNKSIZE; 
                    let len = sglist.iter().map(|b| b.len()).sum::<usize>();
                    len == gap_lbas as usize * BYTES_PER_LBA
                }), 32)
                    .and_return( Box::new( future::ok::<(), Error>(())))
                );
            }
            bd
        };

        // On this layout, zone 1 begins at the third row in the repetition.
        // Stripes 2 and 3 are wasted, so disks 0, 1, 2, 4, and 5 have a wasted
        // LBA that needs zero-filling.  Disks 3 and 6 have no wasted LBA.
        blockdevs.push(bd(1));  //disk 0
        blockdevs.push(bd(2));  //disk 1
        blockdevs.push(bd(1));  //disk 2
        blockdevs.push(bd(0));  //disk 3
        blockdevs.push(bd(1));  //disk 4
        blockdevs.push(bd(1));  //disk 5
        blockdevs.push(bd(0));  //disk 6

        let vdev_raid = VdevRaid::new(CHUNKSIZE, k, f,
                                      Uuid::new_v4(),
                                      LayoutAlgorithm::PrimeS,
                                      blockdevs.into_boxed_slice(),
                                      Handle::default());
        vdev_raid.open_zone(1);
}
}
// LCOV_EXCL_START
