// vim: tw=80

use common::*;
use common::dva::*;
use common::vdev::*;
use common::vdev_block::*;
use common::raid::*;
use common::declust::*;
use divbuf::DivBufShared;
use futures::{Future, future};
use itertools::multizip;
use nix::Error;
use std::cell::RefCell;
use std::{cmp, mem, ptr};
use std::collections::BTreeMap;
use tokio::reactor::Handle;

#[cfg(test)]
/// Only exists so mockers can replace VdevBlock
pub trait VdevBlockTrait : Vdev {
    fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut>;
    fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut>;
    fn open_zone(&self, lba: LbaT) -> Box<VdevBlockFut>;
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevBlockFut>;
    fn readv_at(&self, buf: SGListMut, lba: LbaT) -> Box<VdevBlockFut>;
    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevBlockFut>;
    fn writev_at(&self, buf: SGList, lba: LbaT) -> Box<VdevBlockFut>;
}
#[cfg(test)]
pub type VdevBlockLike = Box<VdevBlockTrait>;
#[cfg(not(test))]
#[doc(hidden)]
pub type VdevBlockLike = VdevBlock;

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
        let old_sglist = mem::replace(&mut self.buf, new_sglist);
        old_sglist
    }

    /// Reset an empty `StripeBuffer` to point to a new stripe.
    ///
    /// It is illegal to call this method on a non-empty `StripeBuffer`
    pub fn reset(&mut self, lba: LbaT) {
        assert!(self.is_empty(), "A StripeBuffer with data cannot be moved");
        self.lba = lba;
    }
}

/// Future representing an any operation on a RAID vdev.
///
/// It's not always possible to know how much data was actually transacted (as
/// opposed to data + parity), so the return value is merely `()` on success, or
/// an error code on failure.
pub type VdevRaidFut = Future<Item = (), Error = Error>;

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
    locator: Box<Locator>,

    /// Underlying block devices.  Order is important!
    blockdevs: Box<[VdevBlockLike]>,

    /// In memory cache of data that has not yet been flushed to the block
    /// devices.
    ///
    /// We cache up to one stripe per zone before flushing it.  Cacheing entire
    /// stripes uses fewer resources than only cacheing the parity information.
    stripe_buffers: RefCell<BTreeMap<ZoneT, StripeBuffer>>,

    /// A convenient buffer prepopulated with zeros with a lifetime as long as
    /// the `VdevRaid`.
    // It would be nice to share this between multiple `VdevRaid`s, but how
    // would you calculate the maximum size?
    zero_region: DivBufShared
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
    pub fn new(chunksize: LbaT, codec: Codec, locator: Box<Locator>,
               blockdevs: Box<[VdevBlockLike]>) -> Self {
        assert_eq!(blockdevs.len(), locator.clustsize() as usize,
            "mismatched cluster size");
        assert_eq!(codec.stripesize(), locator.stripesize(),
            "mismatched stripe size");
        assert_eq!(codec.protection(), locator.protection(),
            "mismatched protection level");
        for i in 1..blockdevs.len() {
            // All blockdevs must be the same size
            assert_eq!(blockdevs[0].size(), blockdevs[i].size());

            // All blockdevs must have the same zone boundaries
            // XXX this check assumes fixed-size zones
            assert_eq!(blockdevs[0].zone_limits(0),
                       blockdevs[i].zone_limits(0));
        }

        let f = codec.protection();
        let m = (codec.stripesize() - f) as LbaT;
        let stripesize = (m * chunksize) as usize * BYTES_PER_LBA;
        let zero_region = DivBufShared::from(vec![0u8; stripesize]);

        VdevRaid { chunksize, codec, locator, blockdevs,
                   stripe_buffers: RefCell::new(BTreeMap::new()),
                   zero_region }
    }

    /// Asynchronously erase a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    pub fn erase_zone(&self, zone: ZoneT) -> Box<VdevRaidFut> {
        assert!(!self.stripe_buffers.borrow().contains_key(&zone),
            "Tried to erase an open zone");
        let (start, end) = self.blockdevs[0].zone_limits(zone);
        let futs : Vec<_> = self.blockdevs.iter().map(|blockdev| {
            blockdev.erase_zone(start, end)
        }).collect();
        Box::new(future::join_all(futs).map(|_| ()))
    }

    /// Asynchronously finish a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    // Zero-fill the current StripeBuffer and write it out.  Then drop the
    // StripeBuffer.
    pub fn finish_zone(&self, zone: ZoneT) -> Box<VdevRaidFut> {
        let mut sbs = self.stripe_buffers.borrow_mut();
        let nfuts = self.blockdevs.len() + 1;
        let mut futs: Vec<Box<VdevRaidFut>> = Vec::with_capacity(nfuts);
        {
            let sb = sbs.get_mut(&zone).expect("Can't finish a closed zone");
            if ! sb.is_empty() {
                let pad_lbas = (sb.stripe_lbas - sb.len()) as usize;
                let padsize = pad_lbas * BYTES_PER_LBA;
                let db = self.zero_region.try().unwrap().slice_to(padsize);
                sb.fill(db);
                debug_assert!(sb.is_full());
                let lba = sb.lba();
                let sgl = sb.pop();
                futs.push(self.writev_at_one(&sgl, lba));
            }
            let (start, end) = self.blockdevs[0].zone_limits(zone);
            futs.extend(
                Box::new(
                    self.blockdevs.iter()
                    .map(|blockdev| {
                        blockdev.finish_zone(start, end)
                    })
                )
            );
        }

        assert!(sbs.remove(&zone).is_some());

        Box::new(future::join_all(futs).map(|_| ()))
    }

    /// Asynchronously open a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    // Create a new StripeBuffer, and zero fill and leading wasted space
    pub fn open_zone(&self, zone: ZoneT) -> Box<VdevRaidFut> {
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
                    let db = self.zero_region.try().unwrap().slice_to(zero_len);
                    blockdev.write_at(db, first_disk_lba)
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
    pub fn read_at(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevRaidFut> {
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
            let direct_len = (stripe_buffer.unwrap().lba() - lba) as usize *
                             BYTES_PER_LBA;
            let mut sb_buf = buf.split_off(direct_len);
            // Copy from StripeBuffer into sb_buf
            for iovec in stripe_buffer.unwrap().peek() {
                sb_buf.split_to(iovec.len())[..].copy_from_slice(&iovec[..]);
            }
            assert!(sb_buf.is_empty(),
                "Read beyond the stripe buffer into unallocated space");
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
    fn read_at_multi(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevRaidFut> {
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
        let mut futs: Vec<Box<VdevBlockFut>> = Vec::with_capacity(max_futs);
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
            } else {
                let chunklen = cmp::min(buf.len(), col_len);
                let col = buf.split_to(chunklen);
                let disk_lba = loc.offset * self.chunksize;
                (col, disk_lba)
            };
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
                              start_lbas.into_iter()))
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
    fn read_at_one(&self, mut buf: IoVecMut, lba: LbaT) -> Box<VdevRaidFut> {
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
                    mut lba: LbaT) -> Box<VdevRaidFut> {
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
    fn write_at_multi(&self, mut buf: IoVec, lba: LbaT) -> Box<VdevRaidFut> {
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
            let col = match &chunk_id {
                &ChunkId::Data(_) => buf.split_to(col_len),
                &ChunkId::Parity(_, ref i) =>
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

        let futs : Vec<Box<VdevBlockFut>> = multizip((self.blockdevs.iter(),
                                                      sglists.into_iter(),
                                                      start_lbas.into_iter()))
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
    fn write_at_one(&self, buf: IoVec, lba: LbaT) -> Box<VdevRaidFut> {
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

    /// Write exactly one stripe, with SGLists.
    ///
    /// This is mostly useful internally, for writing from the stripe buffer.
    /// It should not be used publicly.
    #[doc(hidden)]
    pub fn writev_at_one(&self, buf: &SGList, lba: LbaT) -> Box<VdevRaidFut> {
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
        panic!("Unimplemented!  Perhaps handle() should not be part of Trait vdev, because it doesn't make sense for VdevRaid");
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        let loc = self.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        let tentative = self.blockdevs[loc.disk as usize].lba2zone(disk_lba);
        // NB: this call to zone_limits is slow, but unfortunately necessary.
        let limits = self.zone_limits(tentative.unwrap());
        if lba >= limits.0 && lba < limits.1 {
            tentative
        } else {
            None
        }
    }

    fn size(&self) -> LbaT {
        let disk_size_in_chunks = self.blockdevs[0].size() / self.chunksize;
        disk_size_in_chunks * self.locator.datachunks() *
            self.chunksize / (self.locator.depth() as LbaT)
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
                    } else if !is_highend && (loc.offset < boundary_chunk) {
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
}


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
use super::super::prime_s::PrimeS;
use futures::future;
use mockers::Scenario;

mock!{
    MockVdevBlock,
    vdev,
    trait Vdev {
        fn handle(&self) -> Handle;
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn size(&self) -> LbaT;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
    },
    self,
    trait VdevBlockTrait{
        fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut> ;
        fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut> ;
        fn open_zone(&self, lba: LbaT) -> Box<VdevBlockFut> ;
        fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevBlockFut>;
        fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevBlockFut>;
        fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevBlockFut>;
        fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevBlockFut>;
    }
}

#[test]
#[should_panic(expected="mismatched cluster size")]
fn vdev_raid_mismatched_clustsize() {
        let n = 7;
        let k = 4;
        let f = 1;

        let blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

#[test]
#[should_panic(expected="mismatched stripe size")]
fn vdev_raid_mismatched_stripesize() {
        let n = 7;
        let k = 4;
        let f = 1;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        for _ in 0..n {
            let mock = Box::new(s.create_mock::<MockVdevBlock>());
            blockdevs.push(mock);
        }
        let codec = Codec::new(5, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

#[test]
#[should_panic(expected="mismatched protection level")]
fn vdev_raid_mismatched_protection() {
        let n = 7;
        let k = 4;
        let f = 1;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        for _ in 0..n {
            let mock = Box::new(s.create_mock::<MockVdevBlock>());
            blockdevs.push(mock);
        }
        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, 2));
        VdevRaid::new(16, codec, locator, blockdevs.into_boxed_slice());
}

test_suite! {
    // Test basic layout properties
    name basic;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
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
                s.expect(mock.lba2zone_call(matchers::lt(65536))
                                    .and_return_clone(Some(0))
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(Some(1))
                                    .times(..));
                s.expect(mock.zone_limits_call(0)
                                    .and_return_clone((0, 65536))
                                    .times(..));
                s.expect(mock.zone_limits_call(1)
                                    // 64k LBAs/zone
                                    .and_return_clone((65536, 131072))
                                    .times(..));

                blockdevs.push(mock);
            }

            let codec = Codec::new(*self.k as u32, *self.f as u32);
            let locator = Box::new(PrimeS::new(*self.n, *self.k, *self.f));
            let vdev_raid = VdevRaid::new(*self.chunksize, codec, locator,
                                          blockdevs.into_boxed_slice());
            (s, vdev_raid)
        }
    });

    test small(mocks((5, 4, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245759), Some(0));
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245760), Some(1));

        assert_eq!(mocks.val.1.size(), 983040);

        assert_eq!(mocks.val.1.zone_limits(0), (0, 245760));
        assert_eq!(mocks.val.1.zone_limits(1), (245760, 491520));
    }

    test medium(mocks((7, 4, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(344063), Some(0));
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(344064), Some(1));

        assert_eq!(mocks.val.1.size(), 1376256);

        assert_eq!(mocks.val.1.zone_limits(0), (0, 344064));
        assert_eq!(mocks.val.1.zone_limits(1), (344064, 688128));
    }

    // A layout whose depth does not evenly divide the zone size.  The zone size
    // is not even a multiple of this layout's iterations.  So, it has a gap of
    // unused LBAs between zones
    test has_gap(mocks((7, 5, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(366975), Some(0));
        // An LBA in between zones 0 and 1
        assert_eq!(mocks.val.1.lba2zone(366976), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(367040), Some(1));

        assert_eq!(mocks.val.1.size(), 1468006);

        assert_eq!(mocks.val.1.zone_limits(0), (0, 366976));
        assert_eq!(mocks.val.1.zone_limits(1), (367040, 733952));
    }

    // A layout whose depth does not evenly divide the zone size and has
    // multiple whole stripes per row.  So, it has a gap of multiple stripes
    // between zones.
    test has_multistripe_gap(mocks((11, 3, 1, 16))) {
        assert_eq!(mocks.val.1.lba2zone(0), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(480511), Some(0));
        // LBAs in between zones 0 and 1
        assert_eq!(mocks.val.1.lba2zone(480512), None);
        assert_eq!(mocks.val.1.lba2zone(480639), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(480640), Some(1));

        assert_eq!(mocks.val.1.size(), 1922389);

        assert_eq!(mocks.val.1.zone_limits(0), (0, 480512));
        assert_eq!(mocks.val.1.zone_limits(1), (480640, 961152));
    }

    // A layout whose chunksize does not evenly divide the zone size.  One or
    // more entire rows must be skipped
    test misaligned_chunksize(mocks((5, 4, 1, 5))) {
        assert_eq!(mocks.val.1.lba2zone(0), Some(0));
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245744), Some(0));
        // LBAs in the zone 0-1 gap
        assert_eq!(mocks.val.1.lba2zone(245745), None);
        assert_eq!(mocks.val.1.lba2zone(245774), None);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245775), Some(1));

        assert_eq!(mocks.val.1.size(), 983025);

        assert_eq!(mocks.val.1.zone_limits(0), (0, 245745));
        assert_eq!(mocks.val.1.zone_limits(1), (245775, 491505));
    }
}

// Use mock VdevBlock objects to test that RAID reads hit the right LBAs from
// the individual disks.  Ignore the actual data values, since we don't have
// real VdevBlocks.  Functional testing will verify the data.
#[test]
fn read_at_one_stripe() {
        let n = 3;
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let m0 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m0.size_call().and_return_clone(262144).times(..));
        s.expect(m0.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m0.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m0.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        s.expect(m0.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m1.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m1.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        s.expect(m1.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m2.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m2.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        blockdevs.push(m2);

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        vdev_raid.open_zone(0);
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let rbuf = dbs.try_mut().unwrap();
        vdev_raid.read_at(rbuf, 0);
}

// Use mock VdevBlock objects to test that RAID writes hit the right LBAs from
// the individual disks.  Ignore the actual data values, since we don't have
// real VdevBlocks.  Functional testing will verify the data.
#[test]
fn write_at_one_stripe() {
        let n = 3;
        let k = 3;
        let f = 1;
        const CHUNKSIZE : LbaT = 2;

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
        let m0 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m0.size_call().and_return_clone(262144).times(..));
        s.expect(m0.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m0.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m0.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        s.expect(m0.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m1.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m1.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        s.expect(m1.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.lba2zone_call(0).and_return_clone(Some(0)).times(..));
        s.expect(m2.open_zone_call(0).and_return(
                Box::new(future::ok::<(), Error>(()))));
        s.expect(m2.zone_limits_call(0).and_return_clone((0, 65536)).times(..));
        s.expect(m2.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return( Box::new( future::ok::<(), Error>(())))
        );
        blockdevs.push(m2);

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        vdev_raid.open_zone(0);
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let wbuf = dbs.try().unwrap();
        vdev_raid.write_at(wbuf, 0, 0);
}

// Open a zone that has wasted leading space due to a chunksize misaligned with
// the zone size.
// Use highly unrealistic disks with 32 LBAs per zone
#[test]
fn open_zone_zero_fill_wasted_chunks() {
        let n = 5;
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 5;
        let zl0 = (0, 32);
        let zl1 = (32, 64);

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

        let bd = || {
            let bd = Box::new(s.create_mock::<MockVdevBlock>());
            s.expect(bd.size_call().and_return_clone(262144).times(..));
            s.expect(bd.lba2zone_call(0).and_return_clone(Some(0)).times(..));
            s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
            s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
            s.expect(bd.open_zone_call(32)
                     .and_return(Box::new(future::ok::<(), Error>(())))
            );
            s.expect(bd.write_at_call(check!(|buf: &IoVec| {
                buf.len() == 3 * BYTES_PER_LBA
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

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        vdev_raid.open_zone(1);
}

// Open a zone that has some leading wasted space.  Use mock VdevBlock objects
// to verify that the leading wasted space gets zero-filled.
// Use highly unrealistic disks with 32 LBAs per zone
#[test]
fn open_zone_zero_fill_wasted_stripes() {
        let n = 7;
        let k = 5;
        let f = 1;
        const CHUNKSIZE : LbaT = 1;
        let zl0 = (0, 32);
        let zl1 = (32, 64);

        let s = Scenario::new();
        let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();

        let bd = |gap_chunks: LbaT| {
            let bd = Box::new(s.create_mock::<MockVdevBlock>());
            s.expect(bd.size_call().and_return_clone(262144).times(..));
            s.expect(bd.lba2zone_call(0).and_return_clone(Some(0)).times(..));
            s.expect(bd.zone_limits_call(0).and_return_clone(zl0).times(..));
            s.expect(bd.zone_limits_call(1).and_return_clone(zl1).times(..));
            s.expect(bd.open_zone_call(32)
                     .and_return(Box::new(future::ok::<(), Error>(())))
            );
            if gap_chunks > 0 {
                s.expect(bd.write_at_call(check!(move |buf: &IoVec| {
                    let gap_lbas = gap_chunks * CHUNKSIZE; 
                    buf.len() == gap_lbas as usize * BYTES_PER_LBA
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

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        vdev_raid.open_zone(1);
}
}
