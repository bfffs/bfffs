// vim: tw=80

use common::*;
use common::dva::*;
use common::vdev::*;
#[cfg(not(test))]
use common::vdev_block::*;
use common::raid::*;
use common::declust::*;
use divbuf::DivBufShared;
use futures::{Future, future};
use itertools::multizip;
use modulo::Mod;
use std::{cmp, mem, ptr};
use tokio::reactor::Handle;

#[cfg(test)]
/// Only exists so mockers can replace VdevBlock
pub trait VdevBlockTrait : SGVdev {
}
#[cfg(test)]
pub type VdevBlockLike = Box<VdevBlockTrait>;
#[cfg(not(test))]
pub type VdevBlockLike = VdevBlock;

/// In-memory cache of data that has not yet been flushed the Block devices.
///
/// Typically there will be one of these for each open zone.
struct StripeBuffer {
    /// Cache of `IoVec`s that haven't yet been flushed
    buf: SGList,

    /// The LBA of the beginning of the cached stripe
    lba: LbaT,

    /// Size of a full stripe, in LBAs
    stripe_lbas: LbaT
}

impl StripeBuffer {
    /// Read more data into this `StripeBuffer`, but don't overflow one row.
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
        StripeBuffer {buf: SGList::new(), lba, stripe_lbas}
    }

    /// Return the value of the next LBA should be written into this buffer
    pub fn next_lba(&self) -> LbaT {
        self.lba + self.len()
    }

    pub fn pop(&mut self) -> SGList {
        let new = SGList::new();
        self.lba = self.next_lba();
        mem::replace(&mut self.buf, new)
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
    locator: Box<Locator>,

    /// Underlying block devices.  Order is important!
    blockdevs: Box<[VdevBlockLike]>,

    /// In memory cache of data that has not yet been flushed to the block
    /// devices.
    ///
    /// We cache up to one stripe per zone before flushing it.  Cacheing entire
    /// stripes uses fewer resources than only cacheing the parity information.
    /// TODO: make this a collection, indexed by zone_id
    _stripe_buffer: StripeBuffer
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
            let futs : Vec<_> = $buf
            .into_iter()
            .enumerate()
            .map(|(i, d)| {
                let chunk_id = if $parity {
                    ChunkId::Parity($lba / $self.chunksize, i as i16)
                } else {
                    ChunkId::Data($lba / $self.chunksize + i as LbaT)
                };
                let loc = $self.locator.id2loc(chunk_id);
                let disk_lba = loc.offset * $self.chunksize;
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
            assert_eq!(blockdevs[0].start_of_zone(1),
                       blockdevs[i].start_of_zone(1));
        }

        let stripe_lbas = codec.stripesize() as LbaT * chunksize as LbaT;
        VdevRaid { chunksize, codec, locator, blockdevs,
                   _stripe_buffer: StripeBuffer::new(0, stripe_lbas) }
    }

    /// Read two or more whole stripes
    fn read_at_multi(&self, mut buf: IoVecMut, lba: LbaT) -> Box<IoVecFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let k = self.codec.stripesize() as usize;
        let m = k - f as usize;
        let n = self.blockdevs.len();
        let chunks = buf.len() / col_len;
        let stripes = chunks / m;

        // Create an SGList for each disk.
        let mut sglists = Vec::<SGListMut>::with_capacity(n);
        const SENTINEL : LbaT = LbaT::max_value();
        let mut start_lbas : Vec<LbaT> = vec![SENTINEL; n];
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            // TODO: calculate a smaller maximum by considering the declustering
            // ratio, especially when n >> m
            sglists.push(SGListMut::with_capacity(stripes));
        }
        // Build the SGLists, one chunk at a time
        let mut futs : Vec<Box<SGListFut>> = Vec::with_capacity(n * stripes);
        for s in 0..stripes {
            for i in 0..k {
                if i < m {
                    let chunk_offs = lba / self.chunksize + (s * m + i) as LbaT;
                    let chunk_id = ChunkId::Data(chunk_offs);
                    let loc = self.locator.id2loc(chunk_id);
                    let disk_lba = loc.offset * self.chunksize;
                    if start_lbas[loc.disk as usize] == SENTINEL {
                        start_lbas[loc.disk as usize] = disk_lba;
                    } else {
                        debug_assert!(start_lbas[loc.disk as usize] < disk_lba);
                    }
                    let col = buf.split_to(col_len);
                    sglists[loc.disk as usize].push(col);
                } else {
                    let chunk_offs = lba / self.chunksize + (s * m) as LbaT;
                    let chunk_id = ChunkId::Parity(chunk_offs, (i - m) as i16);
                    let loc = self.locator.id2loc(chunk_id);
                    let disk = loc.disk as usize;
                    if start_lbas[disk] == SENTINEL {
                        // We haven't yet planned any reads from this disk.  We
                        // can simply ignore the parity chunk
                    } else {
                        // We've already planned some reads to this disk.  We
                        // must issue them now.
                        let new = SGListMut::with_capacity(stripes - s);
                        let old = mem::replace(&mut sglists[disk], new);
                        let lba = start_lbas[disk];
                        futs.push(self.blockdevs[disk].readv_at(old, lba));
                        start_lbas[disk] = SENTINEL;
                    }
                }
            }
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
        Box::new(fut.map(|v| {
            let value = v.into_iter().map(|x| x.value).sum();
            IoVecResult{value}
        }))
    }

    /// Read exactly one stripe
    fn read_at_one(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;

        let data: Vec<IoVecMut> = buf.into_chunks(col_len).collect();
        debug_assert_eq!(data.len(), m);

        let fut = issue_1stripe_ops!(self, data, lba, false, read_at);
        // TODO: on error, some futures get cancelled.  Figure out how to clean
        // them up.
        // TODO: on error, record error statistics, possibly fault a drive,
        // request the faulty drive's zone to be rebuilt, and read parity to
        // reconstruct the data.
        Box::new(fut.map(|v| {
            let value = v.into_iter().map(|x| x.value).sum();
            IoVecResult{value}
        }))
    }

    /// Write two or more whole stripes
    fn write_at_multi(&mut self, mut buf: IoVec, lba: LbaT) -> Box<IoVecFut> {
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
        for _ in 0..n {
            // Size each SGList to the maximum possible size
            // TODO: calculate a smaller maximum by considering the declustering
            // ratio, especially when n >> m
            sglists.push(SGList::with_capacity(stripes));
        }
        // Build the SGLists, one chunk at a time
        for s in 0..stripes {
            for i in 0..k {
                let (chunk_id, col) = if i < m {
                    let chunk_offs = lba / self.chunksize + (s * m + i) as LbaT;
                    (ChunkId::Data(chunk_offs), buf.split_to(col_len))
                } else {
                    let chunk_offs = lba / self.chunksize + (s * m) as LbaT;
                    let col = parity[i - m].split_to(col_len).freeze();
                    (ChunkId::Parity(chunk_offs, (i - m) as i16), col)
                };
                let loc = self.locator.id2loc(chunk_id);
                let disk_lba = loc.offset * self.chunksize;
                if start_lbas[loc.disk as usize] == SENTINEL {
                    start_lbas[loc.disk as usize] = disk_lba;
                } else {
                    debug_assert!(start_lbas[loc.disk as usize] < disk_lba);
                }
                sglists[loc.disk as usize].push(col);
            }
        }

        let futs : Vec<Box<SGListFut>> = multizip((self.blockdevs.iter_mut(),
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
        Box::new(fut.map(move |v| {
            let _ = parity_dbses;   // Needs to live this long
            let value = v.into_iter().map(|x| x.value).sum();
            IoVecResult{value}
        }))
    }

    /// Write exactly one stripe
    fn write_at_one(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut> {
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
        Box::new(data_fut.join(parity_fut).map(move |(data_r, parity_r)| {
            let _ = parity_dbses;   // Needs to live this long
            let data_v : isize = data_r.into_iter().map(|x| x.value).sum();
            let parity_v : isize = parity_r.into_iter().map(|x| x.value).sum();
            IoVecResult { value: data_v + parity_v }
        }))
    }

    /// Write exactly one stripe, with SGLists.
    ///
    /// This is mostly useful internally, for writing from the row buffer.  It
    /// should not be used publicly.
    #[doc(hidden)]
    pub fn writev_at_one(&mut self, buf: &SGList, lba: LbaT) -> Box<SGListFut> {
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
        Box::new(data_fut.join(parity_fut).map(move |(data_r, parity_r)| {
            let _ = parity_dbses;   // Needs to live this long
            let data_v : isize = data_r.into_iter().map(|x| x.value).sum();
            let parity_v : isize = parity_r.into_iter().map(|x| x.value).sum();
            SGListResult { value: data_v + parity_v }
        }))
    }
}

impl Vdev for VdevRaid {
    fn handle(&self) -> Handle {
        panic!("Unimplemented!  Perhaps handle() should not be part of Trait vdev, because it doesn't make sense for VdevRaid");
    }

    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        let loc = self.locator.id2loc(ChunkId::Data(lba / self.chunksize));
        let disk_lba = loc.offset * self.chunksize;
        self.blockdevs[loc.disk as usize].lba2zone(disk_lba)
    }

    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;
        assert_eq!(buf.len().modulo(col_len * m), 0,
                   "Only stripe-aligned reads are currently supported");
        assert_eq!(lba.modulo(self.chunksize as u64 * m as u64), 0,
            "Unaligned reads are not yet supported");
        let chunks = buf.len() / col_len;
        let stripes = chunks / m;

        if stripes == 1 {
            self.read_at_one(buf, lba)
        } else {
            self.read_at_multi(buf, lba)
        }
    }

    fn size(&self) -> LbaT {
        let disk_size_in_chunks = self.blockdevs[0].size() / self.chunksize;
        disk_size_in_chunks * self.locator.datachunks() *
            self.chunksize / (self.locator.depth() as LbaT)
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        // Zones don't necessarily line up with repetition boundaries.  So we
        // don't know the disk were a given zone begins.  Instead, we'll have to
        // search through every disk to find the one where the zone begins,
        // which will be disk that has the lowest LBA for that disk LBA.

        // All blockdevs must have the same zone map, so we only need to do the
        // start_of_zone call once.
        let disk_lba = self.blockdevs[0].start_of_zone(zone);
        (0..self.blockdevs.len()).map(|i| {
            let disk_chunk = disk_lba / self.chunksize;
            let cid = self.locator.loc2id(Chunkloc::new(i as i16, disk_chunk));
            match cid {
                ChunkId::Data(id) => id * self.chunksize,
                ChunkId::Parity(_, _) => LbaT::max_value()
            }
        }).min().unwrap()
    }

    fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut> {
        let col_len = self.chunksize as usize * BYTES_PER_LBA;
        let f = self.codec.protection() as usize;
        let m = self.codec.stripesize() as usize - f as usize;
        assert_eq!(buf.len().modulo(col_len * m), 0,
                   "Only stripe-aligned writes are currently supported");
        assert_eq!(lba.modulo(self.chunksize as u64 * m as u64), 0,
            "Unaligned writes are not yet supported");
        let chunks = buf.len() / col_len;
        let stripes = chunks / m;

        if stripes == 1 {
            self.write_at_one(buf, lba)
        } else {
            self.write_at_multi(buf, lba)
        }
    }
}

#[cfg(feature = "mocks")]
#[cfg(test)]
mod t {

use super::*;
use super::super::prime_s::PrimeS;
use futures::future;
use mockers::Scenario;
use std::io::Error;

mock!{
    MockVdevBlock,
    vdev,
    trait Vdev {
        fn handle(&self) -> Handle;
        fn lba2zone(&self, lba: LbaT) -> ZoneT;
        fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut>;
        fn size(&self) -> LbaT;
        fn start_of_zone(&self, zone: ZoneT) -> LbaT;
        fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut>;
    },
    vdev,
    trait SGVdev  {
        fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<SGListFut>;
        fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<SGListFut>;
    },
    self,
    trait VdevBlockTrait{
    }
}

#[test]
fn stripe_buffer_empty() {
    let mut sb = StripeBuffer::new(99, 6);
    assert!(!sb.is_full());
    assert!(sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 99);
    assert_eq!(sb.len(), 0);
    assert!(sb.pop().is_empty());
    // Adding an empty iovec should change nothing
    let dbs = DivBufShared::from(vec![0; 4096]);
    let db = dbs.try().unwrap();
    let db0 = db.slice(0, 0);
    assert!(sb.fill(db0).is_empty());
    assert!(!sb.is_full());
    assert!(sb.is_empty());
    assert_eq!(sb.lba(), 99);
    assert_eq!(sb.next_lba(), 99);
    assert_eq!(sb.len(), 0);
    assert!(sb.pop().is_empty());
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
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 1);
    assert_eq!(&sglist[0][..], &vec![0; 4096][..]);
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
    let sglist = sb.pop();
    assert_eq!(sglist.len(), 2);
    assert_eq!(&sglist[0][..], &vec![0; 16384][..]);
    assert_eq!(&sglist[1][..], &vec![1; 8192][..]);
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
    // A small layout that is a multiple of the zone size
    name small;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::{matchers, Scenario};

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let s = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..5 {
                let mock = Box::new(s.create_mock::<MockVdevBlock>());
                s.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                s.expect(mock.lba2zone_call(matchers::lt(65536))
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(1)
                                    .times(..));
                s.expect(mock.start_of_zone_call(0)
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.start_of_zone_call(1)
                                    .and_return_clone(65536)   // 64k LBAs/zone
                                    .times(..));

                blockdevs.push(mock);
            }

            let n = 5;
            let k = 4;
            let f = 1;

            let codec = Codec::new(k, f);
            let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
            let vdev_raid = VdevRaid::new(16, codec, locator,
                                          blockdevs.into_boxed_slice());
            (s, vdev_raid)
        }
    });

    test lba2zone(mocks) {
        assert_eq!(mocks.val.1.lba2zone(0), 0);
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(245759), 0);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(245760), 1);
    }

    test size(mocks) {
        assert_eq!(mocks.val.1.size(), 983040);
    }

    test start_of_zone(mocks) {
        assert_eq!(mocks.val.1.start_of_zone(0), 0);
        assert_eq!(mocks.val.1.start_of_zone(1), 245760);
    }
}

test_suite! {
    // A medium layout that is not a multiple of the zone size
    name medium;

    use super::super::*;
    use super::MockVdevBlock;
    use super::super::super::prime_s::PrimeS;
    use mockers::{matchers, Scenario};

    fixture!( mocks() -> (Scenario, VdevRaid) {
            setup(&mut self) {
            let s = Scenario::new();
            let mut blockdevs = Vec::<Box<VdevBlockTrait>>::new();
            for _ in 0..7 {
                let mock = Box::new(s.create_mock::<MockVdevBlock>());
                s.expect(mock.size_call()
                                    .and_return_clone(262144)
                                    .times(..));  // 256k LBAs
                s.expect(mock.lba2zone_call(matchers::lt(65536))
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.lba2zone_call(matchers::in_range(65536..131072))
                                    .and_return_clone(1)
                                    .times(..));
                s.expect(mock.start_of_zone_call(0)
                                    .and_return_clone(0)
                                    .times(..));
                s.expect(mock.start_of_zone_call(1)
                                    .and_return_clone(65536)   // 64k LBAs/zone
                                    .times(..));

                blockdevs.push(mock);
            }

            let n = 7;
            let k = 4;
            let f = 1;

            let codec = Codec::new(k, f);
            let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
            let vdev_raid = VdevRaid::new(16, codec, locator,
                                          blockdevs.into_boxed_slice());
            (s, vdev_raid)
        }
    });

    test lba2zone(mocks) {
        assert_eq!(mocks.val.1.lba2zone(0), 0);
        // Last LBA in zone 0
        assert_eq!(mocks.val.1.lba2zone(344063), 0);
        // First LBA in zone 1
        assert_eq!(mocks.val.1.lba2zone(344064), 1);
    }

    test size(mocks) {
        assert_eq!(mocks.val.1.size(), 1376256);
    }

    test start_of_zone(mocks) {
        assert_eq!(mocks.val.1.start_of_zone(0), 0);
        assert_eq!(mocks.val.1.start_of_zone(1), 344064);
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
        s.expect(m0.start_of_zone_call(1).and_return_clone(65536).times(..));
        let r = IoVecResult {
            value: CHUNKSIZE as isize * BYTES_PER_LBA as isize
        };
        s.expect(m0.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return(
                Box::new(
                    future::ok::<IoVecResult, Error>(r.clone())
                )
            )
        );

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m1.read_at_call(check!(|buf: &IoVecMut| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return(
                Box::new(
                    future::ok::<IoVecResult, Error>(r.clone())
                )
            )
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.start_of_zone_call(1).and_return_clone(65536).times(..));
        blockdevs.push(m2);

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
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
        s.expect(m0.start_of_zone_call(1).and_return_clone(65536).times(..));
        let r = IoVecResult {
            value: CHUNKSIZE as isize * BYTES_PER_LBA as isize
        };
        s.expect(m0.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return(
                Box::new(
                    future::ok::<IoVecResult, Error>(r.clone())
                )
            )
        );

        blockdevs.push(m0);
        let m1 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m1.size_call().and_return_clone(262144).times(..));
        s.expect(m1.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m1.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return(
                Box::new(
                    future::ok::<IoVecResult, Error>(r.clone())
                )
            )
        );

        blockdevs.push(m1);
        let m2 = Box::new(s.create_mock::<MockVdevBlock>());
        s.expect(m2.size_call().and_return_clone(262144).times(..));
        s.expect(m2.start_of_zone_call(1).and_return_clone(65536).times(..));
        s.expect(m2.write_at_call(check!(|buf: &IoVec| {
            buf.len() == CHUNKSIZE as usize * BYTES_PER_LBA
        }), 0)
            .and_return(
                Box::new(
                    future::ok::<IoVecResult, Error>(r.clone())
                )
            )
        );
        blockdevs.push(m2);

        let codec = Codec::new(k, f);
        let locator = Box::new(PrimeS::new(n, k as i16, f as i16));
        let mut vdev_raid = VdevRaid::new(CHUNKSIZE, codec, locator,
                                      blockdevs.into_boxed_slice());
        let dbs = DivBufShared::from(vec![0u8; 16384]);
        let wbuf = dbs.try().unwrap();
        vdev_raid.write_at(wbuf, 0);
}

}
