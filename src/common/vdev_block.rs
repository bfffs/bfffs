// vim: tw=80

use futures;
use mio;
use std::collections::BinaryHeap;
use std::collections::btree_map::BTreeMap;
use std::io;
use std::rc::{Rc, Weak};
use tokio_core::reactor::{Handle, PollEvented};

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;
use common::zoned_device::*;

/// mio implementation detail
#[doc(hidden)]
struct VdevBlockFutEvented {
    /// Used by `mio::Evented`
    registration: mio::Registration,

    /// Used by the `VdevLeaf` to complete this future
    promise: mio::SetReadiness
}

impl mio::Evented for VdevBlockFutEvented {
    fn register(&self,
                poll: &mio::Poll,
                token: mio::Token,
                interest: mio::Ready,
                opts: mio::PollOpt) -> io::Result<()> {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(&self,
                poll: &mio::Poll,
                token: mio::Token,
                interest: mio::Ready,
                opts: mio::PollOpt) -> io::Result<()> {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        self.registration.deregister(poll)
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct VdevBlockFut<T: Vdev + ?Sized> {
    /// Link to the target `Vdev` that will complete this future
    vdev: Rc<T>,

    /// The associated `BlockOp`.  Whether it's a read or write will be clear
    /// from which `ZoneQueue` it's stored in.
    block_op: BlockOp,

    /// Has this I/O already been scheduled?
    scheduled: bool,

    /// Is this a read or a write operation?
    write: bool,

    // Used by the mio stuff
    io: PollEvented<VdevBlockFutEvented>
}

impl<T: Vdev + ?Sized> VdevBlockFut<T> {
    pub fn new(vdev: Rc<T>,
               block_op: BlockOp,
               write:bool) -> VdevBlockFut<T> {
        let (registration, promise) = mio::Registration::new2();
        let io = VdevBlockFutEvented {registration: registration,
                                 promise: promise};
        let handle = vdev.handle().clone();
        VdevBlockFut::<T> {vdev: vdev,
                           block_op: block_op,
                           scheduled: false,
                           write: write,
                           io: PollEvented::new(io, &handle).unwrap()}
    }
}

impl<T: Vdev + ?Sized> futures::Future for VdevBlockFut<T> {
    type Item = isize;
    type Error = io::Error;

    fn poll(&mut self) -> futures::Poll<isize, io::Error> {
        if ! self.scheduled {
            //match self.block_op.bufs {
                //BlockOpBufT::IoVec(ref iovec) => {
                    //if self.write {
                    //} else {
                        //self.vdev.read_at(iovec.clone(), self.block_op.lba);
                    //}
                //},
                //BlockOpBufT::SGList(ref sglist) => {
                    //if self.write {
                    //} else {
                        ////self.vdev.readv_at(sglist ,self.block_op.lba);
                    //}
                //}
            //}
            self.scheduled = true;
        }
        /// Arbitrary use Readable readiness for this type.  It doesn't matter
        /// what kind of readiness we use, so long as we're consistent.
        let poll_result = self.io.poll_read();
        if poll_result == futures::Async::NotReady {
            return Ok(futures::Async::NotReady);
        }
        Ok(futures::Async::Ready(42))  //TODO: get the real result somehow.
    }
}

/// Used for scheduling writes within a single Zone
struct ZoneQueue {
    /// The zone's write pointer, as an LBA.  Absolute, not relative to
    /// start-of-zone.
    wp: LbaT,

    /// Priority queue of pending `BlockOp`s for a single zone.  It stores
    /// operations that aren't ready to be issued to the underlying storage,
    /// then issues them in LBA order.  There may be gaps between adjacent ops.
    /// However, sine it is illegal for the client to write to the same location
    /// twice without explicitly erasing the zone, there are guaranteed to be no
    /// overlapping ops.
    q: BinaryHeap<BlockOp>
}

/// ZoneScheduler: I/O scheduler for a single zoned block device
///
/// This object schedules I/O to a block device.  Its main purpose is to provide
/// the strictly sequential write operations that zoned devices require.
struct ZoneScheduler {
}

impl ZoneScheduler {
    //pub fn new(leaf: Box<VdevLeaf>, handle: Handle) -> ZoneScheduler {
        //ZoneScheduler{ leaf: leaf,
                       //handle: handle,
                       //write_queues: BTreeMap::new(),
                       //read_queue: BTreeMap::new() }
    //}

    // Schedule the BlockOp, then issue any reads that are ready.
    //pub fn sched_read(&self, block_op: BlockOp) {
        ////TODO eventually these should be scheduled by LBA order and to reduce
        ////the disks' queue depth, but for now push them straight through
        //match self.block_op.bufs {
            //BlockOpBufT::IoVec(iovec) => self.leaf.read_at(iovec, self.block_op.lba),
            //BlockOpBufT::SGList(sglist) => self.leaf.readv_at(sglist, self.block_op.lba)
        //}.and_then(|| {//TODO
        //})
    //}

    //pub fn readv_at(&self, bufs: SGList, lba: LbaT) -> (
        //AioFut<isize>, ZoneSchedIter) {
        ////TODO
        //ZoneSchedIter{}
    //}

    //pub fn write_at(&self, buf: IoVec, lba: LbaT) -> (
        //AioFut<isize>, ZoneSchedIter) {
        ////TODO
        //ZoneSchedIter{}
    //}

    //pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> (
        //AioFut<isize>, ZoneSchedIter) {
        ////TODO
        //ZoneSchedIter{}
    //}
}

///// An iterator that yields successive `BlockOp`s of a `ZoneScheduler` that are
///// ready to issue
//pub struct ZoneSchedIter {
//}

//impl Iterator for ZoneSchedIter {
    //type Item = BlockOp;

    //fn next(&mut self) -> Option<BlockOp> {
        ////TODO
        //None
    //}
//}

/// VdevBlock: Virtual Device for basic block device
///
/// This struct contains the functionality that is common between all types of
/// leaf vdev.
pub struct VdevBlock {
    /// Handle to a Tokio reactor
    handle: Handle,

    /// Underlying device
    pub leaf: Box<VdevLeaf>,

    /// Usable size of the vdev, in LBAs
    size:   LbaT,

    // Needed so we can hand out Rc<Vdev> to `VdevBlockFut`s
    selfref: Weak<VdevBlock>,

    /// A collection of BlockOps.  Newly received reads must land here.  They
    /// will be issued to the OS as the scheduler sees fit.
    read_queue: BTreeMap<LbaT, BlockOp>,

    /// A collection of ZoneQueues, one for each open Zone.  Newly received
    /// writes must land here.  They will be issued to the OS in LBA-order, per
    /// zone.  If a Zone is not present in the map, then it must be either full
    /// or empty.
    write_queues: BTreeMap<ZoneT, ZoneQueue>,
}

impl VdevBlock {
    /// Helper function for read and write methods
    fn check_iovec_bounds(&self, lba: LbaT, buf: &IoVec) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (dva::BYTES_PER_LBA as u64);
        assert!(last_lba < self.size as u64)
    }

    /// Helper function for read and write methods
    fn check_sglist_bounds(&self, lba: LbaT, bufs: &SGList) {
        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (dva::BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Open a VdevBlock
    ///
    /// * `leaf`    An already-open underlying VdevLeaf 
    // The 'static enforces that if the VdevLeaf implementor contains any
    // references, they must be 'static.
    pub fn open<T: VdevLeaf + 'static>(leaf: Box<T>, handle: Handle) -> Self {
        let size = leaf.size();
        VdevBlock { handle: handle,
                    leaf: leaf,
                    size: size,
                    // TODO: how to correctly create selfref?
                    selfref: Weak::new(),
                    write_queues: BTreeMap::new(),
                    read_queue: BTreeMap::new()
                   }
    }

    ///// Helper function that reads a `BlockOp` popped off the scheduler
    //fn read_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        //let bufs = block_op.bufs;
        //if bufs.len() == 1 {
            //self.leaf.read_at(bufs.first().unwrap().clone(), block_op.lba)
        //} else {
            //self.leaf.readv_at(bufs, block_op.lba)
        //}
    //}

    ///// Helper function that writes a `BlockOp` popped off the scheduler
    //fn write_blockop(&self, block_op: BlockOp) -> io::Result<AioFut<isize>>{
        //let bufs = block_op.bufs;
        //if bufs.len() == 1 {
            //self.leaf.write_at(bufs.first().unwrap().clone(), block_op.lba)
        //} else {
            //self.leaf.writev_at(bufs, block_op.lba)
        //}
    //}
}

impl SGVdev for VdevBlock {
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut> {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        let selfref = self.selfref.upgrade().unwrap().clone();
        Box::new(VdevBlockFut::new(selfref, block_op, false))
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut> {
        self.check_sglist_bounds(lba, &bufs);
        let block_op = BlockOp::writev_at(bufs, lba);
        let selfref = self.selfref.upgrade().unwrap().clone();
        Box::new(VdevBlockFut::new(selfref, block_op, true))
    }
}

impl Vdev for VdevBlock {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::read_at(buf, lba);
        let selfref = self.selfref.upgrade().unwrap().clone();
        Box::new(VdevBlockFut::new(selfref, block_op, false))
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        self.check_iovec_bounds(lba, &buf);
        let block_op = BlockOp::write_at(buf, lba);
        let selfref = self.selfref.upgrade().unwrap().clone();
        Box::new(VdevBlockFut::new(selfref, block_op, true))
    }
}

impl ZonedDevice for VdevBlock {
    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        self.leaf.lba2zone(lba)
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        self.leaf.start_of_zone(zone)
    }
}
