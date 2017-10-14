// vim: tw=80

use futures::{Future, Poll};
use futures::sync::oneshot;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::collections::btree_map::BTreeMap;
use std::io;
use std::mem;
use std::rc::{Rc, Weak};
use tokio_core::reactor::Handle;

use common::*;
use common::vdev::*;
use common::vdev_leaf::*;

#[derive(Eq, PartialEq)]
pub enum BlockOpBufT {
    IoVec(IoVec),
    //None,
    SGList(SGList)
}

/// A single read or write command that is queued at the VdevBlock layer
struct BlockOp {
    pub lba: LbaT,
    pub bufs: BlockOpBufT,

    /// Used by the `VdevLeaf` to complete this future
    pub sender: oneshot::Sender<isize>
}

impl Eq for BlockOp {
}

impl Ord for BlockOp {
    /// Compare `BlockOp`s by LBA in *reverse* order.  We must use reverse order
    /// because Rust's standard library includes a max heap but not a min heap,
    /// and we want to pop `BlockOp`s lowest-LBA first.
    fn cmp(&self, other: &BlockOp) -> Ordering {
        self.lba.cmp(&other.lba).reverse()
    }
}

impl PartialEq for BlockOp {
    fn eq(&self, other: &BlockOp) -> bool {
        self.lba == other.lba
    }
}

impl PartialOrd for BlockOp {
    fn partial_cmp(&self, other: &BlockOp) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl BlockOp {
    pub fn read_at(buf: IoVec, lba: LbaT, sender: oneshot::Sender<isize>) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::IoVec(buf), sender: sender}
    }

    pub fn readv_at(bufs: SGList, lba: LbaT, sender: oneshot::Sender<isize>) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::SGList(bufs), sender: sender}
    }

    pub fn write_at(buf: IoVec, lba: LbaT, sender: oneshot::Sender<isize>) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::IoVec(buf), sender: sender}
    }

    pub fn writev_at(bufs: SGList, lba: LbaT, sender: oneshot::Sender<isize>) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::SGList(bufs), sender: sender}
    }
}

#[must_use = "futures do nothing unless polled"]
struct VdevBlockFut {
    // Used by the mio stuff
    receiver: oneshot::Receiver<isize>
}

impl VdevBlockFut {
    pub fn new( receiver: oneshot::Receiver<isize>) -> VdevBlockFut {
        VdevBlockFut{receiver: receiver}
    }
}

impl Future for VdevBlockFut {
    type Item = isize;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<isize, io::Error> {
        self.receiver.poll()
        .map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "")
        })
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
                    write_queues: BTreeMap::new(),
                    read_queue: BTreeMap::new()
                   }
    }

    fn sched_read(&self, block_op: BlockOp) {
        //TODO eventually these should be scheduled by LBA order and to reduce
        //the disks' queue depth, but for now push them straight through
        // In the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        let sender = block_op.sender;
        match block_op.bufs {
            BlockOpBufT::IoVec(iovec) =>
                self.handle.spawn(
                    self.leaf.read_at(iovec, block_op.lba)
                    .and_then(move |r| {
                        sender.send(r).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    })),
            BlockOpBufT::SGList(sglist) =>
                self.handle.spawn(
                    self.leaf.readv_at(sglist, block_op.lba)
                    .and_then(move |r| {
                        sender.send(r).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    }))
        };
    }

    fn sched_write(&self, block_op: BlockOp) {
        //TODO actually schedule them instead of issueing immediately
        // In the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        let sender = block_op.sender;
        match block_op.bufs {
            BlockOpBufT::IoVec(iovec) =>
                self.handle.spawn(
                    self.leaf.write_at(iovec, block_op.lba)
                    .and_then(move |r| {
                        sender.send(r).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    })),
            BlockOpBufT::SGList(sglist) =>
                self.handle.spawn(
                    self.leaf.writev_at(sglist, block_op.lba)
                    .and_then(move |r| {
                        sender.send(r).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    }))
        };
    }

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
        let (sender, receiver) = oneshot::channel::<isize>();
        let block_op = BlockOp::readv_at(bufs, lba, sender);
        self.vdev.sched_read(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut> {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<isize>();
        let block_op = BlockOp::writev_at(bufs, lba, sender);
        self.vdev.sched_write(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }
}

impl Vdev for VdevBlock {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> ZoneT {
        self.leaf.lba2zone(lba)
    }

    fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<isize>();
        let block_op = BlockOp::read_at(buf, lba, sender);
        self.vdev.sched_read(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn start_of_zone(&self, zone: ZoneT) -> LbaT {
        self.leaf.start_of_zone(zone)
    }

    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<isize>();
        let block_op = BlockOp::write_at(buf, lba, sender);
        self.vdev.sched_write(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }
}
