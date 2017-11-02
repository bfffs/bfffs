// vim: tw=80

use futures::{Future, Poll};
use futures::sync::oneshot;
use std::cell::RefCell;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::collections::btree_map::BTreeMap;
use std::vec::Vec;
use std::io;
use tokio_core::reactor::Handle;

use common::*;
use common::dva::*;
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

impl BlockOp {
    pub fn len(&self) -> usize {
        match self.bufs {
            BlockOpBufT::IoVec(ref iovec) => iovec.len(),
            BlockOpBufT::SGList(ref sglist) => {
                sglist.iter().fold(0, |acc, ref iovec| acc + iovec.len())
            }
        }
    }
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
    pub wp: LbaT,

    /// Priority queue of pending `BlockOp`s for a single zone.  It stores
    /// operations that aren't ready to be issued to the underlying storage,
    /// then issues them in LBA order.  There may be gaps between adjacent ops.
    /// However, sine it is illegal for the client to write to the same location
    /// twice without explicitly erasing the zone, there are guaranteed to be no
    /// overlapping ops.
    pub q: BinaryHeap<BlockOp>
}

impl ZoneQueue {
    fn new(start_of_zone: LbaT) -> Self {
        ZoneQueue {
            wp: start_of_zone,
            q: BinaryHeap::<BlockOp>::new()
        }
    }
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
    // Use a RefCell so that the VdevBlock can be manipulated by multiple
    // continuations which may have shared references, but all run in the same
    // reactor.
    _read_queue: RefCell<BTreeMap<LbaT, BlockOp>>,

    /// A collection of ZoneQueues, one for each open Zone.  Newly received
    /// writes must land here.  They will be issued to the OS in LBA-order, per
    /// zone.  If a Zone is not present in the map, then it must be either full
    /// or empty.
    // Use a RefCell so that the VdevBlock can be manipulated by multiple
    // continuations which may have shared references, but all run in the same
    // reactor.
    write_queues: RefCell<BTreeMap<ZoneT, ZoneQueue>>,
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
                    write_queues: RefCell::new(BTreeMap::new()),
                    _read_queue: RefCell::new(BTreeMap::new())
                   }
    }

    /// If possible, issue any writes from the given zone.
    fn issue_writes(&self, zone: ZoneT) {
        let mut wq = self.write_queues.borrow_mut();
        let zq = wq.get_mut(&zone).expect("Tried to issue from a closed zone");
        assert!(! zq.q.is_empty(), "Tried to issue from an empty zone");
        // Optimistically allocate enough for every BlockOp in the zone queue.
        let l = zq.q.len();
        let mut to_notify = Vec::<oneshot::Sender<isize>>::with_capacity(l);
        let mut combined_bufs = Vec::<Rc<Box<[u8]>>>::with_capacity(l);
        let start_lba = zq.wp;
        loop {
            if zq.q.is_empty() {
                // TODO: close the zone if it's full
                break;
            }
            if zq.q.peek().unwrap().lba != zq.wp {
                //Lowest queued write is higher than the block pointer; can't
                //issue yet.
                break;
            }
            let block_op = zq.q.pop().unwrap();
            let lbas = (block_op.len() / BYTES_PER_LBA as usize) as LbaT;
            zq.wp += lbas;
            match block_op.bufs {
                BlockOpBufT::IoVec(iovec) => combined_bufs.push(iovec),
                BlockOpBufT::SGList(sglist) => {
                    combined_bufs.extend_from_slice(&sglist)
                }
            };
            to_notify.push(block_op.sender);
        }
        if combined_bufs.is_empty() {
            // Nothing to do
            return;
        }
        let fut = match combined_bufs.len() {
            0 => unreachable!(),
            1 => self.leaf.write_at(combined_bufs.pop().unwrap(), start_lba),
            _ => self.leaf.writev_at(combined_bufs.into_boxed_slice(), start_lba)
        }.and_then(move |_| {
            for sender in to_notify.drain(..) {
                // XXX We don't actually know how much data this
                // sender's receiver was expecting.  Maybe we should just change
                // VdevFut to return () instead of isize
                sender.send(0).unwrap();
            }
            Ok(())
        }).map_err(|_| {
            ()
        });
        self.handle.spawn(fut);
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
        let zone = self.leaf.lba2zone(block_op.lba);
        {
            let wq = &mut self.write_queues.borrow_mut();
            let newzone : Option<ZoneQueue> = {
                let zq = wq.get_mut(&zone);
                if zq.is_some() {
                    zq.unwrap().q.push(block_op);
                    None
                } else {
                    let mut zq = ZoneQueue::new(self.leaf.start_of_zone(zone));
                    zq.q.push(block_op);
                    Some(zq)
                }
            };
            if newzone.is_some() {
                // Placate the borrow checker.  We can't do this if the previous
                // reference to wq is still alive.
                wq.insert(zone, newzone.unwrap());
            }
        }

        self.issue_writes(zone);
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
        self.sched_read(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }

    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut> {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<isize>();
        let block_op = BlockOp::writev_at(bufs, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA as usize, 0,
            "VdevBlock does not yet support fragmentary writes");
        self.sched_write(block_op);
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
        self.sched_read(block_op);
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
        assert_eq!(block_op.len() % BYTES_PER_LBA as usize, 0,
            "VdevBlock does not yet support fragmentary writes");
        self.sched_write(block_op);
        Box::new(VdevBlockFut::new(receiver))
    }
}

#[cfg(feature = "mocks")]
#[cfg(test)]
test_suite! {
    name mock_vdev_block;

    use super::*;
    use futures::future;
    use mockers::{Scenario, Sequence};
    use mockers::matchers::ANY;
    use std::io::Error;
    use std::rc::Rc;
    use tokio_core::reactor::{Core, Handle};

    mock!{
        MockVdevLeaf2,
        vdev,
        trait Vdev {
            fn handle(&self) -> Handle;
            fn lba2zone(&self, lba: LbaT) -> ZoneT;
            fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
            fn size(&self) -> LbaT;
            fn start_of_zone(&self, zone: ZoneT) -> LbaT;
            fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
        },
        vdev,
        trait SGVdev  {
            fn readv_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
            fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
        },
        vdev_leaf,
        trait VdevLeaf  {
        }
    }

    fixture!( mocks() -> (Scenario, Box<MockVdevLeaf2>) {
            setup(&mut self) {
            let scenario = Scenario::new();
            let leaf = Box::new(scenario.create_mock::<MockVdevLeaf2>());
            scenario.expect(leaf.size_call()
                                .and_return(16384));
            scenario.expect(leaf.lba2zone_call(ANY)
                                .and_return_clone(0)
                                .times(..));
            scenario.expect(leaf.start_of_zone_call(0)
                                .and_return_clone(0)
                                .times(..));
            (scenario, leaf)
        }
    });

    // Reads should be passed straight through, even if they're out-of-order
    test read_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        seq.expect(leaf.read_at_call(ANY, 1)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        seq.expect(leaf.read_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        scenario.expect(seq);

        let rbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.read_at(rbuf.clone(), 1);
        let second = vdev.read_at(rbuf.clone(), 0);
        let futs = future::Future::join(first, second);
        core.run(futs).unwrap();
    }

    // Basic writing at the WP works
    test write_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.write_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let fut = vdev.write_at(wbuf.clone(), 0);
        core.run(fut).unwrap();
    }

    // Basic vectored writing at the WP works
    test writev_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf0 = Rc::new(vec![0u8; 1024].into_boxed_slice());
        let wbuf1 = Rc::new(vec![0u8; 3072].into_boxed_slice());
        let wbufs = vec![wbuf0, wbuf1].into_boxed_slice();
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let fut = vdev.writev_at(wbufs, 0);
        core.run(fut).unwrap();
    }

    // Writes should be reordered and combined if out-of-order
    test write_at_combining(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        // Issue writes out-of-order
        let first = vdev.write_at(wbuf.clone(), 1);
        let second = vdev.write_at(wbuf.clone(), 0);
        let futs = future::Future::join(first, second);
        core.run(futs).unwrap();
    }

    // Writes should be issued ASAP, even if they could be combined later
    test write_at_issue_asap(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        scenario.expect(leaf.lba2zone_call(2).and_return(0));
        seq.expect(leaf.writev_at_call(ANY, 0)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        seq.expect(leaf.write_at_call(ANY, 2)
                        .and_return(Box::new(future::ok::<isize, Error>((0)))));
        scenario.expect(seq);

        let wbuf = Rc::new(vec![0u8; 4096].into_boxed_slice());
        let mut core = Core::new().unwrap();
        let vdev = Rc::new(VdevBlock::open(leaf, core.handle()));
        let first = vdev.write_at(wbuf.clone(), 1);
        let second = vdev.write_at(wbuf.clone(), 0);
        let third = vdev.write_at(wbuf.clone(), 2);
        let futs = future::Future::join3(first, second, third);
        core.run(futs).unwrap();
    }
}
