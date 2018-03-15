// vim: tw=80

use futures::Future;
use futures::sync::oneshot;
use nix;
use std::cell::RefCell;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::collections::btree_map::BTreeMap;
use std::vec::Vec;
use tokio::executor::current_thread;
use tokio::reactor::Handle;

use common::*;
use common::dva::*;
use common::vdev::*;
use common::vdev_leaf::*;

struct BlockOpBufG<T> {
    pub buf: T,
    /// Used by the `VdevLeaf` to complete this future
    pub sender: oneshot::Sender<()>
}

enum BlockOpBufT {
    IoVec(BlockOpBufG<IoVec>),
    IoVecMut(BlockOpBufG<IoVecMut>),
    SGList(BlockOpBufG<SGList>),
    SGListMut(BlockOpBufG<SGListMut>)
}

/// A single read or write command that is queued at the `VdevBlock` layer
struct BlockOp {
    pub lba: LbaT,
    pub bufs: BlockOpBufT,
}

impl BlockOp {
    pub fn len(&self) -> usize {
        match self.bufs {
            BlockOpBufT::IoVec(ref iovec) => iovec.buf.len(),
            BlockOpBufT::IoVecMut(ref iovec) => iovec.buf.len(),
            BlockOpBufT::SGList(ref sglist) => {
                sglist.buf.iter().fold(0, |acc, iovec| acc + iovec.len())
            }
            BlockOpBufT::SGListMut(ref sglist) => {
                sglist.buf.iter().fold(0, |acc, iovec| acc + iovec.len())
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
    pub fn read_at(buf: IoVecMut, lba: LbaT, sender: oneshot::Sender<()>) -> BlockOp {
        let g = BlockOpBufG::<IoVecMut>{ buf, sender };
        BlockOp { lba, bufs: BlockOpBufT::IoVecMut(g)}
    }

    pub fn readv_at(bufs: SGListMut, lba: LbaT, sender: oneshot::Sender<()>) -> BlockOp {
        let g = BlockOpBufG::<SGListMut>{buf: bufs, sender};
        BlockOp { lba, bufs: BlockOpBufT::SGListMut(g)}
    }

    pub fn write_at(buf: IoVec, lba: LbaT, sender: oneshot::Sender<()>) -> BlockOp {
        let g = BlockOpBufG::<IoVec>{ buf, sender };
        BlockOp { lba: lba, bufs: BlockOpBufT::IoVec(g)}
    }

    pub fn writev_at(bufs: SGList, lba: LbaT, sender: oneshot::Sender<()>) -> BlockOp {
        let g = BlockOpBufG::<SGList>{buf: bufs, sender};
        BlockOp { lba, bufs: BlockOpBufT::SGList(g)}
    }
}

/// Future representing an any operation on a block vdev.
///
/// Since the scheduler combines adjacent operations, it's not always possible
/// to know how much of an original operation's data was successfully transacted
/// (as opposed to the combined operation's), so the return value is merely `()`
/// on success, or an error code on failure.
#[must_use = "futures do nothing unless polled"]
pub type VdevBlockFut = Future<Item = (), Error = nix::Error>;

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

/// `VdevBlock`: Virtual Device for basic block device
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
    fn check_iovec_bounds(&self, lba: LbaT, buf: &[u8]) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (dva::BYTES_PER_LBA as u64);
        assert!(last_lba < self.size as u64)
    }

    /// Helper function for read and write methods
    fn check_sglist_bounds(&self, lba: LbaT, bufs: &[IoVec]) {
        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (dva::BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Helper function for readv and writev methods
    ///
    /// TODO: combine this method with `check_sglist_bounds`
    fn check_sglistmut_bounds(&self, lba: LbaT, bufs: &[IoVecMut]) {
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
        VdevBlock { handle,
                    leaf,
                    size,
                    write_queues: RefCell::new(BTreeMap::new()),
                    _read_queue: RefCell::new(BTreeMap::new())
                   }
    }

    /// If possible, issue any writes from the given zone.
    fn issue_writes(&mut self, zone: ZoneT) {
        let mut wq = self.write_queues.borrow_mut();
        let zq = wq.get_mut(&zone).expect("Tried to issue from a closed zone");
        assert!(! zq.q.is_empty(), "Tried to issue from an empty zone");
        // Optimistically allocate enough for every BlockOp in the zone queue.
        let l = zq.q.len();
        let mut iovec_s = Vec::<oneshot::Sender<()>>::with_capacity(l);
        let mut sg_s = Vec::<oneshot::Sender<()>>::with_capacity(l);
        let mut combined_bufs = SGList::with_capacity(l);
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
            let lbas = (block_op.len() / BYTES_PER_LBA) as LbaT;
            zq.wp += lbas;
            match block_op.bufs {
                BlockOpBufT::IoVec(g) => {
                    combined_bufs.push(g.buf);
                    iovec_s.push(g.sender);
                },
                BlockOpBufT::SGList(g) => {
                    combined_bufs.extend_from_slice(&g.buf);
                    sg_s.push(g.sender);
                },
                _ => unreachable!("buffer type that's not used for writing!")
            };
        }
        if combined_bufs.is_empty() {
            // Nothing to do
            return;
        } else if combined_bufs.len() == 1  && sg_s.is_empty(){
            assert_eq!(iovec_s.len(), 1);
            let sender = iovec_s.pop().unwrap();
            let fut = self.leaf.write_at(combined_bufs.pop().unwrap(),
                                         start_lba)
                .and_then(move |_| {
                    sender.send(()).unwrap();
                    Ok(())
                }).map_err(|_| {
                    ()
                });
            current_thread::spawn(fut);
        } else {
            let fut = self.leaf.writev_at(combined_bufs, start_lba)
                .and_then(move|_| {
                    for sender in iovec_s.drain(..) {
                        sender.send(()).unwrap();
                    }
                    for sender in sg_s.drain(..) {
                        sender.send(()).unwrap();
                    }
                    Ok(())
                }).map_err(|_| {
                    ()
                });
            current_thread::spawn(fut);
        }
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::read_at(buf, lba, sender);
        self.sched_read(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// The asynchronous scatter/gather read function.
    ///
    /// Returns nothing on success, and on error on failure
    ///
    /// # Parameters
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_sglistmut_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::readv_at(bufs, lba, sender);
        self.sched_read(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    fn sched_read(&self, block_op: BlockOp) {
        // TODO eventually these should be scheduled by LBA order to reduce
        // the disks' queue depth, but for now push them straight through.  In
        // the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        match block_op.bufs {
            BlockOpBufT::IoVecMut(iovec_mut) => {
                let sender = iovec_mut.sender;
                current_thread::spawn(
                    self.leaf.read_at(iovec_mut.buf, block_op.lba)
                    .and_then(move |_| {
                        sender.send(()).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    }))
            },
            BlockOpBufT::SGListMut(sglist_mut) => {
                let sender = sglist_mut.sender;
                current_thread::spawn(
                    self.leaf.readv_at(sglist_mut.buf, block_op.lba)
                    .and_then(move |_| {
                        sender.send(()).unwrap();
                        Ok(())})
                    .map_err(|_| {
                        ()
                    }))
            },
            _ => unreachable!("buffer type that's not used for reading!")
        };
    }

    fn sched_write(&mut self, block_op: BlockOp) {
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

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns nothing on success, and on error on failure
    pub fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::write_at(buf, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not yet support fragmentary writes");
        self.sched_write(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// Returns nothing on success, or an error on failure
    ///
    /// # Parameters
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    pub fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::writev_at(bufs, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not yet support fragmentary writes");
        self.sched_write(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }
}

impl Vdev for VdevBlock {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

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

#[cfg(feature = "mocks")]
#[cfg(test)]
test_suite! {
    name mock_vdev_block;

    use super::*;
    use divbuf::DivBufShared;
    use futures::future;
    use mockers::{Scenario, Sequence};
    use mockers::matchers::ANY;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    mock!{
        MockVdevLeaf2,
        vdev,
        trait Vdev {
            fn handle(&self) -> Handle;
            fn lba2zone(&self, lba: LbaT) -> ZoneT;
            fn size(&self) -> LbaT;
            fn start_of_zone(&self, zone: ZoneT) -> LbaT;
        },
        vdev_leaf,
        trait VdevLeaf  {
            fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut>;
            fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<SGListFut>;
            fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut>;
            fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<SGListFut>;
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
        let r0 = IoVecResult { value: 4096 };
        let r1 = IoVecResult { value: 4096 };
        seq.expect(leaf.read_at_call(ANY, 1)
                       .and_return(Box::new(future::ok::<IoVecResult,
                                                         nix::Error>(r0))));
        seq.expect(leaf.read_at_call(ANY, 0)
                       .and_return(Box::new(future::ok::<IoVecResult,
                                                         nix::Error>(r1))));
        scenario.expect(seq);

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            let first = vdev.read_at(rbuf0, 1);
            let second = vdev.read_at(rbuf1, 0);
            future::Future::join(first, second)
        })).unwrap();
    }

    // Basic writing at the WP works
    test write_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let r = IoVecResult { value: 4096 };
        scenario.expect(leaf.write_at_call(ANY, 0)
                            .and_return(Box::new(future::ok::<IoVecResult,
                                                              nix::Error>(r))));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let mut vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.write_at(wbuf, 0)
        })).unwrap();
    }

    // Basic vectored writing of just 1 lba at the WP works
    test writev_1_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let r = SGListResult { value: 4096 };
        scenario.expect(leaf.writev_at_call(ANY, 0)
                            .and_return(Box::new(future::ok::<SGListResult,
                                                              nix::Error>(r))));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let wbufs = vec![wbuf];
        let mut vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.writev_at(wbufs, 0)
        })).unwrap();
    }

    // Basic vectored writing at the WP works
    test writev_2_at_0(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let r = SGListResult { value: 4096 };
        scenario.expect(leaf.writev_at_call(ANY, 0)
                            .and_return(Box::new(future::ok::<SGListResult,
                                                              nix::Error>(r))));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(1024);
        let wbuf1 = wbuf.slice_from(1024);
        let wbufs = vec![wbuf0, wbuf1];
        let mut vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.writev_at(wbufs, 0)
        })).unwrap();
    }

    // Writes should be reordered and combined if out-of-order
    test write_at_combining(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let r = SGListResult { value: 8192 };
        scenario.expect(leaf.writev_at_call(ANY, 0)
                            .and_return(Box::new(future::ok::<SGListResult,
                                                              nix::Error>(r))));

        let mut vdev = VdevBlock::open(leaf, Handle::current());
        let dbs = DivBufShared::from(vec![0u8; 8192]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(4096);
        let wbuf1 = wbuf.slice_from(4096);
        current_thread::block_on_all(future::lazy(|| {
            // Issue writes out-of-order
            let first = vdev.write_at(wbuf0, 1);
            let second = vdev.write_at(wbuf1, 0);
            future::Future::join(first, second)
        })).unwrap();
    }

    // Writes should be issued ASAP, even if they could be combined later
    test write_at_issue_asap(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        scenario.expect(leaf.lba2zone_call(2).and_return(0));
        let r0 = SGListResult { value: 8192 };
        let r1 = IoVecResult { value: 4096 };
        seq.expect(leaf.writev_at_call(ANY, 0)
                       .and_return(Box::new(future::ok::<SGListResult,
                                                         nix::Error>(r0))));
        seq.expect(leaf.write_at_call(ANY, 2)
                       .and_return(Box::new(future::ok::<IoVecResult,
                                                         nix::Error>(r1))));
        scenario.expect(seq);

        let dbs = DivBufShared::from(vec![0u8; 12288]);
        let wbuf = dbs.try().unwrap();
        let wbuf0 = wbuf.slice_to(4096);
        let wbuf1 = wbuf.slice(4096, 8192);
        let wbuf2 = wbuf.slice_from(8192);
        let mut vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            let first = vdev.write_at(wbuf0, 1);
            let second = vdev.write_at(wbuf1, 0);
            let third = vdev.write_at(wbuf2, 2);
            future::Future::join3(first, second, third)
        })).unwrap();
    }
}
