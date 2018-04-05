// vim: tw=80

use futures::{Async, Future};
use futures::sync::oneshot;
use nix;
use std::cell::RefCell;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::BinaryHeap;
use std::{mem, ops, thread, time};
use std::rc::{Rc, Weak};
use tokio::executor::current_thread;
use tokio::reactor::Handle;

use common::*;
use common::dva::*;
use common::vdev::*;
use common::vdev_leaf::*;

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum Cmd {
    OpenZone,
    ReadAt(IoVecMut),
    ReadvAt(SGListMut),
    WriteAt(IoVec),
    WritevAt(SGList),
    // The extra LBA is the zone's starting LBA
    EraseZone(LbaT),
    // The extra LBA is the zone's starting LBA
    FinishZone(LbaT),
}

/// A single read or write command that is queued at the `VdevBlock` layer
struct BlockOp {
    /// The effective LBA for sorting purposes.  Usually it's also the command's
    /// actual LBA
    pub lba: LbaT,
    pub cmd: Cmd,
    /// Used by the `VdevLeaf` to complete this future
    pub sender: oneshot::Sender<()>
}

impl Eq for BlockOp {
}

impl Ord for BlockOp {
    /// Compare `BlockOp`s by LBA.
    ///
    /// This comparison is only correct for `BlockOp`s that are both on the same
    /// side of the scheduler.  Comparing `BlockOp`s on different sides of the
    /// scheduler would require knowledge of `Inner::last_lba`, and the `Ord`
    /// trait doesn't allow that.
    fn cmp(&self, other: &BlockOp) -> Ordering {
        // Reverse the usual ordering, because we want to issue the minimum LBA
        // first, and Rust provides a max heap but not a min heap.
        other.lba.cmp(&self.lba).then_with(|| {
            other.cmd.cmp(&self.cmd)
        })
    }
}

impl PartialEq for BlockOp {
    fn eq(&self, other: &BlockOp) -> bool {
        self.lba == other.lba && self.cmd == other.cmd
    }
}

impl PartialOrd for BlockOp {
    fn partial_cmp(&self, other: &BlockOp) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl BlockOp {
    pub fn erase_zone(start: LbaT, end: LbaT,
                      sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba: end, cmd: Cmd::EraseZone(start), sender }
    }

    pub fn finish_zone(start: LbaT, end: LbaT,
                       sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba: end, cmd: Cmd::FinishZone(start), sender }
    }

    pub fn len(&self) -> usize {
        match self.cmd {
            Cmd::WriteAt(ref iovec) => iovec.len(),
            Cmd::ReadAt(ref iovec) => iovec.len(),
            Cmd::WritevAt(ref sglist) => {
                sglist.iter().fold(0, |acc, iovec| acc + iovec.len())
            },
            Cmd::ReadvAt(ref sglist) => {
                sglist.iter().fold(0, |acc, iovec| acc + iovec.len())
            },
            _ => 0
        }
    }

    pub fn open_zone(lba: LbaT, sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::OpenZone, sender }
    }

    pub fn read_at(buf: IoVecMut, lba: LbaT,
                   sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::ReadAt(buf), sender}
    }

    pub fn readv_at(bufs: SGListMut, lba: LbaT,
                    sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::ReadvAt(bufs), sender}
    }

    pub fn write_at(buf: IoVec, lba: LbaT,
                    sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WriteAt(buf), sender}
    }

    pub fn writev_at(bufs: SGList, lba: LbaT,
                     sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WritevAt(bufs), sender}
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

struct Inner {
    /// A VdevLeaf future that got delayed by an EAGAIN error.  We hold the
    /// future around instead of spawning it into the reactor.
    delayed: Option<(oneshot::Sender<()>, Box<VdevFut>)>,

    /// Max commands that will be simultaneously queued to the VdevLeaf
    optimum_queue_depth: u32,

    /// Current queue depth
    queue_depth: u32,

    /// Underlying device
    pub leaf: Box<VdevLeaf>,

    /// The last LBA issued an operation
    last_lba: LbaT,

    // Pending operations are stored in a pair of priority queues.  They _could_
    // be stored in a single queue, _if_ the priority queue's comparison
    // function were allowed to be stateful, as in C++'s STL.  However, Rust's
    // standard library does not have any way to create a priority queueful with
    // a stateful comparison function.
    /// Pending operations ahead of the scheduler's current LBA.
    ahead: BinaryHeap<BlockOp>,
    /// Pending operations behind the scheduler's current LBA.
    behind: BinaryHeap<BlockOp>,

    /// A `Weak` pointer back to `self`.  Used for closures that require a
    /// reference to `self`, but also require `'static` lifetime.
    weakself: Weak<RefCell<Inner>>
}

impl Inner {
    /// Issue as many scheduled operations as possible
    // Use the C-LOOK scheduling algorithm.  It guarantees that writes scheduled
    // in LBA order will also be issued in LBA order.
    fn issue_all(&mut self) {
        while self.queue_depth < self.optimum_queue_depth {
            let delayed = self.delayed.take();
            let (sender, fut) = if let Some((sender, fut)) = delayed {
                (sender, fut)
            } else if let Some(op) = self.pop_op() {
                self.make_fut(op)
            } else {
                // Ran out of pending operations
                break;
            };
            if let Some(d) = self.issue_fut(sender, fut) {
                self.delayed = Some(d);
                if self.queue_depth == 1 {
                    // TODO this sleep works if there is only one VdevBlock in
                    // the entire reactor.  But if there are more than one, then
                    // we need a tokio-enabled sleep so the other VdevBlocks can
                    // complete their futures.  tokio-timer has lousy
                    // performance, and tokio doesn't yet have a builtin timer,
                    // so I'll defer this until it does.
                    thread::sleep(time::Duration::from_millis(10));
                    continue;
                }
                break;
            }
        }
        // Ran out of operations to issue or exceeded queue depth.  If queue
        // depth was exceeded, an operation's completion will call issue_all
        // again.
    }

    /// Immediately issue one I/O future
    fn issue_fut(&mut self, sender: oneshot::Sender<()>, mut fut: Box<VdevFut>)
        -> Option<(oneshot::Sender<()>, Box<VdevFut>)> {

        let weakself = self.weakself.clone();
        let inner = weakself.upgrade().expect(
            "VdevBlock dropped with outstanding I/O");

        // Certain errors, like EAGAIN, happen synchronously.  If the future is
        // going to fail synchronously, then we want to handle the error
        // synchronously.  So we will poll it once before spawning it into the
        // reactor.
        match fut.poll() {
            Err(nix::Error::Sys(nix::errno::Errno::EAGAIN)) => {
                // Out of resources to issue this future.  Delay it
                return Some((sender, fut));
            },
            Err(e) => panic!("got error {:?}", e),
            Ok(r) => {
                match r {
                    Async::NotReady => {
                        current_thread::spawn(
                            fut.and_then(move |_| {
                                sender.send(()).unwrap();
                                inner.borrow_mut().queue_depth -= 1;
                                inner.borrow_mut().issue_all();
                                Ok(())
                            })
                            .map_err(|e| {
                                panic!("Unhandled error {:?}", e);
                            })
                        );
                    },
                    Async::Ready(_) => {
                        // This normally doesn't happen, but it can happen on a
                        // heavily laden system or one with very fast storage.
                        sender.send(()).unwrap();
                        self.queue_depth -= 1;
                    }
                }
            }
        }
        None
    }

    /// Create a future from a BlockOp, but don't spawn it yet
    fn make_fut(&mut self, block_op: BlockOp)
        -> (oneshot::Sender<()>, Box<VdevFut>) {

        self.queue_depth += 1;
        let lba = block_op.lba;

        // In the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        let fut = match block_op.cmd {
            Cmd::WriteAt(iovec) => self.leaf.write_at(iovec, lba),
            Cmd::ReadAt(iovec_mut) => self.leaf.read_at(iovec_mut, lba),
            Cmd::WritevAt(sglist) => self.leaf.writev_at(sglist, lba),
            Cmd::ReadvAt(sglist_mut) => self.leaf.readv_at(sglist_mut, lba),
            Cmd::EraseZone(start) => self.leaf.erase_zone(start),
            Cmd::FinishZone(start) => self.leaf.finish_zone(start),
            Cmd::OpenZone => self.leaf.open_zone(lba),
        };
        (block_op.sender, fut)
    }

    /// Get the next pending operation, if any
    fn pop_op(&mut self) -> Option<BlockOp> {
        if let Some(op) = self.ahead.pop() {
            self.last_lba = op.lba;
            Some(op)
        } else if !self.behind.is_empty() {
            // Ran out of operations ahead of the scheduler.  Go back to the
            // beginning
            mem::swap(&mut self.behind, &mut self.ahead);
            let op = self.ahead.pop().unwrap();
            self.last_lba = op.lba;
            Some(op)
        } else {
            // Ran out of operations everywhere.  Prepare to idle
            None
        }
    }

    /// Schedule the `block_op`
    fn sched(&mut self, block_op: BlockOp) {
        if block_op.lba >= self.last_lba {
            self.ahead.push(block_op);
        } else {
            self.behind.push(block_op);
        }
    }

    /// Schedule the `block_op`, and try to issue it
    fn sched_and_issue(&mut self, block_op: BlockOp) {
        self.sched(block_op);
        self.issue_all();
    }
}

/// `VdevBlock`: Virtual Device for basic block device
///
/// Wraps a single `VdevLeaf`.  But unlike `VdevLeaf`, `VdevBlock` operations
/// are scheduled.  They may not be issued in the order requested, and they may
/// not be issued immediately.
pub struct VdevBlock {
    inner: Rc<RefCell<Inner>>,

    /// Handle to a Tokio reactor
    handle: Handle,

    /// Usable size of the vdev, in LBAs
    size:   LbaT,
}

impl VdevBlock {
    /// Helper function for read and write methods
    fn check_iovec_bounds(&self, lba: LbaT, buf: &[u8]) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (dva::BYTES_PER_LBA as u64);
        assert!(last_lba < self.size as u64)
    }

    /// Helper function for readv and writev methods
    fn check_sglist_bounds<T>(&self, lba: LbaT, bufs: &[T])
        where T: ops::Deref<Target=[u8]> {

        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (dva::BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Asynchronously erase a zone on a block device
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut> {
        // The zone must already be closed, but VdevBlock doesn't keep enough
        // information to assert that
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::erase_zone(start, end, sender);

        // Sanity check LBAs
        #[cfg(test)]
        {
            let inner = self.inner.borrow();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the end of a zone
            debug_assert_eq!(start, limits.0);
            debug_assert_eq!(end, limits.1 - 1);
        }

        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// Asynchronously finish a zone on a block device
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn finish_zone(&self, start: LbaT, end: LbaT) -> Box<VdevBlockFut> {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::finish_zone(start, end, sender);

        // Sanity check LBAs
        #[cfg(test)]
        {
            let inner = self.inner.borrow();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the end of a zone
            debug_assert_eq!(start, limits.0);
            debug_assert_eq!(end, limits.1 - 1);
        }

        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// Asynchronously open a zone on a block device
    ///
    /// # Parameters
    /// - `start`:    The first LBA within the target zone
    pub fn open_zone(&self, start: LbaT) -> Box<VdevBlockFut> {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::open_zone(start, sender);

        // Sanity check LBA
        #[cfg(test)]
        {
            let inner = self.inner.borrow();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the begining of a zone
            debug_assert_eq!(start, limits.0);
            // The scheduler must not currently reside in this zone.  That would
            // imply that we just operated on an empty zone
            debug_assert!(inner.last_lba >= limits.1 ||
                          inner.last_lba < limits.0);
        }

        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// Open a VdevBlock
    ///
    /// * `leaf`    An already-open underlying VdevLeaf 
    // The 'static enforces that if the VdevLeaf implementor contains any
    // references, they must be 'static.
    pub fn open<T: VdevLeaf + 'static>(leaf: Box<T>, handle: Handle) -> Self {
        let size = leaf.size();
        let inner = Rc::new(RefCell::new(Inner {
            delayed: None,
            optimum_queue_depth: leaf.optimum_queue_depth(),
            queue_depth: 0,
            leaf,
            last_lba: 0,
            ahead: BinaryHeap::new(),
            behind: BinaryHeap::new(),
            weakself: Weak::new()
        }));
        inner.borrow_mut().weakself = Rc::downgrade(&inner);
        VdevBlock {
            inner,
            handle,
            size,
        }
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::read_at(buf, lba, sender);
        self.inner.borrow_mut().sched_and_issue(block_op);
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
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::readv_at(bufs, lba, sender);
        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns nothing on success, and on error on failure
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::write_at(buf, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not support fragmentary writes");
        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// Returns nothing on success, or an error on failure
    ///
    /// # Parameters
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA at which to write
    pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevBlockFut> {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::writev_at(bufs, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not support fragmentary writes");
        self.inner.borrow_mut().sched_and_issue(block_op);
        Box::new(receiver.map_err(|_| nix::Error::from(nix::errno::Errno::EPIPE)))
    }
}

impl Vdev for VdevBlock {
    fn handle(&self) -> Handle {
        self.handle.clone()
    }

    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.inner.borrow().leaf.lba2zone(lba)
    }

    /// Returns the "best" number of operations to queue to this `VdevBlock`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    fn optimum_queue_depth(&self) -> u32 {
        self.inner.borrow().optimum_queue_depth
    }

    fn size(&self) -> LbaT {
        self.size
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.inner.borrow().leaf.zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.inner.borrow().leaf.zones()
    }
}

#[cfg(feature = "mocks")]
#[cfg(test)]
test_suite! {
    name t;

    use super::*;
    use divbuf::DivBufShared;
    use futures;
    use futures::{Poll, future};
    use mockers::{Scenario, Sequence};
    use mockers::matchers::ANY;
    use permutohedron;
    use tokio::executor::current_thread;
    use tokio::reactor::Handle;

    mock!{
        MockVdevLeaf2,
        vdev,
        trait Vdev {
            fn handle(&self) -> Handle;
            fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
            fn optimum_queue_depth(&self) -> u32;
            fn size(&self) -> LbaT;
            fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
            fn zones(&self) -> ZoneT;
        },
        vdev_leaf,
        trait VdevLeaf  {
            fn erase_zone(&self, lba: LbaT) -> Box<VdevFut>;
            fn finish_zone(&self, lba: LbaT) -> Box<VdevFut>;
            fn open_zone(&self, lba: LbaT) -> Box<VdevFut>;
            fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;
            fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevFut>;
            fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
            fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
        }
    }

    mock!{
        MockVdevFut,
        futures,
        trait Future {
            type Item = ();
            type Error = nix::Error;
            fn poll(&mut self) -> Poll<Item, Error>;
        },
    }

    fixture!( mocks() -> (Scenario, Box<MockVdevLeaf2>) {
            setup(&mut self) {
            let scenario = Scenario::new();
            let leaf = Box::new(scenario.create_mock::<MockVdevLeaf2>());
            scenario.expect(leaf.size_call()
                                .and_return(16384));
            scenario.expect(leaf.lba2zone_call(ANY)
                                .and_return_clone(Some(0))
                                .times(..));
            scenario.expect(leaf.optimum_queue_depth_call()
                                .and_return_clone(10)
                                .times(..));
            scenario.expect(leaf.zone_limits_call(0)
                                .and_return_clone((0, 1 << 16))
                                .times(..));
            scenario.expect(leaf.zone_limits_call(1)
                                .and_return_clone((1 << 16, 2 << 16))
                                .times(..));
            scenario.expect(leaf.zone_limits_call(2)
                                .and_return_clone((2 << 16, 3 << 16))
                                .times(..));
            (scenario, leaf)
        }
    });

    // Issueing an operation fails with EAGAIN.  This can happen if the
    // per-process or per-system AIO limits are reached
    test issueing_eagain(mocks) {
        let scenario = mocks.val.0;
        let s_handle = scenario.handle();
        let leaf = mocks.val.1;
        let mut seq0 = Sequence::new();

        let (sender, receiver) = oneshot::channel::<()>();
        let e = nix::Error::from(nix::errno::Errno::EPIPE);
        let fut0 = receiver.map_err(move |_| e);
        // The first operation succeeds.  When it does, that will cause the
        // second to be reissued
        seq0.expect(leaf.read_at_call(ANY, 0).and_return(Box::new(fut0)));
        seq0.expect(leaf.read_at_call(ANY, 1)
            .and_call( move |_, _| {
                let mut seq1 = Sequence::new();
                let fut = s_handle.create_mock::<MockVdevFut<(), nix::Error>>();
                seq1.expect(fut.poll_call()
                    .and_return(
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN))));
                seq1.expect(fut.poll_call().and_return(Ok(Async::Ready(()))));
                s_handle.expect(seq1);
                Box::new(fut)
            }));
        scenario.expect(seq0);
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            let f0 = vdev.read_at(rbuf0, 0);
            let f1 = vdev.read_at(rbuf1, 1);
            sender.send(()).expect("send");
            f0.join(f1)
        })).expect("test eagain");
    }

    // Issueing an operation fails with EAGAIN, when the queue depth is 0.  This
    // can happen if the per-process or per-system AIO limits are monopolized by
    // other reactors.  In this case, we need a timer to wake us up.
    test issueing_eagain_queue_depth_0(mocks) {
        let scenario = mocks.val.0;
        let s_handle = scenario.handle();
        let leaf = mocks.val.1;
        let mut seq0 = Sequence::new();

        seq0.expect(leaf.read_at_call(ANY, 0)
            .and_call( move |_, _| {
                let mut seq1 = Sequence::new();
                let fut = s_handle.create_mock::<MockVdevFut<(), nix::Error>>();
                seq1.expect(fut.poll_call()
                    .and_return(
                        Err(nix::Error::Sys(nix::errno::Errno::EAGAIN))));
                seq1.expect(fut.poll_call()
                    .and_return(Ok(Async::Ready(()))));
                s_handle.expect(seq1);
                Box::new(fut)
            }));
        scenario.expect(seq0);
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.read_at(rbuf, 0)
        })).expect("test eagain_queue_depth_0");
    }

    // basic reading works
    test basic_read_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        seq.expect(leaf.read_at_call(ANY, 1)
                       .and_return(Box::new(future::ok::<(), nix::Error>(()))));
        scenario.expect(seq);

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.read_at(rbuf0, 1)
        })).unwrap();
    }

    // vectored reading works
    test basic_readv_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();
        seq.expect(leaf.readv_at_call(ANY, 1)
                       .and_return(Box::new(future::ok::<(), nix::Error>(()))));
        scenario.expect(seq);

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = vec![dbs0.try_mut().unwrap()];
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.readv_at(rbuf0, 1)
        })).unwrap();
    }

    // data operations will be issued in C-LOOK order (from lowest LBA to
    // highest, then start over at lowest)
    test sched_data(mocks) {
        let leaf = mocks.val.1;
        let vdev = VdevBlock::open(leaf, Handle::current());
        let mut inner = vdev.inner.borrow_mut();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy_buffer = dummy_dbs.try().unwrap();

        // Start with some intermedia LBA and schedule some ops
        let same_lba = 1000;
        let just_after = 1001;
        let just_after_dup = 1001;
        let just_before = 999;
        let min = 0;
        let max = 16383;
        let mut lbas = vec![same_lba, just_after, just_after_dup, just_before,
                            min, max];
        // Test scheduling the ops in all possible permutations
        permutohedron::heap_recursive(&mut lbas, |permutation| {
            inner.last_lba = 1000;
            for lba in permutation {
                let op = BlockOp::write_at(dummy_buffer.clone(), *lba,
                    oneshot::channel::<()>().0);
                inner.sched(op);
            }

            // Check that they're scheduled in the correct order
            assert_eq!(inner.pop_op().unwrap().lba, 1000);
            assert_eq!(inner.pop_op().unwrap().lba, 1001);
            assert_eq!(inner.pop_op().unwrap().lba, 1001);

            // Schedule two more operations behind the scheduler, but ahead of
            // some already-scheduled ops, to make sure they get issued in the
            // right order
            let just_before2 = BlockOp::write_at(dummy_buffer.clone(), 1000,
                oneshot::channel::<()>().0);
            let well_before = BlockOp::write_at(dummy_buffer.clone(), 990,
                oneshot::channel::<()>().0);
            inner.sched(just_before2);
            inner.sched(well_before);

            assert_eq!(inner.pop_op().unwrap().lba, 16383);
            assert_eq!(inner.pop_op().unwrap().lba, 0);
            assert_eq!(inner.pop_op().unwrap().lba, 990);
            assert_eq!(inner.pop_op().unwrap().lba, 999);
            assert_eq!(inner.pop_op().unwrap().lba, 1000);
            assert!(inner.pop_op().is_none());
        });
    }

    // An erase zone command should be scheduled after any reads from that zone
    test sched_erase_zone(mocks) {
        let leaf = mocks.val.1;
        let vdev = VdevBlock::open(leaf, Handle::current());
        let mut inner = vdev.inner.borrow_mut();
        let dummy_dbs = DivBufShared::from(vec![0; 12288]);
        let mut dummy = dummy_dbs.try_mut().unwrap();

        inner.last_lba = 1 << 16;   // In zone 1
        // Read from zones that lie behind, around, and ahead of the scheduler,
        // then erase them.  This simulates garbage collection.
        let ez0 = BlockOp::erase_zone(0, (1 << 16) - 1,
            oneshot::channel::<()>().0);
        let ez_discriminant = mem::discriminant(&ez0.cmd);
        inner.sched(ez0);
        let r = BlockOp::read_at(dummy.split_to(4096), (1 << 16) - 1,
            oneshot::channel::<()>().0);
        let read_at_discriminant = mem::discriminant(&r.cmd);
        inner.sched(r);
        inner.sched(BlockOp::erase_zone(1 << 16, (2 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::read_at(dummy.split_to(4096), (2 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::erase_zone(2 << 16, (3 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::read_at(dummy, (3 << 16) - 1,
            oneshot::channel::<()>().0));

        let first = inner.pop_op().unwrap();
        assert_eq!(first.lba, (2 << 16) - 1);
        assert_eq!(mem::discriminant(&first.cmd), read_at_discriminant);
        let second = inner.pop_op().unwrap();
        assert_eq!(second.lba, (2 << 16) - 1);
        assert_eq!(mem::discriminant(&second.cmd), ez_discriminant);
        let third = inner.pop_op().unwrap();
        assert_eq!(third.lba, (3 << 16) - 1);
        assert_eq!(mem::discriminant(&third.cmd), read_at_discriminant);
        let fourth = inner.pop_op().unwrap();
        assert_eq!(fourth.lba, (3 << 16) - 1);
        assert_eq!(mem::discriminant(&fourth.cmd), ez_discriminant);
        let fifth = inner.pop_op().unwrap();
        assert_eq!(fifth.lba, (1 << 16) - 1);
        assert_eq!(mem::discriminant(&fifth.cmd), read_at_discriminant);
        let sixth = inner.pop_op().unwrap();
        assert_eq!(sixth.lba, (1 << 16) - 1);
        assert_eq!(mem::discriminant(&sixth.cmd), ez_discriminant);
    }

    // A finish zone command should be scheduled after any writes to that zone
    test sched_finish_zone(mocks) {
        let leaf = mocks.val.1;
        let vdev = VdevBlock::open(leaf, Handle::current());
        let mut inner = vdev.inner.borrow_mut();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy = dummy_dbs.try().unwrap();

        inner.last_lba = 1 << 16;   // In zone 1
        // Write to zones that lie behind, around, and ahead of the scheduler,
        // then finish them.
        let fz0 = BlockOp::finish_zone(0, (1 << 16) - 1,
            oneshot::channel::<()>().0);
        let fz_discriminant = mem::discriminant(&fz0.cmd);
        inner.sched(fz0);
        let r = BlockOp::write_at(dummy.clone(), (1 << 16) - 1,
            oneshot::channel::<()>().0);
        let write_at_discriminant = mem::discriminant(&r.cmd);
        inner.sched(r);
        inner.sched(BlockOp::finish_zone(1 << 16, (2 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), (2 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::finish_zone(2 << 16, (3 << 16) - 1,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy, (3 << 16) - 1,
            oneshot::channel::<()>().0));

        let first = inner.pop_op().unwrap();
        assert_eq!(first.lba, (2 << 16) - 1);
        assert_eq!(mem::discriminant(&first.cmd), write_at_discriminant);
        let second = inner.pop_op().unwrap();
        assert_eq!(second.lba, (2 << 16) - 1);
        assert_eq!(mem::discriminant(&second.cmd), fz_discriminant);
        let third = inner.pop_op().unwrap();
        assert_eq!(third.lba, (3 << 16) - 1);
        assert_eq!(mem::discriminant(&third.cmd), write_at_discriminant);
        let fourth = inner.pop_op().unwrap();
        assert_eq!(fourth.lba, (3 << 16) - 1);
        assert_eq!(mem::discriminant(&fourth.cmd), fz_discriminant);
        let fifth = inner.pop_op().unwrap();
        assert_eq!(fifth.lba, (1 << 16) - 1);
        assert_eq!(mem::discriminant(&fifth.cmd), write_at_discriminant);
        let sixth = inner.pop_op().unwrap();
        assert_eq!(sixth.lba, (1 << 16) - 1);
        assert_eq!(mem::discriminant(&sixth.cmd), fz_discriminant);
    }

    // An open zone command should be scheduled before any writes to that zone
    test sched_open_zone(mocks) {
        let leaf = mocks.val.1;
        let vdev = VdevBlock::open(leaf, Handle::current());
        let mut inner = vdev.inner.borrow_mut();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy = dummy_dbs.try().unwrap();

        inner.last_lba = 1 << 16;   // In zone 1
        // Open zones 0 and 2 and write to both.  Note that it is illegal for
        // the scheduler's last_lba to lie within either of these zones, because
        // that would imply that it had just performed an operation on an empty
        // zone.
        let w = BlockOp::write_at(dummy.clone(), 0, oneshot::channel::<()>().0);
        let write_at_discriminant = mem::discriminant(&w.cmd);
        inner.sched(w);
        inner.sched(BlockOp::write_at(dummy.clone(), (1 << 16) - 1,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), 1,
                    oneshot::channel::<()>().0));
        let oz0 = BlockOp::open_zone(0, oneshot::channel::<()>().0);
        let oz_discriminant = mem::discriminant(&oz0.cmd);
        inner.sched(oz0);
        inner.sched(BlockOp::open_zone(2 << 16, oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), (2 << 16) + 1,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), 2 << 16,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), (3 << 16) - 1,
                    oneshot::channel::<()>().0));

        let first = inner.pop_op().unwrap();
        assert_eq!(first.lba, 2 << 16);
        assert_eq!(mem::discriminant(&first.cmd), oz_discriminant);
        let second = inner.pop_op().unwrap();
        assert_eq!(second.lba, 2 << 16);
        assert_eq!(mem::discriminant(&second.cmd), write_at_discriminant);
        assert_eq!(inner.pop_op().unwrap().lba, (2 << 16) + 1);
        assert_eq!(inner.pop_op().unwrap().lba, (3 << 16) - 1);
        let fifth = inner.pop_op().unwrap();
        assert_eq!(fifth.lba, 0);
        assert_eq!(mem::discriminant(&fifth.cmd), oz_discriminant);
        let sixth = inner.pop_op().unwrap();
        assert_eq!(sixth.lba, 0);
        assert_eq!(mem::discriminant(&sixth.cmd), write_at_discriminant);
        assert_eq!(inner.pop_op().unwrap().lba, 1);
        assert_eq!(inner.pop_op().unwrap().lba, (1 << 16) - 1);
        assert!(inner.pop_op().is_none());
    }

    // Queued operations will both complete
    test issueing_queued(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let mut seq = Sequence::new();

        let (sender, receiver) = oneshot::channel::<()>();
        let e = nix::Error::from(nix::errno::Errno::EPIPE);
        let fut0 = receiver.map_err(move |_| e);
        let fut1 = future::ok::<(), nix::Error>(());
        seq.expect(leaf.read_at_call(ANY, 0)
                       .and_return(Box::new(fut0)));
        seq.expect(leaf.read_at_call(ANY, 1)
                       .and_call(|_, _| {
                           sender.send(()).unwrap();
                           Box::new(fut1)
                       }));
        scenario.expect(seq);
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            let f0 = vdev.read_at(rbuf0, 0);
            let f1 = vdev.read_at(rbuf1, 1);
            f0.join(f1)
        })).unwrap();
    }

    // Operations will be buffered after the max queue depth is reached
    // The first MAX_QUEUE_DEPTH operations will be issued immediately, in the
    // order in which they are requested.  Subsequent operations will be
    // reordered into LBA order
    test issueing_queue_depth(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        let num_ops = leaf.optimum_queue_depth() + 2;
        let mut seq = Sequence::new();

        let channels = (0..num_ops - 2).map(|_| oneshot::channel::<()>());
        let (futs, senders) : (Vec<_>, Vec<_>) = channels.map(|chan| {
            let e = nix::Error::from(nix::errno::Errno::EPIPE);
            (chan.1.map_err(move |_| e), chan.0)
        })
        .unzip();
        for (i, f) in futs.into_iter().enumerate().rev() {
            seq.expect(leaf.write_at_call(ANY, i as LbaT)
                           .and_return(Box::new(f)));
        }
        // Schedule the final two operations in reverse LBA order, but verify
        // that they get issued in actual LBA order
        let final_fut = future::ok::<(), nix::Error>(());
        seq.expect(leaf.write_at_call(ANY, num_ops as LbaT - 2)
                            .and_call(|_, _| {
                                Box::new(final_fut)
                            }));
        let penultimate_fut = future::ok::<(), nix::Error>(());
        seq.expect(leaf.write_at_call(ANY, num_ops as LbaT - 1)
                            .and_call(|_, _| {
                                Box::new(penultimate_fut)
                            }));
        scenario.expect(seq);
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            // First schedule all operations.  There are too many to issue them
            // all immediately
            let unbuf_fut = future::join_all((0..num_ops - 2).rev().map(|i| {
                vdev.write_at(wbuf.clone(), i as LbaT)
            }));
            let penultimate_fut = vdev.write_at(wbuf.clone(),
                                                (num_ops - 1) as LbaT);
            let final_fut = vdev.write_at(wbuf.clone(),
                                          (num_ops - 2) as LbaT);
            let fut = unbuf_fut.join3(penultimate_fut, final_fut);
            // Verify that they weren't all issued
            let inner = vdev.inner.borrow_mut();
            assert_eq!(inner.ahead.len() + inner.behind.len(), 2);
            // Finally, complete them.
            for chan in senders {
                chan.send(()).unwrap();
            }
            fut
        })).unwrap();
    }

    // Basic writing works
    test basic_write_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.write_at_call(ANY, 0)
                    .and_return(Box::new(future::ok::<(), nix::Error>(()))));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try().unwrap();
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.write_at(wbuf, 0)
        })).unwrap();
    }

    // vectored writing works
    test basic_writev_at(mocks) {
        let scenario = mocks.val.0;
        let leaf = mocks.val.1;
        scenario.expect(leaf.writev_at_call(ANY, 0)
                    .and_return(Box::new(future::ok::<(), nix::Error>(()))));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = vec![dbs.try().unwrap()];
        let vdev = VdevBlock::open(leaf, Handle::current());
        current_thread::block_on_all(future::lazy(|| {
            vdev.writev_at(wbuf, 0)
        })).unwrap();
    }
}
