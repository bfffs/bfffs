// vim: tw=80

use futures::{
    Future,
    FutureExt,
    TryFutureExt,
    channel::oneshot,
    task::{Context, Poll}
};
use std::{
    cmp::{Ord, Ordering, PartialOrd},
    collections::BinaryHeap,
    collections::VecDeque,
    io,
    mem,
    num::NonZeroU64,
    path::Path,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    ops,
    time,
};
#[cfg(test)] use mockall::*;

use crate::common::{*, label::*, vdev::*, vdev_file::*};

#[cfg(test)]
pub type VdevLeaf = MockVdevFile;
#[cfg(not(test))]
pub type VdevLeaf = VdevFile;

#[derive(Debug)]
enum Cmd {
    OpenZone,
    ReadAt(IoVecMut),
    ReadSpacemap(IoVecMut, u32),
    ReadvAt(SGListMut),
    WriteAt(IoVec),
    WritevAt(SGList),
    // The extra LBA is the zone's starting LBA
    EraseZone(LbaT),
    // The extra LBA is the zone's starting LBA
    FinishZone(LbaT),
    WriteLabel(LabelWriter),
    WriteSpacemap(SGList, u32, LbaT),
    SyncAll,
}

impl Cmd {
    // Oh, this would be so much easier if only `std::mem::Discriminant`
    // implemented `Ord`!
    fn discriminant(&self) -> i32 {
        match *self {
            Cmd::OpenZone => 0,
            Cmd::ReadAt(_) => 1,
            Cmd::ReadSpacemap(_, _) => 2,
            Cmd::ReadvAt(_) => 3,
            Cmd::WriteAt(_) => 4,
            Cmd::WritevAt(_) => 5,
            Cmd::EraseZone(_) => 6,
            Cmd::FinishZone(_) => 7,
            Cmd::WriteLabel(_) => 8,
            Cmd::WriteSpacemap(_, _, _) => 9,
            Cmd::SyncAll => 10,
        }
    }   // LCOV_EXCL_LINE   kcov false negative
}

impl Eq for Cmd {
}

impl Ord for Cmd {
    /// Compare `Cmd` type only, not contents
    fn cmp(&self, other: &Cmd) -> Ordering {
        self.discriminant().cmp(&other.discriminant())
    }
}

impl PartialEq for Cmd {
    /// Compare the `Cmd` type only, not its contents
    fn eq(&self, other: &Cmd) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Cmd {
    /// Compare the `Cmd` type only, not its contents
    fn partial_cmp(&self, other: &Cmd) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

    pub fn read_spacemap(buf: IoVecMut, lba: LbaT, idx: u32,
                         sender: oneshot::Sender<()>) -> BlockOp
    {
        BlockOp { lba, cmd: Cmd::ReadSpacemap(buf, idx), sender}
    }

    pub fn readv_at(bufs: SGListMut, lba: LbaT,
                    sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::ReadvAt(bufs), sender}
    }

    pub fn sync_all(sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba: 0, cmd: Cmd::SyncAll, sender}
    }

    pub fn write_at(buf: IoVec, lba: LbaT,
                    sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WriteAt(buf), sender}
    }

    pub fn write_label(labeller: LabelWriter,
                       sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba: 0, cmd: Cmd::WriteLabel(labeller), sender}
    }

    pub fn write_spacemap(sglist: SGList, lba: LbaT, idx: u32, block: LbaT,
                          sender: oneshot::Sender<()>) -> BlockOp
    {
        BlockOp { lba, cmd: Cmd::WriteSpacemap(sglist, idx, block), sender}
    }

    pub fn writev_at(bufs: SGList, lba: LbaT,
                     sender: oneshot::Sender<()>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WritevAt(bufs), sender}
    }
}

struct Inner {
    /// A VdevLeaf future that got delayed by an EAGAIN error.  We hold the
    /// future around instead of spawning it into the reactor.
    delayed: Option<(oneshot::Sender<()>, Pin<Box<VdevFut>>)>,

    /// Max commands that will be simultaneously queued to the VdevLeaf
    optimum_queue_depth: u32,

    /// Current queue depth
    queue_depth: u32,

    /// Underlying device
    pub leaf: VdevLeaf,

    /// The last LBA issued an operation
    last_lba: LbaT,

    /// If true, then we are preparing to issue sync_all to the underlying
    /// storage
    syncing: bool,

    // Pending operations are stored in a pair of priority queues.  They _could_
    // be stored in a single queue, _if_ the priority queue's comparison
    // function were allowed to be stateful, as in C++'s STL.  However, Rust's
    // standard library does not have any way to create a priority queueful with
    // a stateful comparison function.
    /// Pending operations ahead of the scheduler's current LBA.
    ahead: BinaryHeap<BlockOp>,

    /// Pending operations behind the scheduler's current LBA.
    behind: BinaryHeap<BlockOp>,

    /// Pending operations that should strictly follow a sync_all (possibly
    /// including other sync_all operations).  We store these in a FIFO because
    /// we can't correctly schedule them until the sync_all is complete.
    after_sync: VecDeque<BlockOp>,

    /// A `Weak` pointer back to `self`.  Used for closures that require a
    /// reference to `self`, but also require `'static` lifetime.
    weakself: Weak<RwLock<Inner>>
}

impl Inner {
    /// Issue as many scheduled operations as possible
    // Use the C-LOOK scheduling algorithm.  It guarantees that writes scheduled
    // in LBA order will also be issued in LBA order.
    fn issue_all(&mut self, cx: &mut Context) {
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
            if let Some(d) = self.issue_fut(sender, fut, cx) {
                self.delayed = Some(d);
                if self.queue_depth == 1 {
                    // Can't issue any I/O at all!  This means that other
                    // processes outside of bfffs's control are using too many
                    // disk resources.  In this case, the only thing we can do
                    // is sleep and try again later.
                    let duration = time::Duration::from_millis(10);
                    let schfut = self.reschedule();
                    let delay_fut = tokio::time::delay_for(duration)
                    .then(move |_| schfut);
                    tokio::spawn(delay_fut);
                }
                break;
            }
        }
        // Ran out of operations to issue or exceeded queue depth.  If queue
        // depth was exceeded, an operation's completion will call issue_all
        // again.
    }

    /// Immediately issue one I/O future.
    ///
    /// Returns a delayed operation if there were insufficient resources to
    /// immediately issue the future.
    fn issue_fut(&mut self,
                 sender: oneshot::Sender<()>,
                 mut fut: Pin<Box<VdevFut>>,
                 cx: &mut Context)
        -> Option<(oneshot::Sender<()>, Pin<Box<VdevFut>>)>
    {

        let inner = self.weakself.upgrade().expect(
            "VdevBlock dropped with outstanding I/O");

        // Certain errors, like EAGAIN, happen synchronously.  If the future is
        // going to fail synchronously, then we want to handle the error
        // synchronously.  So we will poll it once before spawning it into the
        // reactor.
        match fut.as_mut().poll(cx) {
            Poll::Ready(Err(Error::EAGAIN)) => {
                // Out of resources to issue this future.  Delay it.
                return Some((sender, fut));
            },
            Poll::Ready(Err(e)) => panic!("Unhandled error {:?}", e),
            Poll::Pending => {
                let schfut = self.reschedule();
                tokio::spawn( async move {
                    let r = fut.await;
                    r.expect("Unhandled error");
                    sender.send(()).unwrap();
                    inner.write().unwrap().queue_depth -= 1;
                    schfut.await
                });
            },
            Poll::Ready(Ok(_)) => {
                // This normally doesn't happen, but it can happen on a
                // heavily laden system or one with very fast storage.
                sender.send(()).unwrap();
                self.queue_depth -= 1;
            }
        }
        None
    }

    /// Create a future from a BlockOp, but don't spawn it yet
    fn make_fut(&mut self, block_op: BlockOp)
        -> (oneshot::Sender<()>, Pin<Box<VdevFut>>) {

        self.queue_depth += 1;
        let lba = block_op.lba;

        // In the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        let fut: Pin<Box<VdevFut>> = match block_op.cmd {
            Cmd::WriteAt(iovec) => self.leaf.write_at(iovec, lba),
            Cmd::ReadAt(iovec_mut) => self.leaf.read_at(iovec_mut, lba),
            Cmd::WritevAt(sglist) => self.leaf.writev_at(sglist, lba),
            Cmd::ReadSpacemap(iovec_mut, idx) =>
                    self.leaf.read_spacemap(iovec_mut, idx),
            Cmd::ReadvAt(sglist_mut) => self.leaf.readv_at(sglist_mut, lba),
            Cmd::EraseZone(start) => self.leaf.erase_zone(start),
            Cmd::FinishZone(start) => self.leaf.finish_zone(start),
            Cmd::OpenZone => self.leaf.open_zone(lba),
            Cmd::WriteLabel(labeller) => self.leaf.write_label(labeller),
            Cmd::WriteSpacemap(sglist, idx, block) =>
                self.leaf.write_spacemap(sglist, idx, block),
            Cmd::SyncAll => self.leaf.sync_all(),
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
        } else if self.syncing {
            debug_assert!(!self.after_sync.is_empty());
            let op = self.after_sync.pop_front().unwrap();
            if op.cmd == Cmd::SyncAll {
                // If this op was the sync_all, then reschedule all the
                // following operations (until and unless there's another
                // sync_all)
                self.syncing = false;
                loop {
                    if self.after_sync.is_empty() {
                        break;
                    }
                    if self.after_sync.front().unwrap().cmd == Cmd::SyncAll {
                        self.syncing = true;
                        break;
                    }
                    let next_op = self.after_sync.pop_front().unwrap();
                    self.sched(next_op);
                }
            }
            Some(op)
        } else {    // LCOV_EXCL_LINE   kcov false negative
            // Ran out of operations everywhere.  Prepare to idle
            None
        }
    }

    /// Create a future which, when polled, will advanced the scheduler,
    /// issueing more disk ops if any are waiting.
    fn reschedule(&self) -> ReschedFut {
        ReschedFut(self.weakself.clone())
    }

    /// Schedule the `block_op`
    fn sched(&mut self, block_op: BlockOp) {
        if block_op.cmd == Cmd::SyncAll || self.syncing {
            self.syncing = true;
            self.after_sync.push_back(block_op);
        } else if block_op.lba >= self.last_lba {
            self.ahead.push(block_op);
        } else {
            self.behind.push(block_op);
        }
    }

    /// Schedule the `block_op`, and try to issue it
    fn sched_and_issue(&mut self, block_op: BlockOp, cx: &mut Context) {
        self.sched(block_op);
        self.issue_all(cx);
    }
}

struct ReschedFut(Weak<RwLock<Inner>>);

impl Future for ReschedFut {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = self.0.upgrade()
            .expect("VdevBlock dropped with outstanding I/O");
        inner.write().unwrap().issue_all(cx);
        Poll::Ready(())
    }
}

/// Return type for most `VdevBlock` asynchronous methods
pub struct VdevBlockFut {
    block_op: Option<BlockOp>,
    inner: Arc<RwLock<Inner>>,
    receiver: oneshot::Receiver<()>,
}

impl Future for VdevBlockFut {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.block_op.is_some() {
            let block_op = self.block_op.take().unwrap();
            self.inner.write().unwrap().sched_and_issue(block_op, cx);
        }
        Pin::new(&mut self.receiver).poll(cx)
            .map_err(|_| Error::EPIPE)
    }
}

/// `VdevBlock`: Virtual Device for basic block device
///
/// Wraps a single `VdevLeaf`.  But unlike `VdevLeaf`, `VdevBlock` operations
/// are scheduled.  They may not be issued in the order requested, and they may
/// not be issued immediately.
pub struct VdevBlock {
    inner: Arc<RwLock<Inner>>,

    /// Usable size of the vdev, in LBAs
    size:   LbaT,

    /// Size of a single spacemap as stored in the leaf vdev
    spacemap_space:  LbaT
}

impl VdevBlock {
    /// Helper function for read and write methods
    fn check_iovec_bounds(&self, lba: LbaT, buf: &[u8]) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (BYTES_PER_LBA as u64);
        assert!(last_lba < self.size as u64)
    }

    /// Helper function for readv and writev methods
    fn check_sglist_bounds<T>(&self, lba: LbaT, bufs: &[T])
        where T: ops::Deref<Target=[u8]> {

        let len : u64 = bufs.iter().fold(0, |accumulator, buf| {
            accumulator + buf.len() as u64
        });
        assert!(lba + len / (BYTES_PER_LBA as u64) < self.size as u64)
    }

    /// Create a new VdevBlock from an unused file or device
    ///
    /// * `path`:           A pathname to a file or device
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
        where P: AsRef<Path> + 'static
    {
        let leaf = VdevLeaf::create(path, lbas_per_zone)?;
        Ok(VdevBlock::new(leaf))
    }

    /// Asynchronously erase a zone on a block device
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn erase_zone(&self, start: LbaT, end: LbaT) -> VdevBlockFut
    {
        // The zone must already be closed, but VdevBlock doesn't keep enough
        // information to assert that
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::erase_zone(start, end, sender);

        // Sanity check LBAs
        #[cfg(debug_assertions)]
        {
            let inner = self.inner.read().unwrap();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the end of a zone
            debug_assert_eq!(start, limits.0);
            debug_assert_eq!(end, limits.1 - 1);
        }

        self.new_fut(block_op, receiver)
    }

    /// Asynchronously finish a zone on a block device
    ///
    /// # Parameters
    /// - `start`:  The first LBA within the target zone
    /// - `end`:    The last LBA within the target zone
    pub fn finish_zone(&self, start: LbaT, end: LbaT) -> VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::finish_zone(start, end, sender);

        // Sanity check LBAs
        #[cfg(debug_assertions)]
        {
            let inner = self.inner.read().unwrap();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the end of a zone to ensure that the operation
            // will be sorted correctly
            debug_assert_eq!(start, limits.0);
            debug_assert_eq!(end, limits.1 - 1);
        }

        self.new_fut(block_op, receiver)
    }

    fn new_fut(&self, block_op: BlockOp,
               receiver: oneshot::Receiver<()>) -> VdevBlockFut {
        VdevBlockFut {
            block_op: Some(block_op),
            inner: self.inner.clone(),
            receiver
        }   // LCOV_EXCL_LINE   kcov false negative
    }

    /// Asynchronously open a zone on a block device
    ///
    /// # Parameters
    /// - `start`:    The first LBA within the target zone
    pub fn open_zone(&self, start: LbaT) -> VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::open_zone(start, sender);

        // Sanity check LBA
        #[cfg(debug_assertions)]
        {
            let inner = self.inner.read().unwrap();
            let limits = inner.leaf.zone_limits(
                inner.leaf.lba2zone(start).unwrap());
            // The LBA must be the begining of a zone
            debug_assert_eq!(start, limits.0);
        }

        self.new_fut(block_op, receiver)
    }

    /// Instantiate a new VdevBlock from an existing VdevLeaf
    ///
    /// * `leaf`    An already-open underlying VdevLeaf
    pub fn new(leaf: VdevLeaf) -> Self {
        let size = leaf.size();
        let spacemap_space = leaf.spacemap_space();
        let inner = Arc::new(RwLock::new(Inner {
            delayed: None,
            optimum_queue_depth: leaf.optimum_queue_depth(),
            queue_depth: 0,
            leaf,
            last_lba: 0,
            syncing: false,
            after_sync: VecDeque::new(),
            ahead: BinaryHeap::new(),
            behind: BinaryHeap::new(),
            weakself: Weak::new()
        }));    // LCOV_EXCL_LINE   kcov false negative
        inner.write().unwrap().weakself = Arc::downgrade(&inner);
        VdevBlock {
            inner,
            size,
            spacemap_space
        }
    }

    /// Open an existing `VdevBlock`
    ///
    /// Returns both a new `VdevBlock` object, and a `LabelReader` that may be
    /// used to construct other vdevs stacked on top of this one.
    ///
    /// * `path`    Pathname for the backing file.  It may be a device node.
    #[cfg(test)]
    pub async fn open<P: AsRef<Path> + 'static>(path: P)
        -> Result<(Self, LabelReader), Error>
    {
        VdevLeaf::open(path)
        .map_ok(|(leaf, label_reader)| {
            (VdevBlock::new(leaf), label_reader)
        }).await
    }

    #[cfg(not(test))]
    pub async fn open<P: AsRef<Path>>(path: P)
        -> Result<(Self, LabelReader), Error>
    {
        VdevLeaf::open(path)
        .map_ok(|(leaf, label_reader)| {
            (VdevBlock::new(leaf), label_reader)
        }).await
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> VdevBlockFut
    {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::read_at(buf, lba, sender);
        self.new_fut(block_op, receiver)
    }

    /// Read the entire serialized spacemap.  `idx` selects which spacemap to
    /// read, and should match whichever label is being read concurrently.
    pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<()>();
        // lba is for sorting purposes only.  It should sort before any other
        // write operation, and different read_spacemap operations should sort
        // in the same order as their true LBA order.
        let lba = 1 + self.spacemap_space * LbaT::from(idx);
        let block_op = BlockOp::read_spacemap(buf, lba, idx, sender);
        self.new_fut(block_op, receiver)
    }

    /// The asynchronous scatter/gather read function.
    ///
    /// Returns nothing on success, and on error on failure
    ///
    /// # Parameters
    ///
    /// * `bufs`    Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> VdevBlockFut
    {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::readv_at(bufs, lba, sender);
        self.new_fut(block_op, receiver)
    }

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns nothing on success, and on error on failure
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> VdevBlockFut
    {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::write_at(buf, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not support fragmentary writes");
        self.new_fut(block_op, receiver)
    }

    pub fn write_label(&self, labeller: LabelWriter) -> VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::write_label(labeller, sender);
        self.new_fut(block_op, receiver)
    }

    pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<()>();
        // lba is for sorting purposes only.  It should sort after write_label,
        // but before any other write operation, and different write_spacemap
        // operations should sort in the same order as their true LBA order.
        let lba = 1 + self.spacemap_space * LbaT::from(idx) + block;
        let block_op = BlockOp::write_spacemap(sglist, lba, idx, block, sender);
        self.new_fut(block_op, receiver)
    }

    /// The asynchronous scatter/gather write function.
    ///
    /// Returns nothing on success, or an error on failure
    ///
    /// # Parameters
    ///
    /// * `bufs`    Scatter-gather list of buffers to receive data
    /// * `lba`     LBA at which to write
    pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> VdevBlockFut
    {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::writev_at(bufs, lba, sender);
        assert_eq!(block_op.len() % BYTES_PER_LBA, 0,
            "VdevBlock does not support fragmentary writes");
        self.new_fut(block_op, receiver)
    }
}

impl Vdev for VdevBlock {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.inner.read().unwrap().leaf.lba2zone(lba)
    }   // LCOV_EXCL_LINE   kcov false negative

    /// Returns the "best" number of operations to queue to this `VdevBlock`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    fn optimum_queue_depth(&self) -> u32 {
        self.inner.read().unwrap().optimum_queue_depth
    }

    fn size(&self) -> LbaT {
        self.size
    }

    /// Asynchronously sync the underlying device, ensuring that all data
    /// reaches stable storage
    fn sync_all(&self) -> BoxVdevFut {
        let (sender, receiver) = oneshot::channel::<()>();
        let block_op = BlockOp::sync_all(sender);
        Box::pin(self.new_fut(block_op, receiver))
    }

    fn uuid(&self) -> Uuid {
        self.inner.read().unwrap().leaf.uuid()
    }   // LCOV_EXCL_LINE   kcov false negative

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.inner.read().unwrap().leaf.zone_limits(zone)
    }   // LCOV_EXCL_LINE   kcov false negative

    fn zones(&self) -> ZoneT {
        self.inner.read().unwrap().leaf.zones()
    }   // LCOV_EXCL_LINE   kcov false negative
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub VdevBlock {
        pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path> + 'static;
        pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn new(leaf: VdevLeaf) -> Self;
        pub fn open<P: AsRef<Path> + 'static>(path: P) -> BoxVdevFut;
        pub fn open_zone(&self, start: LbaT) -> BoxVdevFut;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn write_at(&self, buf: IoVec, lba: LbaT) -> BoxVdevFut;
        pub fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;
        pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
            ->  BoxVdevFut;
        pub fn writev_at(&self, bufs: SGList, lba: LbaT) -> BoxVdevFut;
    }
    impl Vdev for VdevBlock {
        fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;
        fn optimum_queue_depth(&self) -> u32;
        fn size(&self) -> LbaT;
        fn sync_all(&self) -> BoxVdevFut;
        fn uuid(&self) -> Uuid;
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {

use galvanic_test::*;
use super::*;

// pet kcov
#[test]
fn debug_cmd() {
    let dbs = DivBufShared::from(vec![0u8]);
    {
        let read_at = Cmd::ReadAt(dbs.try_mut().unwrap());
        format!("{:?}", read_at);
    }
    {
        let readv_at = Cmd::ReadvAt(vec![dbs.try_mut().unwrap()]);
        format!("{:?}", readv_at);
    }
    {
        let read_spacemap = Cmd::ReadSpacemap(dbs.try_mut().unwrap(), 0);
        format!("{:?}", read_spacemap);
    }
    let write_at = Cmd::WriteAt(dbs.try_const().unwrap());
    let writev_at = Cmd::WritevAt(vec![dbs.try_const().unwrap()]);
    let erase_zone = Cmd::EraseZone(0);
    let finish_zone = Cmd::FinishZone(0);
    let sync_all = Cmd::SyncAll;
    let label_writer = LabelWriter::new(0);
    let write_label = Cmd::WriteLabel(label_writer);
    let write_spacemap = Cmd::WriteSpacemap(vec![dbs.try_const().unwrap()],
                                            0, 0);
    format!("{:?} {:?} {:?} {:?} {:?} {:?} {:?}", write_at, writev_at,
            erase_zone, finish_zone, sync_all, write_label, write_spacemap);
}

// pet kcov
#[test]
fn cmd_partial_cmp() {
    let c0 = Cmd::SyncAll;
    let c1 = Cmd::OpenZone;
    assert_eq!(c0.partial_cmp(&c1), Some(Ordering::Greater));
}

// pet kcov
#[test]
#[allow(clippy::eq_op)]
fn blockop_partial_eq() {
    let (tx0, _rx) = oneshot::channel();
    let (tx1, _rx) = oneshot::channel();
    let (tx2, _rx) = oneshot::channel();
    let bo0 = BlockOp::erase_zone(0, 100, tx0);
    let bo1 = BlockOp::erase_zone(100, 200, tx1);
    let bo2 = BlockOp::finish_zone(100, 200, tx2);
    assert!(bo1 == bo1);
    assert!(bo0 != bo1);
    assert!(bo2 != bo1);
}

test_suite! {
    name t;

    use divbuf::DivBufShared;
    use futures::{
        Future,
        TryFutureExt,
        channel::oneshot,
        future,
        task::{Context, Poll}
    };
    use futures_test::task::noop_context;
    use mockall::*;
    use mockall::predicate::*;
    use mockall::PredicateBooleanExt;
    use pretty_assertions::assert_eq;
    use super::*;

    mock!{
        VdevFut {}
        impl Future for VdevFut {
            type Output = Result<(), Error>;
            fn poll<'a>(self: Pin<&mut Self>, cx: &mut Context<'a>)
                -> Poll<Result<(), Error>>;
        }
    }

    fixture!( mocks() -> MockVdevFile {
            setup(&mut self) {
            let mut leaf = MockVdevFile::new();
            leaf.expect_size()
                .once()
                .return_const(262_144u64);
            leaf.expect_lba2zone()
                .with(ge(1).and(lt(1<<16)))
                .return_const(Some(0));
            leaf.expect_optimum_queue_depth()
                .return_const(10u32);
            leaf.expect_spacemap_space()
                .return_const(1u64);
            leaf.expect_zone_limits()
                .with(eq(0))
                .return_const((1, 1 << 16));
            leaf.expect_zone_limits()
                .with(eq(1))
                .return_const((1 << 16, 2 << 16));
            leaf.expect_zone_limits()
                .with(eq(2))
                .return_const((2 << 16, 3 << 16));
            leaf
        }
    });

    // Issueing an operation fails with EAGAIN.  This can happen if the
    // per-process or per-system AIO limits are reached
    test issueing_eagain(mocks) {
        let mut leaf = mocks.val;
        let mut seq0 = Sequence::new();

        // The first operation succeeds asynchronously.  When it does, that will
        // cause the second to be reissued.
        leaf.expect_read_at()
            .with(always(), eq(1))
            .once()
            .in_sequence(&mut seq0)
            .returning( move |_, _| {
                let mut seq1 = Sequence::new();
                let mut fut = MockVdevFut::new();
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Pending);
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Ready(Ok(())));
                Box::pin(fut)
            });

        leaf.expect_read_at()
            .with(always(), eq(2))
            .once()
            .in_sequence(&mut seq0)
            .returning( move |_, _| {
                let mut seq1 = Sequence::new();
                let mut fut = MockVdevFut::new();
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Ready(Err(Error::EAGAIN)));
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Ready(Ok(())));
                Box::pin(fut)
            });
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            let f0 = vdev.read_at(rbuf0, 1);
            let f1 = vdev.read_at(rbuf1, 2);
            future::try_join(f0, f1).await
        }).unwrap();
    }

    // Issueing an operation fails with EAGAIN, when the queue depth is 1.  This
    // can happen if the per-process or per-system AIO limits are monopolized by
    // other reactors.  In this case, we need a timer to wake us up.
    test issueing_eagain_queue_depth_1(mocks) {
        let mut leaf = mocks.val;
        let mut seq0 = Sequence::new();

        leaf.expect_read_at()
            .with(always(), eq(1))
            .once()
            .in_sequence(&mut seq0)
            .returning(move |_, _| {
                let mut seq1 = Sequence::new();
                let mut fut = MockVdevFut::new();
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Ready(Err(Error::EAGAIN)));
                fut.expect_poll()
                    .once()
                    .in_sequence(&mut seq1)
                    .return_const(Poll::Ready(Ok(())));
                Box::pin(fut)
            });
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let rbuf = dbs.try_mut().unwrap();
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.read_at(rbuf, 1).await
        }).expect("test eagain_queue_depth_1");
    }

    test basic_erase_zone(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_erase_zone()
            .with(eq(1))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));

        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.erase_zone(1, (1 << 16) - 1).await
        }).unwrap();
    }

    test basic_finish_zone(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_finish_zone()
            .with(eq(1))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));

        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.finish_zone(1, (1 << 16) - 1).await
        }).unwrap();
    }

    test basic_open_zone(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_open_zone()
            .with(eq(1))
            .returning(|_| Box::pin(future::ok::<(), Error>(())));

        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.open_zone(1).await
        }).unwrap();
    }

    // basic reading works
    test basic_read_at(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_read_at()
            .with(always(), eq(2))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.read_at(rbuf0, 2).await
        }).unwrap();
    }

    // vectored reading works
    test basic_readv_at(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_readv_at()
            .with(always(), eq(2))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = vec![dbs0.try_mut().unwrap()];
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.readv_at(rbuf0, 2).await
        }).unwrap();
    }

    // sync_all works
    test basic_sync_all(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_sync_all()
            .returning(|| Box::pin(future::ok::<(), Error>(())));

        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.sync_all().await
        }).unwrap();
    }

    // data operations will be issued in C-LOOK order (from lowest LBA to
    // highest, then start over at lowest)
    test sched_data(mocks) {
        let leaf = mocks.val;
        let vdev = VdevBlock::new(leaf);
        let mut inner = vdev.inner.write().unwrap();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy_buffer = dummy_dbs.try_const().unwrap();

        // Start with some intermedia LBA and schedule some ops
        let same_lba = 1000;
        let just_after = 1001;
        let just_after_dup = 1001;
        let just_before = 999;
        let min = 1;
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
            assert_eq!(inner.pop_op().unwrap().lba, 1);
            assert_eq!(inner.pop_op().unwrap().lba, 990);
            assert_eq!(inner.pop_op().unwrap().lba, 999);
            assert_eq!(inner.pop_op().unwrap().lba, 1000);
            assert!(inner.pop_op().is_none());
        });
    }

    // An erase zone command should be scheduled after any reads from that zone
    test sched_erase_zone(mocks) {
        let leaf = mocks.val;
        let vdev = VdevBlock::new(leaf);
        let mut inner = vdev.inner.write().unwrap();
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
        let leaf = mocks.val;
        let vdev = VdevBlock::new(leaf);
        let mut inner = vdev.inner.write().unwrap();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy = dummy_dbs.try_const().unwrap();

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
        let leaf = mocks.val;
        let vdev = VdevBlock::new(leaf);
        let mut inner = vdev.inner.write().unwrap();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy = dummy_dbs.try_const().unwrap();

        inner.last_lba = 1 << 16;   // In zone 1
        // Open zones 0 and 2 and write to both.  Note that it is illegal for
        // the scheduler's last_lba to lie within either of these zones, because
        // that would imply that it had just performed an operation on an empty
        // zone.
        let w = BlockOp::write_at(dummy.clone(), 1, oneshot::channel::<()>().0);
        let write_at_discriminant = mem::discriminant(&w.cmd);
        inner.sched(w);
        inner.sched(BlockOp::write_at(dummy.clone(), (1 << 16) - 1,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), 2,
                    oneshot::channel::<()>().0));
        let oz0 = BlockOp::open_zone(1, oneshot::channel::<()>().0);
        let oz_discriminant = mem::discriminant(&oz0.cmd);
        inner.sched(oz0);
        inner.sched(BlockOp::open_zone(2 << 16, oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), (2 << 16) + 1,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy.clone(), 2 << 16,
                    oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy, (3 << 16) - 1,
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
        assert_eq!(fifth.lba, 1);
        assert_eq!(mem::discriminant(&fifth.cmd), oz_discriminant);
        let sixth = inner.pop_op().unwrap();
        assert_eq!(sixth.lba, 1);
        assert_eq!(mem::discriminant(&sixth.cmd), write_at_discriminant);
        assert_eq!(inner.pop_op().unwrap().lba, 2);
        assert_eq!(inner.pop_op().unwrap().lba, (1 << 16) - 1);
        assert!(inner.pop_op().is_none());
    }

    // A sync_all command should be issued in strictly ordered mode; after all
    // previous commands and before all subsequent commands
    test sched_sync_all(mocks) {
        let leaf = mocks.val;
        let vdev = VdevBlock::new(leaf);
        let mut inner = vdev.inner.write().unwrap();
        let dummy_dbs = DivBufShared::from(vec![0; 4096]);
        let dummy_buffer = dummy_dbs.try_const().unwrap();

        // Start with some intermediate LBA and schedule ops both before and
        // after
        inner.last_lba = 1000;
        inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1001,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy_buffer.clone(), 999,
            oneshot::channel::<()>().0));
        // Now schedule a sync_all, too
        inner.sched(BlockOp::sync_all(oneshot::channel::<()>().0));
        // Now schedule some more data ops both before and after the scheudler
        inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1002,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy_buffer.clone(), 998,
            oneshot::channel::<()>().0));
        // For good measure, schedule a second sync and some more data after
        // that
        inner.sched(BlockOp::sync_all(oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1003,
            oneshot::channel::<()>().0));
        inner.sched(BlockOp::write_at(dummy_buffer, 997,
            oneshot::channel::<()>().0));

        // All pre-sync operations should be issued, then the sync, then the
        // post-sync operations
        assert_eq!(inner.pop_op().unwrap().lba, 1001);
        assert_eq!(inner.pop_op().unwrap().lba, 999);
        assert_eq!(inner.pop_op().unwrap().cmd, Cmd::SyncAll);
        assert_eq!(inner.pop_op().unwrap().lba, 1002);
        assert_eq!(inner.pop_op().unwrap().lba, 998);
        assert_eq!(inner.pop_op().unwrap().cmd, Cmd::SyncAll);
        assert_eq!(inner.pop_op().unwrap().lba, 1003);
        assert_eq!(inner.pop_op().unwrap().lba, 997);
    }

    // Queued operations will both complete
    test issueing_queued(mocks) {
        let mut leaf = mocks.val;
        let mut seq = Sequence::new();

        let (sender, receiver) = oneshot::channel::<()>();
        let e = Error::EPIPE;
        let fut0 = Box::pin(receiver.map_err(move |_| e));
        let fut1 = Box::pin(future::ok::<(), Error>(()));
        leaf.expect_read_at()
            .with(always(), eq(1))
            .once()
            .in_sequence(&mut seq)
            .return_once(move |_, _| fut0);
        leaf.expect_read_at()
            .with(always(), eq(2))
            .once()
            .in_sequence(&mut seq)
            .return_once(move |_, _| {
                sender.send(()).unwrap();
                fut1
            });
        let dbs0 = DivBufShared::from(vec![0u8; 4096]);
        let dbs1 = DivBufShared::from(vec![0u8; 4096]);
        let rbuf0 = dbs0.try_mut().unwrap();
        let rbuf1 = dbs1.try_mut().unwrap();
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            let f0 = vdev.read_at(rbuf0, 1);
            let f1 = vdev.read_at(rbuf1, 2);
            future::try_join(f0, f1).await
        }).unwrap();
    }

    // Operations will be buffered after the max queue depth is reached
    // The first MAX_QUEUE_DEPTH operations will be issued immediately, in the
    // order in which they are requested.  Subsequent operations will be
    // reordered into LBA order
    test issueing_queue_depth(mocks) {
        let mut leaf = mocks.val;
        let num_ops = leaf.optimum_queue_depth() + 2;
        let mut seq = Sequence::new();

        let channels = (0..num_ops - 2).map(|_| oneshot::channel::<()>());
        let (receivers, senders) : (Vec<_>, Vec<_>) = channels.map(|chan| {
            let e = Error::EPIPE;
            (chan.1.map_err(move |_| e), chan.0)
        })
        .unzip();
        for (i, r) in receivers.into_iter().enumerate().rev() {
            leaf.expect_write_at()
                .with(always(), eq(i as LbaT + 1))
                .once()
                .in_sequence(&mut seq)
                .return_once(|_, _| Box::pin(r));
        }
        // Schedule the final two operations in reverse LBA order, but verify
        // that they get issued in actual LBA order
        let final_fut = future::ok::<(), Error>(());
        leaf.expect_write_at()
            .with(always(), eq(LbaT::from(num_ops) - 1))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::pin(final_fut));
        let penultimate_fut = future::ok::<(), Error>(());
        leaf.expect_write_at()
            .with(always(), eq(LbaT::from(num_ops)))
            .once()
            .in_sequence(&mut seq)
            .return_once(|_, _| Box::pin(penultimate_fut));
        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let vdev = VdevBlock::new(leaf);

        basic_runtime().block_on(async {
            let mut ctx = noop_context();
            // First schedule all operations.  There are too many to issue them
            // all immediately
            let early_futs = (1..num_ops - 1).rev().map(|i| {
                let mut fut = Box::pin(
                    vdev.write_at(wbuf.clone(), LbaT::from(i))
                );
                // Manually poll so the VdevBlockFut will get scheduled
                assert!(fut.as_mut().poll(&mut ctx).is_pending());
                fut
            });
            let unbuf_fut = Box::pin(
                future::try_join_all(early_futs)
            );
            let mut penultimate_fut = Box::pin(
                vdev.write_at(wbuf.clone(), LbaT::from(num_ops))
            );
            // Manually poll so the VdevBlockFut will get scheduled
            assert!(penultimate_fut.as_mut().poll(&mut ctx).is_pending());
            let mut final_fut = Box::pin(
                vdev.write_at(wbuf.clone(), LbaT::from(num_ops - 1))
            );
            // Manually poll so the VdevBlockFut will get scheduled
            assert!(final_fut.as_mut().poll(&mut ctx).is_pending());
            let fut = future::try_join3(unbuf_fut, penultimate_fut, final_fut);
            {
                // Verify that they weren't all issued
                let inner = vdev.inner.write().unwrap();
                assert_eq!(inner.ahead.len() + inner.behind.len(), 2);
            }
            // Finally, complete the blocked operations
            for chan in senders {
                chan.send(()).unwrap();
            }
            fut.await
        }).unwrap();
    }

    // Basic writing works
    test basic_write_at(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_write_at()
            .with(always(), eq(1))
            .once()
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = dbs.try_const().unwrap();
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.write_at(wbuf, 1).await
        }).unwrap();
    }

    // vectored writing works
    test basic_writev_at(mocks) {
        let mut leaf = mocks.val;
        leaf.expect_writev_at()
            .with(always(), eq(1))
            .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

        let dbs = DivBufShared::from(vec![0u8; 4096]);
        let wbuf = vec![dbs.try_const().unwrap()];
        let vdev = VdevBlock::new(leaf);
        basic_runtime().block_on(async {
            vdev.writev_at(wbuf, 1).await
        }).unwrap();
    }
}
}
// LCOV_EXCL_STOP
