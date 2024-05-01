// vim: tw=80

use futures::{
    Future,
    FutureExt,
    channel::oneshot,
    task::{Context, Poll}
};
#[cfg(not(test))] use futures::{TryFutureExt, future};
use lazy_static::lazy_static;
use nix::unistd::{sysconf, SysconfVar};
use pin_project::pin_project;
use serde_derive::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    collections::VecDeque,
    io,
    mem,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    ops,
    time,
};
#[cfg(not(test))]
use std::collections::BTreeMap;
#[cfg(test)] use mockall::*;

use crate::{
    label::*,
    types::*,
    util::*,
    vdev::*,
    vdev_file::*,
};

#[cfg(test)]
pub type VdevLeaf = MockVdevFile;
#[cfg(not(test))]
pub type VdevLeaf = VdevFile;

lazy_static! {
    static ref IOV_MAX: Option<NonZeroUsize> = {
        sysconf(SysconfVar::IOV_MAX)
            .unwrap()
            .map(usize::try_from)
            .map(std::result::Result::unwrap)
            .map(NonZeroUsize::try_from)
            .map(std::result::Result::unwrap)
    };
}

#[derive(Debug)]
enum Cmd {
    OpenZone,
    ReadAt(IoVecMut),
    ReadSpacemap(IoVecMut),
    ReadvAt(SGListMut),
    WriteAt(IoVec),
    WritevAt(SGList),
    // The extra LBA is the zone's starting LBA
    EraseZone(LbaT),
    // The extra LBA is the zone's starting LBA
    FinishZone(LbaT),
    WriteLabel(LabelWriter),
    WriteSpacemap(SGList),
    SyncAll,
}

impl Cmd {
    #[cfg(test)]
    fn as_readv_at(&self) -> &SGListMut {
        if let Cmd::ReadvAt(sglist_mut) = &self {
            sglist_mut
        } else {
            panic!("Not a Cmd::WriteAt");
        }
    }

    #[cfg(test)]
    fn as_write_spacemap(&self) -> &SGList {
        if let Cmd::WriteSpacemap(sglist) = &self {
            sglist
        } else {
            panic!("Not a Cmd::WriteSpacemap");
        }
    }

    #[cfg(test)]
    fn as_writev_at(&self) -> &SGList {
        if let Cmd::WritevAt(sglist) = &self {
            sglist
        } else {
            panic!("Not a Cmd::WriteAt");
        }
    }

    // Oh, this would be so much easier if only `std::mem::Discriminant`
    // implemented `Ord`!
    fn discriminant(&self) -> i32 {
        match *self {
            Cmd::OpenZone => 0,
            Cmd::ReadAt(_) => 1,
            Cmd::ReadSpacemap(_) => 2,
            Cmd::ReadvAt(_) => 3,
            Cmd::WriteAt(_) => 4,
            Cmd::WritevAt(_) => 5,
            Cmd::EraseZone(_) => 6,
            Cmd::FinishZone(_) => 7,
            Cmd::WriteLabel(_) => 8,
            Cmd::WriteSpacemap(_) => 9,
            Cmd::SyncAll => 10,
        }
    }

    #[cfg(test)]
    fn is_sync_all(&self) -> bool {
        matches!(self, Cmd::SyncAll)
    }
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
    pub senders: Vec<oneshot::Sender<Result<()>>>
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
    pub fn can_accumulate(&self, other: &BlockOp) -> bool {

        // Assuming everything else is ok, can two operations with the given
        // LBA offsets and total byte lengths be accumulated?
        let bytes_ok = |len0: usize, _len1: usize| {
            other.lba == self.lba + (len0 / BYTES_PER_LBA) as LbaT
        };
        // Assuming everything else is ok, can two operations be accumulated
        // without exceeding the operating system's IOV_MAX limitation?
        fn iov_max_ok(len0: usize, len1: usize) -> bool {
            match *IOV_MAX {
                Some(iov_max) => usize::from(iov_max) >= len0 + len1,
                None => true
            }
        }

        match (&self.cmd, &other.cmd) {
            // Adjacent reads may be combined
            (Cmd::ReadAt(iovec0), Cmd::ReadAt(iovec1)) => {
                bytes_ok(iovec0.len(), iovec1.len())
            }
            (Cmd::ReadAt(iovec0), Cmd::ReadvAt(sglist1)) => {
                bytes_ok(iovec0.len(), sglist_len(sglist1)) &&
                    iov_max_ok(1, sglist1.len())
            }
            (Cmd::ReadvAt(sglist0), Cmd::ReadAt(iovec1)) => {
                bytes_ok(sglist_len(sglist0), iovec1.len()) &&
                    iov_max_ok(sglist0.len(), 1)
            }
            (Cmd::ReadvAt(sglist0), Cmd::ReadvAt(sglist1)) => {
                bytes_ok(sglist_len(sglist0), sglist_len(sglist1)) &&
                    iov_max_ok(sglist0.len(), sglist1.len())
            }
            (Cmd::SyncAll, Cmd::SyncAll) => {
                // There's no point to adjacent syncs.  Combine them
                true
            },
            // Adjacent writes may be combined
            (Cmd::WriteAt(iovec0), Cmd::WriteAt(iovec1)) => {
                bytes_ok(iovec0.len(), iovec1.len())
            }
            (Cmd::WriteAt(iovec0), Cmd::WritevAt(sglist1)) => {
                bytes_ok(iovec0.len(), sglist_len(sglist1)) &&
                    iov_max_ok(1, sglist1.len())
            }
            (Cmd::WritevAt(sglist0), Cmd::WriteAt(iovec1)) => {
                bytes_ok(sglist_len(sglist0), iovec1.len()) &&
                    iov_max_ok(sglist0.len(), 1)
            }
            (Cmd::WritevAt(sglist0), Cmd::WritevAt(sglist1)) => {
                bytes_ok(sglist_len(sglist0), sglist_len(sglist1)) &&
                    iov_max_ok(sglist0.len(), sglist1.len())
            }
            _ => {
                // Other pairs of commands never accumulate
                false
            }
        }
    }

    /// Attempt to merge the two BlockOps together.  If successful, the result
    /// will be a single block op that spans the contiguous LBA range of the
    /// originals and includes the buffers of both.  When complete, it will
    /// signal notification to both originals' waiters.
    ///
    /// # Panics
    ///
    /// Panics if the BlckOps cannot be accumulated.
    pub fn accumulate(self, mut other: BlockOp) -> BlockOp {
        debug_assert!(self.can_accumulate(&other));

        debug_assert_eq!(other.senders.len(), 1);
        let mut senders = self.senders;
        senders.push(other.senders.pop().unwrap());

        let cmd = match (self.cmd, other.cmd) {
            (Cmd::SyncAll, Cmd::SyncAll) => {
                // Nothing to do
                Cmd::SyncAll
            },
            (Cmd::ReadAt(iovec0), Cmd::ReadAt(iovec1)) => {
                Cmd::ReadvAt(vec![iovec0, iovec1])
            }
            (Cmd::ReadAt(iovec0), Cmd::ReadvAt(sglist1)) => {
                let mut sglist = Vec::with_capacity(sglist1.len() + 1);
                sglist.push(iovec0);
                sglist.extend(sglist1);
                Cmd::ReadvAt(sglist)
            }
            (Cmd::ReadvAt(mut sglist0), Cmd::ReadAt(iovec1)) => {
                sglist0.push(iovec1);
                Cmd::ReadvAt(sglist0)
            }
            (Cmd::ReadvAt(mut sglist0), Cmd::ReadvAt(sglist1)) => {
                sglist0.extend(sglist1);
                Cmd::ReadvAt(sglist0)
            }
            (Cmd::WriteAt(iovec0), Cmd::WriteAt(iovec1)) => {
                Cmd::WritevAt(vec![iovec0, iovec1])
            }
            (Cmd::WriteAt(iovec0), Cmd::WritevAt(sglist1)) => {
                let mut sglist = Vec::with_capacity(sglist1.len() + 1);
                sglist.push(iovec0);
                sglist.extend(sglist1);
                Cmd::WritevAt(sglist)
            }
            (Cmd::WritevAt(mut sglist0), Cmd::WriteAt(iovec1)) => {
                sglist0.push(iovec1);
                Cmd::WritevAt(sglist0)
            }
            (Cmd::WritevAt(mut sglist0), Cmd::WritevAt(sglist1)) => {
                sglist0.extend(sglist1);
                Cmd::WritevAt(sglist0)
            }
            _ => todo!()
        };

        BlockOp {
            lba: self.lba,
            senders,
            cmd
        }
    }

    pub fn erase_zone(start: LbaT, end: LbaT,
                      sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba: end, cmd: Cmd::EraseZone(start), senders: vec![sender] }
    }

    pub fn finish_zone(start: LbaT, end: LbaT,
                       sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba: end, cmd: Cmd::FinishZone(start), senders: vec![sender] }
    }

    pub fn open_zone(lba: LbaT, sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::OpenZone, senders: vec![sender] }
    }

    pub fn read_at(buf: IoVecMut, lba: LbaT,
                   sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::ReadAt(buf), senders: vec![sender]}
    }

    pub fn read_spacemap(buf: IoVecMut, lba: LbaT,
                         sender: oneshot::Sender<Result<()>>) -> BlockOp
    {
        BlockOp { lba, cmd: Cmd::ReadSpacemap(buf), senders: vec![sender]}
    }

    pub fn readv_at(bufs: SGListMut, lba: LbaT,
                    sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::ReadvAt(bufs), senders: vec![sender]}
    }

    pub fn sync_all(sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba: 0, cmd: Cmd::SyncAll, senders: vec![sender]}
    }

    pub fn write_at(buf: IoVec, lba: LbaT,
                    sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WriteAt(buf), senders: vec![sender]}
    }

    pub fn write_label(labeller: LabelWriter,
                       sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba: 0, cmd: Cmd::WriteLabel(labeller), senders: vec![sender]}
    }

    pub fn write_spacemap(sglist: SGList, lba: LbaT,
                          sender: oneshot::Sender<Result<()>>) -> BlockOp
    {
        BlockOp{
            lba,
            cmd: Cmd::WriteSpacemap(sglist),
            senders: vec![sender]
        }
    }

    pub fn writev_at(bufs: SGList, lba: LbaT,
                     sender: oneshot::Sender<Result<()>>) -> BlockOp {
        BlockOp { lba, cmd: Cmd::WritevAt(bufs), senders: vec![sender]}
    }
}

struct Inner {
    /// A VdevLeaf future that got delayed by an EAGAIN error.  We hold the
    /// future around instead of spawning it into the reactor.
    delayed: Option<(Vec<oneshot::Sender<Result<()>>>, Pin<Box<VdevFut>>)>,

    /// Max commands that will be simultaneously queued to the VdevLeaf
    optimum_queue_depth: u32,

    /// Current queue depth
    queue_depth: u32,

    /// Underlying device
    pub leaf: VdevLeaf,

    /// The last LBA issued an operation
    last_lba: LbaT,

    /// If Some, then we are in the process of removing this VdevBlock from the
    /// pool.  On completion, notify the Sender.
    remover: Option<oneshot::Sender<()>>,

    /// If true, then we are preparing to issue sync_all to the underlying
    /// storage
    syncing: bool,

    // Pending operations are stored in a pair of priority queues.  They _could_
    // be stored in a single queue, _if_ the priority queue's comparison
    // function were allowed to be stateful, as in C++'s STL.  However, Rust's
    // standard library does not have any way to create a priority queue with
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
            let (senders, fut) = if let Some((senders, fut)) = delayed {
                (senders, fut)
            } else if let Some(mut op) = self.pop_op() {
                while self.peek_op()
                    .map(|op2| op.can_accumulate(op2))
                    .unwrap_or(false)
                {
                    op = op.accumulate(self.pop_op().unwrap());
                }
                self.make_fut(op)
            } else {
                // Ran out of pending operations
                break;
            };
            if let Some(d) = self.issue_fut(senders, fut, cx) {
                self.delayed = Some(d);
                if self.queue_depth == 1 {
                    // Can't issue any I/O at all!  This means that other
                    // VdevBlocks or other processes outside of bfffs's control
                    // are using too many disk resources.  In this case, the
                    // only thing we can do is sleep and try again later.
                    tracing::warn!("No disk resources available");
                    let duration = time::Duration::from_millis(10);
                    let schfut = self.reschedule();
                    let delay_fut = tokio::time::sleep(duration)
                    .then(move |_| schfut);
                    tokio::spawn(delay_fut);
                }
                break;
            }
        }
        if self.queue_depth == 0 {
            // All I/O operations are done.  Notify the remover.
            if let Some(remover) = self.remover.take() {
                remover.send(()).unwrap()
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
                 senders: Vec<oneshot::Sender<Result<()>>>,
                 mut fut: Pin<Box<VdevFut>>,
                 cx: &mut Context)
        -> Option<(Vec<oneshot::Sender<Result<()>>>, Pin<Box<VdevFut>>)>
    {

        if self.remover.is_some() {
            // Abort the future immediately
            for sender in senders {
                sender.send(Err(Error::ENXIO)).unwrap();
                self.queue_depth -= 1;
            }
            return None;
        }

        let inner = self.weakself.upgrade().expect(
            "VdevBlock dropped with outstanding I/O");

        // Certain errors, like EAGAIN, happen synchronously.  If the future is
        // going to fail synchronously, then we want to handle the error
        // synchronously.  So we will poll it once before spawning it into the
        // reactor.
        match fut.as_mut().poll(cx) {
            Poll::Ready(Err(Error::EAGAIN)) => {
                // Out of resources to issue this future.  Delay it.
                return Some((senders, fut));
            },
            Poll::Pending => {
                let schfut = self.reschedule();
                tokio::spawn( async move {
                    let r = fut.await;
                    for sender in senders{
                        sender.send(r).unwrap();
                    }
                    inner.write().unwrap().queue_depth -= 1;
                    schfut.await
                });
            },
            Poll::Ready(r) => {
                // This normally doesn't happen, but it can happen on a
                // heavily laden system or one with very fast storage.
                for sender in senders {
                    sender.send(r).unwrap();
                }
                self.queue_depth -= 1;
            }
        }
        None
    }

    /// Create a future from a BlockOp, but don't spawn it yet
    fn make_fut(&mut self, block_op: BlockOp)
        -> (Vec<oneshot::Sender<Result<()>>>, Pin<Box<VdevFut>>) {

        self.queue_depth += 1;
        let lba = block_op.lba;

        // In the context where this is called, we can't return a future.  So we
        // have to spawn it into the event loop manually
        let fut: Pin<Box<VdevFut>> = match block_op.cmd {
            Cmd::WriteAt(iovec) => self.leaf.write_at(iovec, lba),
            Cmd::ReadAt(iovec_mut) => self.leaf.read_at(iovec_mut, lba),
            Cmd::WritevAt(sglist) => self.leaf.writev_at(sglist, lba),
            Cmd::ReadSpacemap(iovec_mut) =>
                    self.leaf.read_spacemap(iovec_mut, lba),
            Cmd::ReadvAt(sglist_mut) => self.leaf.readv_at(sglist_mut, lba),
            Cmd::EraseZone(start) => self.leaf.erase_zone(start, lba),
            Cmd::FinishZone(start) => self.leaf.finish_zone(start),
            Cmd::OpenZone => self.leaf.open_zone(lba),
            Cmd::WriteLabel(labeller) => self.leaf.write_label(labeller),
            Cmd::WriteSpacemap(sglist) =>
                self.leaf.write_spacemap(sglist, lba),
            Cmd::SyncAll => self.leaf.sync_all(),
        };
        (block_op.senders, fut)
    }

    /// Get a reference to the next pending operation, if any
    fn peek_op(&self) -> Option<&BlockOp> {
        if let Some(op) = self.ahead.peek() {
            Some(op)
        } else if let Some(op) = self.behind.peek() {
            Some(op)
        } else if let Some(op) = self.after_sync.front() {
            Some(op)
        } else {
            None
        }
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
            }   // LCOV_EXCL_LINE   grcov false negative
            Some(op)
        } else {
            // Ran out of operations everywhere.  Prepare to idle
            None
        }
    }

    /// Create a future which, when polled, will advance the scheduler,
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

#[pin_project]
/// Return type for most `VdevBlock` asynchronous methods
pub struct VdevBlockFut {
    block_op: Option<BlockOp>,
    inner: Arc<RwLock<Inner>>,
    #[pin]
    receiver: oneshot::Receiver<Result<()>>,
}

impl Future for VdevBlockFut {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.block_op.is_some() {
            let block_op = self.block_op.take().unwrap();
            self.inner.write().unwrap().sched_and_issue(block_op, cx);
        }
        match self.project().receiver.poll(cx) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::EPIPE)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e))
        }
    }
}

#[pin_project]
pub struct RemoveFut {
    vdev: VdevBlock,
    #[pin]
    receiver: oneshot::Receiver<()>,
    sender: Option<oneshot::Sender<()>>
}

impl Future for RemoveFut {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(sender) = self.sender.take() {
            let mut guard = self.vdev.inner.write().unwrap();
            assert!(guard.remover.replace(sender).is_none());
            guard.issue_all(cx);
        }
        match self.project().receiver.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => unreachable!("How did the sender get dropped?"),
        }
    }
}

/// `VdevBlock`: Virtual Device for basic block device
///
/// Wraps a single `VdevLeaf`.  But unlike `VdevLeaf`, `VdevBlock` operations
/// are scheduled.  They may not be issued in the order requested, and they may
/// not be issued immediately.
pub struct VdevBlock {
    inner: Arc<RwLock<Inner>>,

    /// Number of LBAs per zone.  May be simulated.
    lbas_per_zone:  LbaT,

    /// Usable size of the vdev, in LBAs
    size:   LbaT,

    /// Size of a single spacemap as stored in the leaf vdev
    spacemap_space:  LbaT,

    /// Last known path for this Vdev
    path: PathBuf,

    uuid:           Uuid,
}

impl VdevBlock {
    /// Helper function for read and write methods
    fn check_iovec_bounds(&self, lba: LbaT, buf: &[u8]) {
        let buflen = buf.len() as u64;
        let last_lba : LbaT = lba + buflen / (BYTES_PER_LBA as u64);
        assert!(last_lba <= self.size)
    }

    /// Helper function for readv and writev methods
    fn check_sglist_bounds<T>(&self, lba: LbaT, bufs: &[T])
        where T: ops::Deref<Target=[u8]> {

        let len = sglist_len(bufs) as u64;
        let last_lba = lba + len / (BYTES_PER_LBA as u64);
        assert!(last_lba <= self.size)
    }

    /// Create a new VdevBlock from an unused file or device
    ///
    /// * `path`:           A pathname to a file or device
    /// * `lbas_per_zone`:  If specified, this many LBAs will be assigned to
    ///                     simulated zones on devices that don't have native
    ///                     zones.
    pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
        -> io::Result<Self>
        where P: AsRef<Path>
    {
        let leaf = VdevLeaf::create(&path, lbas_per_zone)?;
        Ok(Self::from_leaf(path, leaf))
    }

    /// Create a new VdevBlock from already-opened but unlabeled leaf
    fn from_leaf<P: AsRef<Path>>(path: P, leaf: VdevLeaf) -> Self {
        let uuid = Uuid::new_v4();
        let size = leaf.size();
        let lbas_per_zone = leaf.lbas_per_zone();
        let path = path.as_ref().to_owned();
        VdevBlock::new(leaf, path, uuid, size, lbas_per_zone)
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
        let (sender, receiver) = oneshot::channel::<Result<()>>();
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
        let (sender, receiver) = oneshot::channel::<Result<()>>();
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
               receiver: oneshot::Receiver<Result<()>>) -> VdevBlockFut {
        VdevBlockFut {
            block_op: Some(block_op),
            inner: self.inner.clone(),
            receiver
        }
    }

    /// Asynchronously open a zone on a block device
    ///
    /// # Parameters
    /// - `start`:    The first LBA within the target zone
    pub fn open_zone(&self, start: LbaT) -> VdevBlockFut
    {
        let (sender, receiver) = oneshot::channel::<Result<()>>();
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
    /// * `leaf`:          An already-open underlying VdevLeaf
    /// * `path`:          Path at which this Leaf was last found.
    /// * `uuid`:          XXX Temporary
    /// * `size`:          TODO describe
    /// * `lbas_per_zone`: Number of LBAs per simulated zone
    fn new(
        leaf: VdevLeaf,
        path: PathBuf,
        uuid: Uuid,
        size: LbaT,
        lbas_per_zone: LbaT
    ) -> Self
    {
        let spacemap_space = leaf.spacemap_space();
        let inner = Arc::new(RwLock::new(Inner {
            delayed: None,
            optimum_queue_depth: leaf.optimum_queue_depth(),
            queue_depth: 0,
            leaf,
            last_lba: 0,
            remover: None,
            syncing: false,
            after_sync: VecDeque::new(),
            ahead: BinaryHeap::new(),
            behind: BinaryHeap::new(),
            weakself: Weak::new()
        }));
        inner.write().unwrap().weakself = Arc::downgrade(&inner);
        VdevBlock {
            inner,
            size,
            spacemap_space,
            lbas_per_zone,
            uuid,
            path
        }
    }

    /// The pathname most recently used to open this device.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    #[tracing::instrument(skip(self, buf))]
    pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> VdevBlockFut
    {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let block_op = BlockOp::read_at(buf, lba, sender);
        self.new_fut(block_op, receiver)
    }

    /// Read the entire serialized spacemap.  `idx` selects which spacemap to
    /// read, and should match whichever label is being read concurrently.
    #[tracing::instrument(skip(self, buf))]
    pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> VdevBlockFut
    {
        assert!(LbaT::from(idx) < LABEL_COUNT);
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let lba = LbaT::from(idx) * self.spacemap_space + 2 * LABEL_LBAS;
        let block_op = BlockOp::read_spacemap(buf, lba, sender);
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
    #[tracing::instrument(skip(self, bufs))]
    pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> VdevBlockFut
    {
        self.check_sglist_bounds(lba, &bufs);
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let block_op = BlockOp::readv_at(bufs, lba, sender);
        self.new_fut(block_op, receiver)
    }

    /// Remove this device from the pool.
    ///
    /// This method will cancel all unissued I/O operations and block until all
    /// issued I/O operations have completed.
    // We could theoretically use aio_cancel to cancel issued operations as
    // well.  However, there would be little point as FreeBSD currently doesn't
    // support cancelling operations to raw disk devices, and file-backed vdevs
    // are not intended for production file systems.
    pub fn remove(self) -> impl Future<Output=()> + Send + Sync {
        let (sender, receiver) = oneshot::channel::<()>();
        RemoveFut {
            vdev: self,
            receiver,
            sender: Some(sender)
        }
    }

    pub fn status(&self) -> Status {
        Status {
            health: Health::Online,
            path: self.path.clone(),
            uuid: self.uuid
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns nothing on success, and on error on failure
    pub fn write_at(&self, buf: IoVec, lba: LbaT) -> VdevBlockFut
    {
        self.check_iovec_bounds(lba, &buf);
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let block_op = BlockOp::write_at(buf, lba, sender);
        self.new_fut(block_op, receiver)
    }

    pub fn write_label(&self, mut labeller: LabelWriter) -> VdevBlockFut
    {
        let label = Label {
            uuid: self.uuid,
            spacemap_space: self.spacemap_space,
            lbas_per_zone: self.lbas_per_zone,
            lbas: self.size
        };
        labeller.serialize(&label).unwrap();
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let block_op = BlockOp::write_label(labeller, sender);
        self.new_fut(block_op, receiver)
    }

    pub fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        ->  VdevBlockFut
    {
        assert!(LbaT::from(idx) < LABEL_COUNT);
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let sglist = copy_and_pad_sglist(sglist);
        let lba = block + u64::from(idx) * self.spacemap_space + 2 * LABEL_LBAS;
        let block_op = BlockOp::write_spacemap(sglist, lba, sender);
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
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let sglist = copy_and_pad_sglist(bufs);
        let block_op = BlockOp::writev_at(sglist, lba, sender);
        self.new_fut(block_op, receiver)
    }
}

impl Vdev for VdevBlock {
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT> {
        self.inner.read().unwrap().leaf.lba2zone(lba)
    }

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
        let (sender, receiver) = oneshot::channel::<Result<()>>();
        let block_op = BlockOp::sync_all(sender);
        Box::pin(self.new_fut(block_op, receiver))
    }

    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT) {
        self.inner.read().unwrap().leaf.zone_limits(zone)
    }

    fn zones(&self) -> ZoneT {
        self.inner.read().unwrap().leaf.zones()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    /// Vdev UUID, fixed at format time
    uuid:           Uuid,
    /// Number of LBAs per simulated zone
    lbas_per_zone:  LbaT,
    /// Number of LBAs that were present at format time
    lbas:           LbaT,
    /// LBAs in the first zone reserved for storing each spacemap.
    spacemap_space: LbaT,
}

/// Holds a VdevLeaf that has already been tasted
#[cfg(not(test))]
struct ImportableLeaf {
    leaf: VdevLeaf,
    /// The path at which this leaf was last seen.  It may change.
    path: PathBuf
}

/// Manage BFFFS-formatted disks that aren't yet part of an imported pool.
#[derive(Default)]
pub struct Manager {
    #[cfg(not(test))]
    devices: BTreeMap<Uuid, ImportableLeaf>,
}

impl Manager {
    /// Import a block device that is already known to exist
    #[cfg(not(test))]
    pub fn import(&mut self, uuid: Uuid)
        -> impl Future<Output=Result<(VdevBlock, LabelReader)>>
    {
        future::ready(self.devices.remove(&uuid).ok_or(Error::ENOENT))
            .and_then(move |rec| async move {
                let mut lr = Self::read_label(&rec.leaf.file).await?;
                let label: Label = lr.deserialize().unwrap();
                assert_eq!(uuid, label.uuid);
                let vb = VdevBlock::new(rec.leaf, rec.path, uuid, label.lbas,
                                        label.lbas_per_zone);
                Ok((vb, lr))
            })
    }

    /// Read just one of a vdev's labels
    #[cfg(not(test))]
    async fn read_label(f: &tokio_file::File) -> Result<LabelReader>
    {
        let mut r = Err(Error::EDOOFUS);    // Will get overridden

        for label in 0..2 {
            let lba = LabelReader::lba(label);
            let offset = lba * BYTES_PER_LBA as u64;
            // TODO: figure out how to use mem::MaybeUninit with File::read_at
            let mut rbuf = vec![0; LABEL_SIZE];
            match f.read_at(&mut rbuf[..], offset).unwrap().await {
                Ok(_aio_result) => {
                    match LabelReader::new(rbuf) {
                        Ok(lr) => {
                            r = Ok(lr);
                            break
                        }
                        Err(e) => {
                            // If this is the first label, try the second.
                            r = Err(e);
                        }
                    }
                },
                Err(e) => {
                    r = Err(Error::from(e));
                }
            }
        }
        r
    }

    /// Taste the device identified by `p` for a BFFFS label.
    ///
    /// If present, retain the device in the `Manager` for use as a spare or
    /// for building Pools.
    // TODO: add a method for tasting disks in parallel.
    #[cfg(not(test))]
    pub async fn taste<P: AsRef<Path>>(&mut self, p: P) -> Result<LabelReader> {
        let mut leaf = VdevLeaf::open(&p).await?;
        let path = p.as_ref().to_path_buf();
        let mut lr = Self::read_label(&leaf.file).await?;
        let label: Label = lr.deserialize().unwrap();
        leaf.set(label.lbas, label.lbas_per_zone);
        let importable = ImportableLeaf{leaf, path};
        self.devices.insert(label.uuid, importable);
        Ok(lr)
    }
    #[cfg(test)]
    pub async fn taste<P: AsRef<Path>>(&mut self, _p: P) -> Result<LabelReader>
    {
        unimplemented!()
    }
}

/// Return value of [`VdevBlock::status`]
#[derive(Clone, Debug)]
pub struct Status {
    pub health: Health,
    pub path: PathBuf,
    pub uuid: Uuid
}

// LCOV_EXCL_START
#[cfg(test)]
mock! {
    pub VdevBlock {
        #[mockall::concretize]
        pub fn create<P>(path: P, lbas_per_zone: Option<NonZeroU64>)
            -> io::Result<Self>
            where P: AsRef<Path>;
        pub fn erase_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        pub fn finish_zone(&self, start: LbaT, end: LbaT) -> BoxVdevFut;
        fn new(leaf: VdevLeaf) -> Self;
        pub fn open_zone(&self, start: LbaT) -> BoxVdevFut;
        pub fn path(&self) -> PathBuf;
        pub fn read_at(&self, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;
        pub fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;
        pub fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> BoxVdevFut;
        pub fn status(&self) -> Status;
        pub fn uuid(&self) -> Uuid;
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
        fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
        fn zones(&self) -> ZoneT;
    }
}

#[cfg(test)]
mod t {
    use divbuf::DivBufShared;
    use super::*;

    mod block_op {
        use super::*;

        mod accumulate {
            use super::*;

            /// Can't accumulate because the second operation's LBA lies before
            /// than the first.  This can happen, especially on a lightly-loaded
            /// system, when the seek pointer wraps around.
            #[test]
            fn backwards_lba() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 999;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let rbuf1 = dbs1.try_mut().unwrap();
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::read_at(rbuf1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Can't accumulate because the commands are different
            #[test]
            fn different_cmds() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let wbuf1 = dbs1.try_const().unwrap();
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::write_at(wbuf1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Erase Zone commands never accumulate
            // They could, when working with non-SMR drives, but the zones are
            // so big that there would be little benefit.  With SMR drives they
            // cannot accumulate, because the RESET_WRITE_POINTER operation
            // works on only one zone at a time.
            #[test]
            fn erase_zone() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0s = 1000;
                let lba0e = 1009;
                let lba1s = 1010;
                let lba1e = 1019;
                let op0 = BlockOp::erase_zone(lba0s, lba0e, tx0);
                let op1 = BlockOp::erase_zone(lba1s, lba1e, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Finish Zone commands never accumulate.
            // For non-SMR disks there is no point because the command is a
            // no-op.  For SMR disks they cannot accumulate because the
            // FINISH_ZONE command works on only one zone at a time.
            #[test]
            fn finish_zone() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0s = 1000;
                let lba0e = 1009;
                let lba1s = 1010;
                let lba1e = 1019;
                let op0 = BlockOp::finish_zone(lba0s, lba0e, tx0);
                let op1 = BlockOp::finish_zone(lba1s, lba1e, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Can't accumulate because both operations have the same LBA.
            /// This ideally shouldn't happen, but it's possible, if two
            /// processes try to read the same uncached block at the same time.
            #[test]
            fn identical_lba() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1000;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let rbuf1 = dbs1.try_mut().unwrap();
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::read_at(rbuf1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// read and write operations cannot accumulate if the resulting
            /// length of iovec would exceed _SC_IOV_MAX.
            #[test]
            fn iov_max() {
                let limit = match sysconf(SysconfVar::IOV_MAX).unwrap() {
                    None => {
                        // No such limit
                        return;
                    }
                    Some(limit) => limit
                };
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let dbs = DivBufShared::from(vec![0; 4096]);
                let mut sglist0 = vec![];
                let mut sglist1 = vec![];
                let iovlen0 = limit / 2;
                let iovlen1 = limit + 1 - iovlen0;
                for _ in 0..iovlen0 {
                    sglist0.push(dbs.try_const().unwrap());
                }
                for _ in 0..iovlen1 {
                    sglist1.push(dbs.try_const().unwrap());
                }
                let lba0: LbaT = 1000;
                let lba1: LbaT = lba0 + iovlen0 as LbaT;

                let op0 = BlockOp::writev_at(sglist0, lba0, tx0);
                let op1 = BlockOp::writev_at(sglist1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Can't accumulate because of a gap in the LBA range
            #[test]
            fn lba_gap() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1002;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let rbuf1 = dbs1.try_mut().unwrap();
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::read_at(rbuf1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// OpenZone commands never accumulate
            #[test]
            fn open_zone() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let op0 = BlockOp::open_zone(lba0, tx0);
                let op1 = BlockOp::open_zone(lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Successfully accumulate two ReadAt operations
            #[test]
            fn read_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rptr0 = &rbuf0[0] as *const _;
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let rbuf1 = dbs1.try_mut().unwrap();
                let rptr1 = &rbuf1[0] as *const _;
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::read_at(rbuf1, lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_readv_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][0] as *const _, rptr0);
                assert_eq!(&sglist[1][0] as *const _, rptr1);
            }

            /// Successfully accumulate a ReadAt with a ReadvAt operation
            #[test]
            fn read_at_and_readv_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs2 = DivBufShared::from(vec![0; 4096]);
                let dbs3 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf2 = dbs2.try_mut().unwrap();
                let rbuf3 = dbs3.try_mut().unwrap();
                let rptr0 = &rbuf0[0] as *const _;
                let rptr2 = &rbuf2[0] as *const _;
                let rptr3 = &rbuf3[0] as *const _;
                let op0 = BlockOp::read_at(rbuf0, lba0, tx0);
                let op1 = BlockOp::readv_at(vec![rbuf2, rbuf3], lba1, tx1);

                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_readv_at();
                assert_eq!(sglist.len(), 3);
                assert_eq!(&sglist[0][0] as *const _, rptr0);
                assert_eq!(&sglist[1][0] as *const _, rptr2);
                assert_eq!(&sglist[2][0] as *const _, rptr3);
            }

            /// ReadSpacemap commands never accumulate
            #[test]
            fn read_spacemap() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf1 = dbs1.try_mut().unwrap();
                let op0 = BlockOp::read_spacemap(rbuf0, lba0, tx0);
                let op1 = BlockOp::read_spacemap(rbuf1, lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Successfully accumulate two ReadvAt operations
            #[test]
            fn readv_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1002;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![0; 4096]);
                let dbs2 = DivBufShared::from(vec![0; 4096]);
                let dbs3 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf1 = dbs1.try_mut().unwrap();
                let rbuf2 = dbs2.try_mut().unwrap();
                let rbuf3 = dbs3.try_mut().unwrap();
                let rptr0 = &rbuf0[0] as *const _;
                let rptr1 = &rbuf1[0] as *const _;
                let rptr2 = &rbuf2[0] as *const _;
                let rptr3 = &rbuf3[0] as *const _;
                let op0 = BlockOp::readv_at(vec![rbuf0, rbuf1], lba0, tx0);
                let op1 = BlockOp::readv_at(vec![rbuf2, rbuf3], lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_readv_at();
                assert_eq!(sglist.len(), 4);
                assert_eq!(&sglist[0][0] as *const _, rptr0);
                assert_eq!(&sglist[1][0] as *const _, rptr1);
                assert_eq!(&sglist[2][0] as *const _, rptr2);
                assert_eq!(&sglist[3][0] as *const _, rptr3);
            }

            /// Successfully accumulate a ReadvAt with a ReadAt operation
            #[test]
            fn readv_at_and_read_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1002;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![0; 4096]);
                let dbs2 = DivBufShared::from(vec![0; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf1 = dbs1.try_mut().unwrap();
                let rbuf2 = dbs2.try_mut().unwrap();
                let rptr0 = &rbuf0[0] as *const _;
                let rptr1 = &rbuf1[0] as *const _;
                let rptr2 = &rbuf2[0] as *const _;
                let op0 = BlockOp::readv_at(vec![rbuf0, rbuf1], lba0, tx0);
                let op1 = BlockOp::read_at(rbuf2, lba1, tx1);

                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_readv_at();
                assert_eq!(sglist.len(), 3);
                assert_eq!(&sglist[0][0] as *const _, rptr0);
                assert_eq!(&sglist[1][0] as *const _, rptr1);
                assert_eq!(&sglist[2][0] as *const _, rptr2);
            }

            /// Successfully accumulate two SyncAll operations
            #[test]
            fn sync_all() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let op0 = BlockOp::sync_all(tx0);
                let op1 = BlockOp::sync_all(tx1);
                let opa = op0.accumulate(op1);
                assert!(opa.cmd.is_sync_all());
            }

            /// Successfully accumulate two WriteAt operations
            #[test]
            fn write_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let wbuf1 = dbs1.try_const().unwrap();
                let op0 = BlockOp::write_at(wbuf0, lba0, tx0);
                let op1 = BlockOp::write_at(wbuf1, lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_writev_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..], &dbs0.try_const().unwrap()[..]);
                assert_eq!(&sglist[1][..], &dbs1.try_const().unwrap()[..]);
            }

            /// Successfully accumulate a WriteAt and a WritevAt operation
            #[test]
            fn write_at_and_writev_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs2 = DivBufShared::from(vec![2; 4096]);
                let dbs3 = DivBufShared::from(vec![3; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let wbuf3 = dbs3.try_const().unwrap();
                let sglist1 = vec![wbuf2, wbuf3];
                let op0 = BlockOp::write_at(wbuf0, lba0, tx0);
                let op1 = BlockOp::writev_at(sglist1, lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_writev_at();
                assert_eq!(sglist.len(), 3);
                assert_eq!(&sglist[0][..], &dbs0.try_const().unwrap()[..]);
                assert_eq!(&sglist[1][..], &dbs2.try_const().unwrap()[..]);
                assert_eq!(&sglist[2][..], &dbs3.try_const().unwrap()[..]);
            }

            /// WriteLabel commands never accumulate
            #[test]
            fn write_label() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lw0 = LabelWriter::new(0);
                let lw1 = LabelWriter::new(1);
                let op0 = BlockOp::write_label(lw0, tx0);
                let op1 = BlockOp::write_label(lw1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// WriteSpacemap commands never accumulate
            #[test]
            fn write_spacemap() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1001;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![0; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let op0 = BlockOp::write_spacemap(vec![wbuf0], lba0, tx0);
                let op1 = BlockOp::write_spacemap(vec![wbuf1], lba1, tx1);
                assert!(!op0.can_accumulate(&op1));
            }

            /// Successfully accumulate two WritevAt operations
            #[test]
            fn writev_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1002;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let dbs2 = DivBufShared::from(vec![2; 4096]);
                let dbs3 = DivBufShared::from(vec![3; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let wbuf3 = dbs3.try_const().unwrap();
                let sglist0 = vec![wbuf0, wbuf1];
                let sglist1 = vec![wbuf2, wbuf3];
                let op0 = BlockOp::writev_at(sglist0, lba0, tx0);
                let op1 = BlockOp::writev_at(sglist1, lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_writev_at();
                assert_eq!(sglist.len(), 4);
                assert_eq!(&sglist[0][..], &dbs0.try_const().unwrap()[..]);
                assert_eq!(&sglist[1][..], &dbs1.try_const().unwrap()[..]);
                assert_eq!(&sglist[2][..], &dbs2.try_const().unwrap()[..]);
                assert_eq!(&sglist[3][..], &dbs3.try_const().unwrap()[..]);
            }

            /// Successfully accumulate a WritevAt and a WriteAt operation
            #[test]
            fn writev_at_and_write_at() {
                let (tx0, _rx) = oneshot::channel();
                let (tx1, _rx) = oneshot::channel();
                let lba0 = 1000;
                let lba1 = 1002;
                let dbs0 = DivBufShared::from(vec![0; 4096]);
                let dbs1 = DivBufShared::from(vec![1; 4096]);
                let dbs2 = DivBufShared::from(vec![2; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let sglist0 = vec![wbuf0, wbuf1];
                let op0 = BlockOp::writev_at(sglist0, lba0, tx0);
                let op1 = BlockOp::write_at(wbuf2, lba1, tx1);
                let opa = op0.accumulate(op1);
                assert_eq!(opa.lba, lba0);
                let sglist = opa.cmd.as_writev_at();
                assert_eq!(sglist.len(), 3);
                assert_eq!(&sglist[0][..], &dbs0.try_const().unwrap()[..]);
                assert_eq!(&sglist[1][..], &dbs1.try_const().unwrap()[..]);
                assert_eq!(&sglist[2][..], &dbs2.try_const().unwrap()[..]);
            }
        }

        // pet kcov
        #[test]
        #[allow(clippy::eq_op)]
        fn partial_eq() {
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
    }

    mod cmd {
        use super::*;

        // pet kcov
        #[test]
        fn debug() {
            let dbs = DivBufShared::from(vec![0u8]);
            {
                let read_at = Cmd::ReadAt(dbs.try_mut().unwrap());
                format!("{read_at:?}");
            }
            {
                let readv_at = Cmd::ReadvAt(vec![dbs.try_mut().unwrap()]);
                format!("{readv_at:?}");
            }
            {
                let read_spacemap = Cmd::ReadSpacemap(dbs.try_mut().unwrap());
                format!("{read_spacemap:?}");
            }
            let write_at = Cmd::WriteAt(dbs.try_const().unwrap());
            let writev_at = Cmd::WritevAt(vec![dbs.try_const().unwrap()]);
            let erase_zone = Cmd::EraseZone(0);
            let finish_zone = Cmd::FinishZone(0);
            let sync_all = Cmd::SyncAll;
            let label_writer = LabelWriter::new(0);
            let write_label = Cmd::WriteLabel(label_writer);
            let write_spacemap = Cmd::WriteSpacemap(
                vec![dbs.try_const().unwrap()]);
            format!("{write_at:?} {writev_at:?} {erase_zone:?} {finish_zone:?}\
 {sync_all:?} {write_label:?} {write_spacemap:?}");
        }

        // pet kcov
        #[test]
        fn partial_cmp() {
            let c0 = Cmd::SyncAll;
            let c1 = Cmd::OpenZone;
            assert_eq!(c0.partial_cmp(&c1), Some(Ordering::Greater));
        }
    }

    mod vdev_block {
        use std::pin::pin;
        use divbuf::DivBufShared;
        use futures::{
            TryFutureExt,
            TryStreamExt,
            channel::oneshot,
            future,
            stream::FuturesUnordered,
            task::{Context, Poll}
        };
        use futures_test::task::noop_context;
        use mockall::*;
        use mockall::predicate::*;
        use rstest::{fixture, rstest};
        use super::*;

        fn rxflatten(rx: oneshot::Receiver<Result<()>>) -> BoxVdevFut {
            Box::pin(
                rx.map_err(|_| Error::EPIPE)
                    // TODO: use Result::flatten when that stabilizes
                    // https://github.com/rust-lang/rust/issues/70142
                    .map(|rr| rr?)
            )
        }

        mock!{
            VdevFut {}
            impl Future for VdevFut {
                type Output = Result<()>;
                fn poll<'a>(self: Pin<&mut Self>, cx: &mut Context<'a>)
                    -> Poll<Result<()>>;
            }
        }

        fn partial_leaf() -> MockVdevFile {
            let mut leaf = MockVdevFile::new();
            leaf.expect_size()
                .once()
                .return_const(262_144u64);
            leaf.expect_lba2zone()
                .with(ge(1).and(lt(1<<16)))
                .return_const(Some(0));
            leaf.expect_lbas_per_zone()
                .return_const(1u64 << 16);
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

        #[fixture]
        fn leaf() -> MockVdevFile {
            let mut leaf = partial_leaf();
            leaf.expect_optimum_queue_depth()
                .return_const(10u32);
            leaf
        }

        #[fixture]
        fn leaf1() -> MockVdevFile {
            let mut leaf = partial_leaf();
            leaf.expect_optimum_queue_depth()
                .return_const(1u32);
            leaf
        }

        mod issue_all {
            use super::*;
            use pretty_assertions::assert_eq;

            /// The upper layer creates two operations that can be accumulated.
            /// VdevBlock::issue_all should do that, and only issue one
            /// operation to VdevFile
            #[rstest]
            #[tokio::test]
            async fn accumulate(mut leaf1: MockVdevFile) {
                leaf1.expect_optimum_queue_depth()
                    .return_const(1u32);
                let mut seq = Sequence::new();

                let (tx, rx) = oneshot::channel::<Result<()>>();
                leaf1.expect_read_at()
                    .with(always(), eq(1))
                    .once()
                    .in_sequence(&mut seq)
                    .return_once(|_, _| rxflatten(rx));
                leaf1.expect_readv_at()
                    .withf(|iovec, lba| {
                        *lba == 2 && iovec.len() == 2
                    }).once()
                    .in_sequence(&mut seq)
                    .returning(|_, _| Box::pin(future::ok(())));
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![0u8; 4096]);
                let dbs2 = DivBufShared::from(vec![0u8; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf1 = dbs1.try_mut().unwrap();
                let rbuf2 = dbs2.try_mut().unwrap();
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf1);

                let mut ctx = noop_context();
                // VdevBlock will issue the first operation immediately.
                // But it won't return immediately.
                let mut f0 = pin!(vdev.read_at(rbuf0, 1));
                assert!(f0.as_mut().poll(&mut ctx).is_pending());
                // Since the optimium queue depth is 1, the second two
                // operations will pile up in the queue.  When eventually
                // issued, they'll be accumulated.
                let mut f1 = pin!(vdev.read_at(rbuf1, 2));
                assert!(f1.as_mut().poll(&mut ctx).is_pending());
                let mut f2 = pin!(vdev.read_at(rbuf2, 3));
                assert!(f2.as_mut().poll(&mut ctx).is_pending());
                // Now that all Futures have been polled to get into the
                // scheduler loop, complete the first one.
                tx.send(Ok(())).unwrap();
                future::try_join3(f0, f1, f2).await.unwrap();
            }

            // Issueing an operation fails with EAGAIN.  This can happen if the
            // per-process or per-system AIO limits are reached
            #[rstest]
            #[tokio::test]
            async fn eagain(mut leaf: MockVdevFile) {
                let mut seq0 = Sequence::new();

                // The first operation succeeds asynchronously.  When it does,
                // that will cause the second to be reissued.
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
                    .with(always(), eq(3))
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
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

                let f0 = vdev.read_at(rbuf0, 1);
                let f1 = vdev.read_at(rbuf1, 3);
                future::try_join(f0, f1).await.unwrap();
            }

            // Issueing an operation fails with EAGAIN, when the queue depth is
            // 1.  This can happen if the per-process or per-system AIO limits
            // are monopolized by other reactors.  In this case, we need a timer
            // to wake us up.
            #[rstest]
            #[tokio::test]
            async fn eagain_queue_depth_1(mut leaf: MockVdevFile) {
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
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

                vdev.read_at(rbuf, 1).await.unwrap();
            }

            // Multiple queued operations that cannot be accumulated will both
            // be issued separately and concurrently.
            #[rstest]
            #[tokio::test]
            async fn unaccumulatable(mut leaf: MockVdevFile) {
                let mut seq = Sequence::new();

                let (sender, receiver) = oneshot::channel();
                let fut0 = rxflatten(receiver);
                let fut1 = Box::pin(future::ok::<(), Error>(()));
                leaf.expect_read_at()
                    .with(always(), eq(1))
                    .once()
                    .in_sequence(&mut seq)
                    .return_once(move |_, _| fut0);
                leaf.expect_read_at()
                    .with(always(), eq(3))
                    .once()
                    .in_sequence(&mut seq)
                    .return_once(move |_, _| {
                        sender.send(Ok(())).unwrap();
                        fut1
                    });
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![0u8; 4096]);
                let rbuf0 = dbs0.try_mut().unwrap();
                let rbuf1 = dbs1.try_mut().unwrap();
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

                let f0 = vdev.read_at(rbuf0, 1);
                let f1 = vdev.read_at(rbuf1, 3);
                future::try_join(f0, f1).await.unwrap();
            }

            // Operations will be buffered after the max queue depth is reached
            // The first MAX_QUEUE_DEPTH operations will be issued immediately,
            // in the order in which they are requested.  Subsequent operations
            // will be reordered into LBA order
            #[rstest]
            #[tokio::test]
            async fn queue_depth(mut leaf: MockVdevFile) {
                let num_ops = leaf.optimum_queue_depth() + 2;
                let mut seq = Sequence::new();

                let channels = (0..num_ops - 2)
                    .map(|_| oneshot::channel::<()>());
                let (receivers, senders): (Vec<_>, Vec<_>) = channels
                .map(|chan| {
                    let e = Error::EPIPE;
                    (chan.1.map_err(move |_| e), chan.0)
                })
                .unzip();
                for (i, r) in receivers.into_iter().enumerate().rev() {
                    leaf.expect_write_at()
                        .with(always(), eq((2 * i) as LbaT + 2))
                        .once()
                        .in_sequence(&mut seq)
                        .return_once(|_, _| Box::pin(r));
                }
                // Schedule the final two operations in reverse LBA order, but
                // verify that they get issued in actual LBA order
                let final_fut = future::ok::<(), Error>(());
                leaf.expect_write_at()
                    .with(always(), eq(LbaT::from(2 * num_ops) - 2))
                    .once()
                    .in_sequence(&mut seq)
                    .return_once(|_, _| Box::pin(final_fut));
                let penultimate_fut = future::ok::<(), Error>(());
                leaf.expect_write_at()
                    .with(always(), eq(LbaT::from(2 * num_ops)))
                    .once()
                    .in_sequence(&mut seq)
                    .return_once(|_, _| Box::pin(penultimate_fut));
                let dbs = DivBufShared::from(vec![0u8; 4096]);
                let wbuf = dbs.try_const().unwrap();
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

                let mut ctx = noop_context();
                // First schedule all operations.  There are too many to
                // issue them all immediately
                let futs = (1..num_ops - 1).rev().map(|i| {
                    let mut fut = Box::pin(
                        // Schedule in increasing but nonadjacent LBAs so
                        // they can't be accumulated.
                        vdev.write_at(wbuf.clone(), LbaT::from(i * 2))
                    );
                    // Manually poll so the VdevBlockFut will get scheduled
                    assert!(fut.as_mut().poll(&mut ctx).is_pending());
                    fut
                }).collect::<FuturesUnordered<_>>();
                let mut penultimate_fut = Box::pin(
                    vdev.write_at(wbuf.clone(), LbaT::from(num_ops * 2))
                );
                // Manually poll so the VdevBlockFut will get scheduled
                assert!(penultimate_fut
                        .as_mut()
                        .poll(&mut ctx)
                        .is_pending()
                );
                futs.push(penultimate_fut);
                let mut final_fut = Box::pin(
                    vdev.write_at(wbuf.clone(), LbaT::from(2 * num_ops - 2))
                );
                // Manually poll so the VdevBlockFut will get scheduled
                assert!(final_fut.as_mut().poll(&mut ctx).is_pending());
                futs.push(final_fut);
                {
                    // Verify that they weren't all issued
                    let inner = vdev.inner.write().unwrap();
                    assert_eq!(inner.ahead.len() + inner.behind.len(), 2);
                }
                // Finally, complete the blocked operations
                for chan in senders {
                    chan.send(()).unwrap();
                }
                futs.try_collect::<Vec<_>>().await.unwrap();
            }
        }

        #[rstest]
        #[tokio::test]
        async fn erase_zone(mut leaf: MockVdevFile) {
            let start = 1;
            let end = (1 << 16) - 1;
            leaf.expect_erase_zone()
                .with(eq(start), eq(end))
                .returning(|_,_| Box::pin(future::ok::<(), Error>(())));

            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.erase_zone(start, end).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn finish_zone(mut leaf: MockVdevFile) {
            leaf.expect_finish_zone()
                .with(eq(1))
                .returning(|_| Box::pin(future::ok::<(), Error>(())));

            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.finish_zone(1, (1 << 16) - 1).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn open_zone(mut leaf: MockVdevFile) {
            leaf.expect_open_zone()
                .with(eq(1))
                .returning(|_| Box::pin(future::ok::<(), Error>(())));

            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.open_zone(1).await.unwrap();
        }

        // basic reading works
        #[rstest]
        #[tokio::test]
        async fn read_at(mut leaf: MockVdevFile) {
            leaf.expect_read_at()
                .with(always(), eq(2))
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

            let dbs0 = DivBufShared::from(vec![0u8; 4096]);
            let rbuf0 = dbs0.try_mut().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.read_at(rbuf0, 2).await.unwrap();
        }

        // Fast I/O errors are handled correctly
        #[rstest]
        #[tokio::test]
        async fn read_at_fail_fast(mut leaf: MockVdevFile) {
            leaf.expect_read_at()
                .with(always(), eq(2))
                .returning(|_, _| Box::pin(future::err(Error::EIO)));

            let dbs0 = DivBufShared::from(vec![0u8; 4096]);
            let rbuf0 = dbs0.try_mut().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            assert_eq!(vdev.read_at(rbuf0, 2).await.unwrap_err(), Error::EIO);
        }

        // Delayed I/O errors are handled correctly
        #[rstest]
        #[tokio::test]
        async fn read_at_fail_slow(mut leaf: MockVdevFile) {
            let mut seq = Sequence::new();
            let mut fut = MockVdevFut::new();
            fut.expect_poll()
                .once()
                .in_sequence(&mut seq)
                .return_const(Poll::Pending);
            fut.expect_poll()
                .once()
                .in_sequence(&mut seq)
                .return_const(Poll::Ready(Err(Error::EIO)));
            leaf.expect_read_at()
                .with(always(), eq(2))
                .return_once(move |_, _| Box::pin(fut));

            let dbs0 = DivBufShared::from(vec![0u8; 4096]);
            let rbuf0 = dbs0.try_mut().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            assert_eq!(vdev.read_at(rbuf0, 2).await.unwrap_err(), Error::EIO);
        }

        // vectored reading works
        #[rstest]
        #[tokio::test]
        async fn readv_at(mut leaf: MockVdevFile) {
            leaf.expect_readv_at()
                .with(always(), eq(2))
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

            let dbs0 = DivBufShared::from(vec![0u8; 4096]);
            let rbuf0 = vec![dbs0.try_mut().unwrap()];
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.readv_at(rbuf0, 2).await.unwrap();
        }

        /// Tests for VdevBlock::remove
        mod remove {
            use super::*;

            /// VdevBlock::remove will wait for issued operations to complete
            #[rstest]
            #[tokio::test]
            async fn issued(mut leaf: MockVdevFile) {
                let (tx, rx) = oneshot::channel::<()>();
                leaf.expect_write_at()
                    .with(always(), eq(1))
                    .once()
                    .return_once(|_, _| Box::pin(rx.map_err(|_| Error::EPIPE)));

                let mut ctx = noop_context();
                let dbs = DivBufShared::from(vec![0u8; 4096]);
                let wbuf = dbs.try_const().unwrap();
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

                // The first operation does not return immediately
                let mut f0 = pin!(vdev.write_at(wbuf, 1));
                assert!(f0.as_mut().poll(&mut ctx).is_pending());

                // VdevBlock::remove does not return immediately either
                let mut f1 = pin!(vdev.remove());
                assert!(f1.as_mut().poll(&mut ctx).is_pending());

                // After the write completes, then both futures will return
                tx.send(()).unwrap();
                f0.await.unwrap();
                f1.await;
            }

            /// VdevBlock::remove with no outstanding operations, either issued
            /// or not.
            #[rstest]
            #[tokio::test]
            async fn no_backlog(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                vdev.remove().await;
            }

            /// VdevBlock::remove will cancel any queued but unissued
            /// operations.
            #[rstest]
            #[tokio::test]
            async fn queued(mut leaf1: MockVdevFile) {
                let (tx, rx) = oneshot::channel::<()>();
                leaf1.expect_write_at()
                    .with(always(), eq(1))
                    .once()
                    .return_once(|_, _| Box::pin(rx.map_err(|_| Error::EPIPE)));
                leaf1.expect_write_at()
                    .with(always(), eq(2))
                    .once()
                    .returning(|_, _| {
                        let mut fut = MockVdevFut::new();
                        fut.expect_poll()
                            .never();
                        Box::pin(fut)
                    });

                let mut ctx = noop_context();
                let dbs = DivBufShared::from(vec![0u8; 4096]);
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf1);

                // The first operation does not return immediately
                let mut f0 = pin!(vdev.write_at(dbs.try_const().unwrap(), 1));
                assert!(f0.as_mut().poll(&mut ctx).is_pending());
                // So the second sits in the queue, never issued
                let mut f1 = pin!(vdev.write_at(dbs.try_const().unwrap(), 2));
                assert!(f1.as_mut().poll(&mut ctx).is_pending());

                // VdevBlock::remove does not return immediately either
                let mut f2 = pin!(vdev.remove());
                assert!(f2.as_mut().poll(&mut ctx).is_pending());

                // After the write completes, then all three futures will
                // return.  Note that the second write never got issued to the
                // leaf vdev.
                tx.send(()).unwrap();
                f0.await.unwrap();
                assert_eq!(f1.await, Err(Error::ENXIO));
                f2.await;
            }

            /// VdevBlock::remove will cancel all queued but unissued
            /// operations.
            // Note that the first queued operation is stored in a different
            // place than subsequent ones, so it makes sense to have separate
            // tests for 1 and 2 queued operations.
            #[rstest]
            #[tokio::test]
            async fn queued2(mut leaf1: MockVdevFile) {
                let (tx, rx) = oneshot::channel::<()>();
                leaf1.expect_write_at()
                    .with(always(), eq(1))
                    .once()
                    .return_once(|_, _| Box::pin(rx.map_err(|_| Error::EPIPE)));
                leaf1.expect_write_at()
                    .with(always(), eq(3))
                    .once()
                    .returning(|_, _| {
                        let mut fut = MockVdevFut::new();
                        fut.expect_poll()
                            .never();
                        Box::pin(fut)
                    });
                leaf1.expect_write_at()
                    .with(always(), eq(5))
                    .once()
                    .returning(|_, _| {
                        let mut fut = MockVdevFut::new();
                        fut.expect_poll()
                            .never();
                        Box::pin(fut)
                    });

                let mut ctx = noop_context();
                let dbs = DivBufShared::from(vec![0u8; 4096]);
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf1);

                // The first operation does not return immediately
                let mut f0 = pin!(vdev.write_at(dbs.try_const().unwrap(), 1));
                assert!(f0.as_mut().poll(&mut ctx).is_pending());
                // So the others sit in the queue, never issued
                let mut f1 = pin!(vdev.write_at(dbs.try_const().unwrap(), 3));
                assert!(f1.as_mut().poll(&mut ctx).is_pending());
                let mut f2 = pin!(vdev.write_at(dbs.try_const().unwrap(), 5));
                assert!(f2.as_mut().poll(&mut ctx).is_pending());

                // VdevBlock::remove does not return immediately either
                let mut f3 = pin!(vdev.remove());
                assert!(f3.as_mut().poll(&mut ctx).is_pending());

                // After the write completes, then all three futures will
                // return.  Note that the second write never got issued to the
                // leaf vdev.
                tx.send(()).unwrap();
                f0.await.unwrap();
                assert_eq!(f1.await, Err(Error::ENXIO));
                assert_eq!(f2.await, Err(Error::ENXIO));
                f3.await;
            }

        }

        /// Tests for Inner::sched
        mod sched {
            use super::*;
            use pretty_assertions::assert_eq;

            // data operations will be issued in C-LOOK order (from lowest LBA
            // to highest, then start over at lowest)
            #[rstest]
            fn data(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
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
                let mut lbas = vec![same_lba, just_after, just_after_dup,
                                    just_before, min, max];
                // Test scheduling the ops in all possible permutations
                permutohedron::heap_recursive(&mut lbas, |permutation| {
                    inner.last_lba = 1000;
                    for lba in permutation {
                        let op = BlockOp::write_at(dummy_buffer.clone(), *lba,
                            oneshot::channel::<Result<()>>().0);
                        inner.sched(op);
                    }

                    // Check that they're scheduled in the correct order
                    assert_eq!(inner.pop_op().unwrap().lba, 1000);
                    assert_eq!(inner.pop_op().unwrap().lba, 1001);
                    assert_eq!(inner.pop_op().unwrap().lba, 1001);

                    // Schedule two more operations behind the scheduler, but
                    // ahead of some already-scheduled ops, to make sure they
                    // get issued in the right order
                    let just_before2 = BlockOp::write_at(dummy_buffer.clone(),
                        1000,
                        oneshot::channel::<Result<()>>().0);
                    let well_before = BlockOp::write_at(dummy_buffer.clone(),
                        990,
                        oneshot::channel::<Result<()>>().0);
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

            // An erase zone command should be scheduled after any reads from
            // that zone
            #[rstest]
            fn erase_zone(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let mut inner = vdev.inner.write().unwrap();
                let dummy_dbs = DivBufShared::from(vec![0; 12288]);
                let mut dummy = dummy_dbs.try_mut().unwrap();

                inner.last_lba = 1 << 16;   // In zone 1
                // Read from zones that lie behind, around, and ahead of the
                // scheduler, then erase them.  This simulates garbage
                // collection.
                let ez0 = BlockOp::erase_zone(0, (1 << 16) - 1,
                    oneshot::channel::<Result<()>>().0);
                let ez_discriminant = mem::discriminant(&ez0.cmd);
                inner.sched(ez0);
                let r = BlockOp::read_at(dummy.split_to(4096), (1 << 16) - 1,
                    oneshot::channel::<Result<()>>().0);
                let read_at_discriminant = mem::discriminant(&r.cmd);
                inner.sched(r);
                inner.sched(BlockOp::erase_zone(1 << 16, (2 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::read_at(dummy.split_to(4096),
                    (2 << 16) - 1, oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::erase_zone(2 << 16, (3 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::read_at(dummy, (3 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));

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

            // A finish zone command should be scheduled after any writes to
            // that zone
            #[rstest]
            fn finish_zone(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let mut inner = vdev.inner.write().unwrap();
                let dummy_dbs = DivBufShared::from(vec![0; 4096]);
                let dummy = dummy_dbs.try_const().unwrap();

                inner.last_lba = 1 << 16;   // In zone 1
                // Write to zones that lie behind, around, and ahead of the
                // scheduler, then finish them.
                let fz0 = BlockOp::finish_zone(0, (1 << 16) - 1,
                    oneshot::channel::<Result<()>>().0);
                let fz_discriminant = mem::discriminant(&fz0.cmd);
                inner.sched(fz0);
                let r = BlockOp::write_at(dummy.clone(), (1 << 16) - 1,
                    oneshot::channel::<Result<()>>().0);
                let write_at_discriminant = mem::discriminant(&r.cmd);
                inner.sched(r);
                inner.sched(BlockOp::finish_zone(1 << 16, (2 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy.clone(), (2 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::finish_zone(2 << 16, (3 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy, (3 << 16) - 1,
                    oneshot::channel::<Result<()>>().0));

                let first = inner.pop_op().unwrap();
                assert_eq!(first.lba, (2 << 16) - 1);
                assert_eq!(mem::discriminant(&first.cmd),
                           write_at_discriminant);
                let second = inner.pop_op().unwrap();
                assert_eq!(second.lba, (2 << 16) - 1);
                assert_eq!(mem::discriminant(&second.cmd), fz_discriminant);
                let third = inner.pop_op().unwrap();
                assert_eq!(third.lba, (3 << 16) - 1);
                assert_eq!(mem::discriminant(&third.cmd),
                           write_at_discriminant);
                let fourth = inner.pop_op().unwrap();
                assert_eq!(fourth.lba, (3 << 16) - 1);
                assert_eq!(mem::discriminant(&fourth.cmd), fz_discriminant);
                let fifth = inner.pop_op().unwrap();
                assert_eq!(fifth.lba, (1 << 16) - 1);
                assert_eq!(mem::discriminant(&fifth.cmd),
                           write_at_discriminant);
                let sixth = inner.pop_op().unwrap();
                assert_eq!(sixth.lba, (1 << 16) - 1);
                assert_eq!(mem::discriminant(&sixth.cmd), fz_discriminant);
            }

            // An open zone command should be scheduled before any writes to
            // that zone
            #[rstest]
            fn open_zone(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let mut inner = vdev.inner.write().unwrap();
                let dummy_dbs = DivBufShared::from(vec![0; 4096]);
                let dummy = dummy_dbs.try_const().unwrap();

                inner.last_lba = 1 << 16;   // In zone 1
                // Open zones 0 and 2 and write to both.  Note that it is
                // illegal for the scheduler's last_lba to lie within either of
                // these zones, because that would imply that it had just
                // performed an operation on an empty zone.
                let w = BlockOp::write_at(dummy.clone(), 1,
                    oneshot::channel::<Result<()>>().0);
                let write_at_discriminant = mem::discriminant(&w.cmd);
                inner.sched(w);
                inner.sched(BlockOp::write_at(dummy.clone(), (1 << 16) - 1,
                            oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy.clone(), 2,
                            oneshot::channel::<Result<()>>().0));
                let oz0 = BlockOp::open_zone(1, oneshot::channel::<Result<()>>().0);
                let oz_discriminant = mem::discriminant(&oz0.cmd);
                inner.sched(oz0);
                inner.sched(BlockOp::open_zone(2 << 16,
                                               oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy.clone(), (2 << 16) + 1,
                            oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy.clone(), 2 << 16,
                            oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy, (3 << 16) - 1,
                            oneshot::channel::<Result<()>>().0));

                let first = inner.pop_op().unwrap();
                assert_eq!(first.lba, 2 << 16);
                assert_eq!(mem::discriminant(&first.cmd), oz_discriminant);
                let second = inner.pop_op().unwrap();
                assert_eq!(second.lba, 2 << 16);
                assert_eq!(mem::discriminant(&second.cmd),
                           write_at_discriminant);
                assert_eq!(inner.pop_op().unwrap().lba, (2 << 16) + 1);
                assert_eq!(inner.pop_op().unwrap().lba, (3 << 16) - 1);
                let fifth = inner.pop_op().unwrap();
                assert_eq!(fifth.lba, 1);
                assert_eq!(mem::discriminant(&fifth.cmd), oz_discriminant);
                let sixth = inner.pop_op().unwrap();
                assert_eq!(sixth.lba, 1);
                assert_eq!(mem::discriminant(&sixth.cmd),
                           write_at_discriminant);
                assert_eq!(inner.pop_op().unwrap().lba, 2);
                assert_eq!(inner.pop_op().unwrap().lba, (1 << 16) - 1);
                assert!(inner.pop_op().is_none());
            }

            // A sync_all command should be issued in strictly ordered mode;
            // after all previous commands and before all subsequent commands
            #[rstest]
            fn sync_all(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let mut inner = vdev.inner.write().unwrap();
                let dummy_dbs = DivBufShared::from(vec![0; 4096]);
                let dummy_buffer = dummy_dbs.try_const().unwrap();

                // Start with some intermediate LBA and schedule ops both before
                // and after
                inner.last_lba = 1000;
                inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1001,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy_buffer.clone(), 999,
                    oneshot::channel::<Result<()>>().0));
                // Now schedule a sync_all, too
                inner.sched(BlockOp::sync_all(oneshot::channel::<Result<()>>().0));
                // Now schedule some more data ops both before and after the
                // scheudler
                inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1002,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy_buffer.clone(), 998,
                    oneshot::channel::<Result<()>>().0));
                // For good measure, schedule a second sync and some more data
                // after that
                inner.sched(BlockOp::sync_all(oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy_buffer.clone(), 1003,
                    oneshot::channel::<Result<()>>().0));
                inner.sched(BlockOp::write_at(dummy_buffer, 997,
                    oneshot::channel::<Result<()>>().0));

                // All pre-sync operations should be issued, then the sync, then
                // the post-sync operations
                assert_eq!(inner.pop_op().unwrap().lba, 1001);
                assert_eq!(inner.pop_op().unwrap().lba, 999);
                assert_eq!(inner.pop_op().unwrap().cmd, Cmd::SyncAll);
                assert_eq!(inner.pop_op().unwrap().lba, 1002);
                assert_eq!(inner.pop_op().unwrap().lba, 998);
                assert_eq!(inner.pop_op().unwrap().cmd, Cmd::SyncAll);
                assert_eq!(inner.pop_op().unwrap().lba, 1003);
                assert_eq!(inner.pop_op().unwrap().lba, 997);
            }
        }

        // sync_all works
        #[rstest]
        #[tokio::test]
        async fn sync_all(mut leaf: MockVdevFile) {
            leaf.expect_sync_all()
                .returning(|| Box::pin(future::ok::<(), Error>(())));

            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.sync_all().await.unwrap();
        }

        // Basic writing works
        #[rstest]
        #[tokio::test]
        async fn write_at(mut leaf: MockVdevFile) {
            leaf.expect_write_at()
                .with(always(), eq(1))
                .once()
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.write_at(wbuf, 1).await.unwrap();
        }

        // Attempting to write after device removal should fail appropriately
        #[rstest]
        #[tokio::test]
        async fn write_at_fails_early(mut leaf: MockVdevFile) {
            leaf.expect_write_at()
                .with(always(), eq(1))
                .once()
                .returning(|_, _| Box::pin(future::err::<(), Error>(Error::ENXIO)));

            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            assert_eq!(Err(Error::ENXIO), vdev.write_at(wbuf, 1).await);
        }

        // Attempting to write after device removal should fail appropriately
        #[rstest]
        #[tokio::test]
        async fn write_at_fails_late(mut leaf: MockVdevFile) {
            leaf.expect_write_at()
                .with(always(), eq(1))
                .once()
                .returning(|_, _| {
                    let mut seq1 = Sequence::new();
                    let mut fut = MockVdevFut::new();
                    fut.expect_poll()
                        .once()
                        .in_sequence(&mut seq1)
                        .return_const(Poll::Pending);
                    fut.expect_poll()
                        .once()
                        .in_sequence(&mut seq1)
                        .return_const(Poll::Ready(Err(Error::ENXIO)));
                    Box::pin(fut)
                });

            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let wbuf = dbs.try_const().unwrap();
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            assert_eq!(Err(Error::ENXIO), vdev.write_at(wbuf, 1).await);
        }

        // vectored writing works
        #[rstest]
        #[tokio::test]
        async fn writev_at(mut leaf: MockVdevFile) {
            leaf.expect_writev_at()
                .with(always(), eq(1))
                .returning(|_, _| Box::pin(future::ok::<(), Error>(())));

            let dbs = DivBufShared::from(vec![0u8; 4096]);
            let wbuf = vec![dbs.try_const().unwrap()];
            let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);

            vdev.writev_at(wbuf, 1).await.unwrap();
        }

        mod write_spacemap {
            use super::*;

            /// Just like VdevBlock::writev_at, VdevBlock::write_spacemap will
            /// ensure that all iovecs are a multiple of blocksize in size.
            #[allow(clippy::async_yields_async)]
            #[rstest]
            #[tokio::test]
            async fn pad_small_tail(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
                let fut = vdev.write_spacemap(wbufs, 0, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_write_spacemap();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..], &wbuf0[..]);
                assert_eq!(&sglist[1][..3072], &wbuf1[..]);
                let zbuf = ZERO_REGION.try_const().unwrap();
                assert_eq!(&sglist[1][3072..], &zbuf[..1024]);
            }
        }

        mod writev_at {
            use super::*;

            /// VdevBlock::writev_at will copy two small iovecs into a new
            /// buffer that is a multiple of a blocksize.
            #[rstest]
            #[tokio::test]
            async fn copy_two_iovecs(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 1024]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 1);
                assert_eq!(&sglist[0][..1024], &wbuf0[..]);
                assert_eq!(&sglist[0][1024..], &wbuf1[..]);
            }

            /// VdevBlock::writev_at will copy three small iovecs into a new
            /// buffer that is a multiple of a blocksize.
            #[rstest]
            #[tokio::test]
            async fn copy_three_iovecs(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 1024]);
                let dbs1 = DivBufShared::from(vec![1u8; 2050]);
                let dbs2 = DivBufShared::from(vec![2u8; 1022]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone(), wbuf2.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 1);
                assert_eq!(&sglist[0][..1024], &wbuf0[..]);
                assert_eq!(&sglist[0][1024..3074], &wbuf1[..]);
                assert_eq!(&sglist[0][3074..], &wbuf2[..]);
            }

            /// VdevBlock::writev_at will copy no more iovecs than is neccessary
            /// into a new blocksize-multiple iovec.
            #[rstest]
            #[tokio::test]
            async fn copy_first_two_iovecs(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 1024]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let dbs2 = DivBufShared::from(vec![2u8; 4096]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let wptr2 = &wbuf2[0] as *const _;
                let wbufs = vec![wbuf0.clone(), wbuf1.clone(), wbuf2.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..1024], &wbuf0[..]);
                assert_eq!(&sglist[0][1024..], &wbuf1[..]);
                assert_eq!(&sglist[1][..], &wbuf2[..]);
                assert_eq!(wptr2, &sglist[1][0]);
            }

            /// VdevBlock::writev_at will copy no more iovecs than is neccessary
            /// into a new blocksize-multiple iovec.
            #[rstest]
            #[tokio::test]
            async fn copy_last_two_iovecs(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let dbs2 = DivBufShared::from(vec![2u8; 1024]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wptr0 = &wbuf0[0] as *const _;
                let wbuf1 = dbs1.try_const().unwrap();
                let wbuf2 = dbs2.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone(), wbuf2.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..], &wbuf0[..]);
                assert_eq!(wptr0, &sglist[0][0]);
                assert_eq!(&sglist[1][..3072], &wbuf1[..]);
                assert_eq!(&sglist[1][3072..], &wbuf2[..]);
            }

            /// VdevBlock::writev_at will copy the tail of a long iovec, if
            /// necessary, but not copy the entire thing.
            #[rstest]
            #[tokio::test]
            async fn copy_tail_of_iovec(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 5120]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wptr0 = &wbuf0[0] as *const _;
                let wbuf1 = dbs1.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..], &wbuf0[..4096]);
                assert_eq!(wptr0, &sglist[0][0]);
                assert_eq!(&sglist[1][..1024], &wbuf0[4096..]);
                assert_eq!(&sglist[1][1024..], &wbuf1[..]);
            }

            /// VdevBlock::writev_at will zero-pad an operation's tail, if need
            /// be, up to a multiple of an LBA.  But it won't copy more data
            /// than is necessary, for iovecs > 1 LBA.
            #[rstest]
            #[tokio::test]
            async fn pad_large_tail(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![1u8; 7168]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wptr1 = &wbuf1[0] as *const _;
                let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 3);
                assert_eq!(&sglist[0][..], &wbuf0[..]);
                assert_eq!(&sglist[1][..], &wbuf1[..4096]);
                assert_eq!(wptr1, &sglist[1][0]);
                let zbuf = ZERO_REGION.try_const().unwrap();
                assert_eq!(&sglist[2][..3072], &wbuf1[4096..]);
                assert_eq!(&sglist[2][3072..], &zbuf[..1024]);
            }

            /// VdevBlock::writev_at will zero-pad an operation's tail, if need
            /// be, up to a multiple of an LBA.
            #[allow(clippy::async_yields_async)]
            #[rstest]
            #[tokio::test]
            async fn pad_small_tail(leaf: MockVdevFile) {
                let vdev = VdevBlock::from_leaf("/dev/whatever", leaf);
                let dbs0 = DivBufShared::from(vec![0u8; 4096]);
                let dbs1 = DivBufShared::from(vec![1u8; 3072]);
                let wbuf0 = dbs0.try_const().unwrap();
                let wbuf1 = dbs1.try_const().unwrap();
                let wbufs = vec![wbuf0.clone(), wbuf1.clone()];
                let fut = vdev.writev_at(wbufs, 10);
                let op = fut.block_op.unwrap();
                let sglist = op.cmd.as_writev_at();
                assert_eq!(sglist.len(), 2);
                assert_eq!(&sglist[0][..], &wbuf0[..]);
                assert_eq!(&sglist[1][..3072], &wbuf1[..]);
                let zbuf = ZERO_REGION.try_const().unwrap();
                assert_eq!(&sglist[1][3072..], &zbuf[..1024]);
            }
        }
    }
}
// LCOV_EXCL_STOP
