// vim: tw=80

use common::*;
use common::zoned_device::*;
use futures;
use mio;
use nix;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::io;
use std::rc::Rc;
use tokio_core::reactor::{Handle, PollEvented};

#[derive(Eq, PartialEq)]
pub enum BlockOpBufT {
    IoVec(IoVec),
    SGList(SGList)
}

/// A single read or write command that is queued at the VdevBlock layer
#[derive(Eq)]
pub struct BlockOp {
    lba: LbaT,
    bufs: BlockOpBufT
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
    pub fn read_at(buf: IoVec, lba: LbaT) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::IoVec(buf)}
    }

    pub fn readv_at(bufs: SGList, lba: LbaT) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::SGList(bufs)}
    }

    pub fn write_at(buf: IoVec, lba: LbaT) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::IoVec(buf)}
    }

    pub fn writev_at(bufs: SGList, lba: LbaT) -> BlockOp {
        BlockOp { lba: lba, bufs: BlockOpBufT::SGList(bufs)}
    }
}

/// mio implementation detail
#[doc(hidden)]
struct VdevFutEvented {
    /// Used by `mio::Evented`
    registration: mio::Registration,

    /// Used by the `VdevLeaf` to complete this future
    promise: mio::SetReadiness
}

impl mio::Evented for VdevFutEvented {
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

/// Future representing an operation on a vdev.  The return type is the amount
/// of data that was actually read/written, or an errno on error
#[must_use = "futures do nothing unless polled"]
pub struct VdevFut {
    /// Link to the target `Vdev` that will complete this future
    vdev: Rc<Vdev>,

    /// The associated `BlockOp`.  Whether it's a read or write will be clear
    /// from which `ZoneQueue` it's stored in.
    block_op: BlockOp,

    /// Has this I/O already been scheduled?
    scheduled: bool,

    /// Is this a read or a write operation?
    write: bool,

    // Used by the mio stuff
    io: PollEvented<VdevFutEvented>
}

impl VdevFut {
    pub fn new(vdev: Rc<Vdev>,
               block_op: BlockOp,
               write:bool) -> VdevFut {
        let (registration, promise) = mio::Registration::new2();
        let io = VdevFutEvented {registration: registration,
                                 promise: promise};
        let handle = vdev.handle().clone();
        VdevFut {vdev: vdev,
                 block_op: block_op,
                 scheduled: false,
                 write: write,
                 io: PollEvented::new(io, &handle).unwrap()}
    }
}

impl futures::Future for VdevFut {
    type Item = isize;
    type Error = nix::Error;    // TODO: define our own error type?

    fn poll(&mut self) -> futures::Poll<isize, nix::Error> {
        if ! self.scheduled {
            match self.block_op.bufs {
                BlockOpBufT::IoVec(ref iovec) => {
                    if self.write {
                    } else {
                        self.vdev.read_at(iovec.clone(), self.block_op.lba);
                    }
                },
                BlockOpBufT::SGList(ref sglist) => {
                    if self.write {
                    } else {
                        //self.vdev.readv_at(sglist ,self.block_op.lba);
                    }
                }
            }
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
/// Vdev: Virtual Device
///
/// This is directly analogous to ZFS Vdevs.  A vdev is a virtual block device
/// built upon one or more virtual or real devices.  Unlike early version of
/// ZFS, vdevs may not be stacked arbitrarily.  Not all Vdevs have the same data
/// plane API.  These methods here are the methods that are common to all Vdev
/// types.
pub trait Vdev : ZonedDevice {
    /// Return the Tokio reactor handle used for this vdev
    fn handle(&self) -> Handle;

    /// Asynchronously read a contiguous portion of the vdev
    fn read_at(&self, buf: IoVec, lba: LbaT) -> VdevFut;

    /// Asynchronously write a contiguous portion of the vdev
    fn write_at(&self, buf: IoVec, lba: LbaT) -> VdevFut;
}

/// Scatter-Gather Vdev
///
/// These Vdevs support scatter-gather operations analogous to preadv/pwritev.
pub trait SGVdev : Vdev {
    /// The asynchronous scatter/gather read function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> VdevFut;

    /// The asynchronous scatter/gather write function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&self, bufs: SGList, lba: LbaT) -> VdevFut;
}
