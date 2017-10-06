// vim: tw=80

use common::*;
use common::zoned_device::*;
use futures;
use nix;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::io;
use std::rc::Rc;
use tokio_core::reactor::Handle;

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

/// Future representing an operation on a vdev.  The return type is the amount
/// of data that was actually read/written, or an errno on error
pub type VdevFut = futures::Future<Item = isize, Error = io::Error>;

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
    fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously write a contiguous portion of the vdev
    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;
}

/// Scatter-Gather Vdev
///
/// These Vdevs support scatter-gather operations analogous to preadv/pwritev.
pub trait SGVdev : Vdev {
    /// The asynchronous scatter/gather read function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;

    /// The asynchronous scatter/gather write function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
}
