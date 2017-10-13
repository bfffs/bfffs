// vim: tw=80

use common::*;
use futures;
use std::io;
use tokio_core::reactor::Handle;

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
pub trait Vdev {
    /// Return the Tokio reactor handle used for this vdev
    fn handle(&self) -> Handle;

    /// Return the zone number at which the given LBA resides
    fn lba2zone(&self, lba: LbaT) -> ZoneT;

    /// Asynchronously read a contiguous portion of the vdev
    fn read_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;

    /// Return the usable space of the Vdev.
    ///
    /// Does not include wasted space, space reserved for labels, space used by
    /// partity, etc.  May not change within the lifetime of a Vdev.
    fn size(&self) -> LbaT;

    /// Return the first LBA of the given zone
    fn start_of_zone(&self, zone: ZoneT) -> LbaT;

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
