/// vim: tw=80

use common::*;
use common::zoned_device::*;
use common::block_fut::*;

/// Vdev: Virtual Device
///
/// This is directly analogous to ZFS Vdevs.  A vdev is a virtual block device
/// built upon one or more virtual or real devices.  Unlike early version of
/// ZFS, vdevs may not be stacked arbitrarily.  Not all Vdevs have the same data
/// plane API.  These methods here are the methods that are common to all Vdev
/// types.
pub trait Vdev : ZonedDevice {
    /// Asynchronously read a contiguous portion of the vdev
    fn read_at(&self, buf: IoVec, lba: LbaT) -> BlockFut;

    /// Asynchronously write a contiguous portion of the vdev
    fn write_at(&self, buf: IoVec, lba: LbaT) -> BlockFut;
}

/// Scatter-Gather Vdev
///
/// These Vdevs support scatter-gather operations analogous to preadv/pwritev.
pub trait SGVdev : Vdev {
    /// The asynchronous scatter/gather read function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGList, lba: LbaT) -> BlockFut;

    /// The asynchronous scatter/gather write function.
    /// 
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&self, bufs: SGList, lba: LbaT) -> BlockFut;
}
