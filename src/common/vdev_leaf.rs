// vim: tw=80
use common::*;
use common::vdev::*;

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeaf : Vdev {
    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<IoVecFut>;

    /// The asynchronous scatter/gather read function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<SGListFut>;

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually written.
    fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<IoVecFut>;

    /// The asynchronous scatter/gather write function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<SGListFut>;
}
