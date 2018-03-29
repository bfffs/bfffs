// vim: tw=80
use common::*;
use common::vdev::*;

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeaf : Vdev {
    /// Asynchronously erase the given zone.
    ///
    /// After this, the zone will be in the empty state.  The data may or may
    /// not be inaccessible, and should not be considered securely erased.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to erase
    fn erase_zone(&self, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously finish the given zone.
    ///
    /// After this, the zone will be in the Full state and writes will not be
    /// allowed.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to finish
    fn finish_zone(&self, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to open
    fn open_zone(&self, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    fn read_at(&self, buf: IoVecMut, lba: LbaT) -> Box<VdevFut>;

    /// The asynchronous scatter/gather read function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually written.
    fn write_at(&mut self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;

    /// The asynchronous scatter/gather write function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&mut self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
}
