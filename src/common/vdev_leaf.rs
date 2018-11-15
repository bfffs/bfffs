// vim: tw=80
use crate::common::{*, label::*, vdev::*};

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
pub trait VdevLeafApi : Vdev {
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

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// - `idx`:        Index of the spacemap to read.  It should be the same as
    ///                 whichever label is being used.
    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> Box<VdevFut>;

    /// The asynchronous scatter/gather read function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn readv_at(&self, bufs: SGListMut, lba: LbaT) -> Box<VdevFut>;

    /// Size of a single serialized spacemap, in LBAs, rounded up.
    fn spacemap_space(&self) -> LbaT;

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually written.
    fn write_at(&self, buf: IoVec, lba: LbaT) -> Box<VdevFut>;

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    fn write_label(&self, label_writer: LabelWriter) -> Box<VdevFut>;

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `buf`:        Buffer of data to write
    /// - `idx`:        Index of the spacemap area to write: there are more than
    ///                 one.  It should be the same as whichever label is being
    ///                 written.
    /// - `block`:      LBA-based offset from the start of the spacemap area
    fn write_spacemap(&self, buf: IoVec, idx: u32, block: LbaT) -> Box<VdevFut>;

    /// The asynchronous scatter/gather write function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    fn writev_at(&self, bufs: SGList, lba: LbaT) -> Box<VdevFut>;
}
