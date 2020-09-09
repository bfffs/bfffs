// vim: tw=80
use async_trait::async_trait;
use crate::common::{*, label::*, vdev::*};

/// The public interface for all leaf Vdevs.  This is a low level thing.  Leaf
/// vdevs are typically files or disks, and this trait is their minimum common
/// interface.  I/O operations on `VdevLeaf` happen immediately; they are not
/// scheduled.
#[async_trait(?Send)]
pub trait VdevLeafApi : Vdev {
    /// Asynchronously erase the given zone.
    ///
    /// After this, the zone will be in the empty state.  The data may or may
    /// not be inaccessible, and should not be considered securely erased.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to erase
    async fn erase_zone(&self, lba: LbaT) -> Result<(), Error>;

    /// Asynchronously finish the given zone.
    ///
    /// After this, the zone will be in the Full state and writes will not be
    /// allowed.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to finish
    async fn finish_zone(&self, lba: LbaT) -> Result<(), Error>;

    /// Asynchronously open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    ///
    /// # Parameters
    ///
    /// -`lba`: The first LBA of the zone to open
    async fn open_zone(&self, lba: LbaT) -> Result<(), Error>;

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Return the number of bytes actually read.
    async fn read_at(&self, buf: &mut [u8], lba: LbaT) -> Result<(), Error>;

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// - `idx`:        Index of the spacemap to read.  It should be the same as
    ///                 whichever label is being used.
    async fn read_spacemap(&self, buf: &mut [u8], idx: u32)
        -> Result<(), Error>;

    /// The asynchronous scatter/gather read function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    async fn readv_at<'a>(&self, bufs: &'a mut [&'a mut [u8]], lba: LbaT)
        -> Result<(), Error>;

    /// Size of a single serialized spacemap, in LBAs, rounded up.
    fn spacemap_space(&self) -> LbaT;

    /// Asynchronously write a contiguous portion of the vdev.
    async fn write_at(&self, buf: &[u8], lba: LbaT) -> Result<(), Error>;

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    async fn write_label(&self, label_writer: LabelWriter) -> Result<(), Error>;

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `sglist`:     Buffers of data to write
    /// - `idx`:        Index of the spacemap area to write: there are more than
    ///                 one.  It should be the same as whichever label is being
    ///                 written.
    /// - `block`:      LBA-based offset from the start of the spacemap area
    async fn write_spacemap<'a>(&self, sglist: &'a [&'a [u8]], idx: u32,
        block: LbaT) -> Result<(), Error>;

    /// The asynchronous scatter/gather write function.
    ///
    /// * `bufs`	Scatter-gather list of buffers to receive data
    /// * `lba`     LBA from which to read
    async fn writev_at<'a>(&self, bufs: &'a [&'a [u8]], lba: LbaT)
        -> Result<(), Error>;
}
