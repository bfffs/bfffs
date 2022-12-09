// vim: tw=80
use std::{
    pin::Pin,
    sync::Arc
};

use async_trait::async_trait;
use divbuf::DivBufShared;
use futures::Future;

use crate::{
    label::*,
    raid::Status,
    types::*,
    vdev::*
};

/// The public interface for all RAID Vdevs.  All Vdevs that slot beneath a
/// cluster must implement this API.
#[async_trait]
pub trait VdevRaidApi : Vdev + Send + Sync + 'static {
    /// Asynchronously erase a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    fn erase_zone(&self, zone: ZoneT) -> BoxVdevFut;

    /// Mark a child device as faulted.
    fn fault(&mut self, uuid: Uuid) -> Result<()>;

    /// Asynchronously finish a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:    The target zone ID
    fn finish_zone(&self, zone: ZoneT) -> BoxVdevFut;

    /// Asynchronously flush any data cached in the RAID device
    ///
    /// # Returns
    ///
    /// The number of LBAs that were zero-filled, and `Future` that will
    /// complete when the zone's contents are fully written
    fn flush_zone(&self, zone: ZoneT) -> (LbaT, BoxVdevFut);

    /// Asynchronously open a zone on a RAID device
    ///
    /// # Parameters
    /// - `zone`:              The target zone ID
    fn open_zone(&self, zone: ZoneT) -> BoxVdevFut;

    /// Asynchronously read a contiguous portion of the vdev.
    ///
    /// Returns `()` on success, or an error on failure.
    ///
    /// As an optimization, if only one reconstruction is possible then
    /// immediately return EINTEGRITY, under the assumption that this method
    /// should only be called after a normal read already returned such an
    /// error.
    // We can't use &Arc<Self> because that isn't object-safe.  But we could use
    // it if we eliminate this trait and just use an enum instead.
    fn read_at(self: Arc<Self>, buf: IoVecMut, lba: LbaT) -> BoxVdevFut;

    /// Read an LBA range including all parity.  Return an iterator that will
    /// yield every possible reconstruction of the data.
    fn read_long(&self, len: LbaT, lba: LbaT)
        -> Pin<Box<dyn Future<Output=Result<Box<dyn Iterator<Item=DivBufShared> + Send>>> + Send>>;

    /// Read one of the spacemaps from disk.
    ///
    /// # Parameters
    /// - `buf`:        Place the still-serialized spacemap here.  `buf` will be
    ///                 resized as needed.
    /// - `idx`:        Index of the spacemap to read.  It should be the same as
    ///                 whichever label is being used.
    fn read_spacemap(&self, buf: IoVecMut, idx: u32) -> BoxVdevFut;

    /// Asynchronously reopen a zone on a RAID device
    ///
    /// The zone must've previously been opened and not closed before the device
    /// was removed or the storage pool exported.
    ///
    /// # Parameters
    /// - `zone`:              The target zone ID
    /// - `already_allocated`: The amount of data that was previously allocated
    ///                        in this zone.
    fn reopen_zone(&self, zone: ZoneT, allocated: LbaT) -> BoxVdevFut;

    /// Return information about the health and composition of this RAID vdev.
    fn status(&self) -> Status;

    /// Asynchronously sync the underlying device, ensuring that all data
    /// reaches stable storage
    fn sync_all(&self) -> BoxVdevFut;

    /// Return the UUID for this vdev.  It is the persistent, unique identifier
    /// for each vdev.
    fn uuid(&self) -> Uuid;

    /// Asynchronously write a contiguous portion of the vdev.
    ///
    /// Returns `()` on success, or an error on failure
    fn write_at(&self, buf: IoVec, zone: ZoneT, lba: LbaT) -> BoxVdevFut;

    /// Asynchronously write this Vdev's label.
    ///
    /// `label_writer` should already contain the serialized labels of every
    /// vdev stacked on top of this one.
    fn write_label(&self, labeller: LabelWriter) -> BoxVdevFut;

    /// Asynchronously write to the Vdev's spacemap area.
    ///
    /// # Parameters
    ///
    /// - `sglist`:     Buffers of data to write
    /// - `idx`:        Index of the spacemap area to write: there are more than
    ///                 one.  It should be the same as whichever label is being
    ///                 written.
    /// - `block`:      LBA-based offset from the start of the spacemap area
    fn write_spacemap(&self, sglist: SGList, idx: u32, block: LbaT)
        -> BoxVdevFut;
}
