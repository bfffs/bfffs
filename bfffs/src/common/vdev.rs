// vim: tw=80

use async_trait::async_trait;
use crate::common::*;
use futures;

/// Future representing an operation on a vdev.
pub type VdevFut = dyn futures::Future<Output = Result<(), Error>>;

/// Boxed `VdevFut`
pub type BoxVdevFut = Box<dyn futures::Future<Output = Result<(), Error>>>;

/// Vdev: Virtual Device
///
/// This is directly analogous to ZFS Vdevs.  A vdev is a virtual block device
/// built upon one or more virtual or real devices.  Unlike early version of
/// ZFS, vdevs may not be stacked arbitrarily.  Not all Vdevs have the same data
/// plane API.  These methods here are the methods that are common to all Vdev
/// types.
///
/// The main datapath interface to any `Vdev` is `read_at` and `write_at`.
/// However, those methods are not technically part of the trait, because they
/// have different return values at different levels.
#[async_trait(?Send)]
pub trait Vdev {
    /// Return the zone number at which the given LBA resides
    ///
    /// There may be unused space in between the zones.  A return value of
    /// `None` indicates that the LBA is unused.
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;

    /// Returns the "best" number of operations to queue to this `Vdev`.  A
    /// smaller number may result in inefficient use of resources, or even
    /// starvation.  A larger number won't hurt, but won't accrue any economies
    /// of scale, either.
    fn optimum_queue_depth(&self) -> u32;

    /// Return approximately the usable space of the Vdev in LBAs.
    ///
    /// Actual usable space may be slightly different due to alignment issues,
    /// fragmentation, etc.  Does not include space used by parity, etc.  May
    /// not change within the lifetime of a Vdev.
    fn size(&self) -> LbaT;

    /// Sync the `Vdev`, ensuring that all data written so far reaches stable
    /// storage.
    async fn sync_all(&self) -> Result<(), Error>;

    /// Return the UUID for this vdev.  It is the persistent, unique identifier
    /// for each vdev.
    fn uuid(&self) -> Uuid;

    /// Return the first and last LBAs of a zone.
    ///
    /// The end LBA is *exclusive*; it is the first LBA that is *not* in the
    /// requested zone.  Note that there may be some unused LBAs in between
    /// adjacent zones.
    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);

    /// Return the number of zones in the Vdev
    fn zones(&self) -> ZoneT;
}
