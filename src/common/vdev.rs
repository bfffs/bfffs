// vim: tw=80

use common::*;
use futures;
use nix;
use tokio::reactor::Handle;

/// Type returned by `Vdev` read/write operations on success.
#[derive(Clone, Copy, Debug, Default)]
pub struct VdevResult {
    /// The result that the operation would've had had it been synchronous.
    /// Usually the number of bytes read or written
    pub value: isize
}

/// Future representing an operation on a vdev.  The returned value is the
/// amount of data that was actually read/written, or an errno on error.
pub type VdevFut = futures::Future<Item = VdevResult, Error = nix::Error>;

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
pub trait Vdev {
    /// Erase the given zone.
    ///
    /// After this, the zone will be in the empty state.  The data may or may
    /// not be inaccessible, and should not be considered securely erased.
    fn erase_zone(&self, zone: ZoneT);

    /// Finish the given zone.
    ///
    /// After this, the zone will be in the Full state.
    fn finish_zone(&self, zone: ZoneT);

    /// Return the Tokio reactor handle used for this vdev
    fn handle(&self) -> Handle;

    /// Return the zone number at which the given LBA resides
    ///
    /// There may be unused space in between the zones.  A return value of
    /// `None` indicates that the LBA is unused.
    fn lba2zone(&self, lba: LbaT) -> Option<ZoneT>;

    /// Open the given zone.
    ///
    /// This should be called on an empty zone before writing to that zone.
    fn open_zone(&self, zone: ZoneT);

    /// Return approximately the usable space of the Vdev.
    ///
    /// Actual usable space may be slightly different due to alignment issues,
    /// fragmentation, etc.  Does not space reserved for labels, space used by
    /// parity, etc.  May not change within the lifetime of a Vdev.
    fn size(&self) -> LbaT;

    /// Return the first and last LBAs of a zone.
    ///
    /// The end LBA is *exclusive*; it is the first LBA that is *not* in the
    /// requested zone.  Note that there may be some unused LBAs in between
    /// adjacent zones.
    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);
}
