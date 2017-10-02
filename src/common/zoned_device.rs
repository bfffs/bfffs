// vim: tw=80

use common::*;

/// Minimum operations that must be supported by any device with zoned storage,
/// including simulated zones.
pub trait ZonedDevice {
    /// Return the zone number at which the given LBA resides
    fn lba2zone(&self, lba: LbaT) -> ZoneT;

    /// Return the usable space of the Vdev.
    ///
    /// Does not include wasted space, space reserved for labels, space used by
    /// partity, etc.  May not change within the lifetime of a Vdev.
    fn size(&self) -> LbaT;

    /// Return the first LBA of the given zone
    fn start_of_zone(&self, zone: ZoneT) -> LbaT;
}
