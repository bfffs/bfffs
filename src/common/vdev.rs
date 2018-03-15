// vim: tw=80

use common::*;
use futures;
use nix;
use tokio::reactor::Handle;

/// Type returned by `IoVec`-oriented `Vdev` operations on success.
#[derive(Clone, Copy, Debug, Default)]
pub struct IoVecResult {
    /// The result that the operation would've had had it been synchronous.
    /// Usually the number of bytes read or written
    pub value: isize
}

/// Type returned by `SGList`-oriented `Vdev` operations on success.
#[derive(Clone, Copy, Debug, Default)]
pub struct SGListResult {
    /// The result that the operation would've had had it been synchronous.
    /// Usually the number of bytes read or written
    pub value: isize
}

/// Future representing an `IoVec`-oriented operation on a vdev.  The returned
/// value is the amount of data that was actually read/written, or an errno on
/// error.
pub type IoVecFut = futures::Future<Item = IoVecResult, Error = nix::Error>;

/// Future representing an `SGList`-oriented operation on a vdev.  The returned
/// value is the amount of data that was actually read/written, or an errno on
/// error.
pub type SGListFut = futures::Future<Item = SGListResult, Error = nix::Error>;

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
    /// Return the Tokio reactor handle used for this vdev
    fn handle(&self) -> Handle;

    /// Return the zone number at which the given LBA resides
    fn lba2zone(&self, lba: LbaT) -> ZoneT;

    /// Return the usable space of the Vdev.
    ///
    /// Does not include wasted space, space reserved for labels, space used by
    /// parity, etc.  May not change within the lifetime of a Vdev.
    fn size(&self) -> LbaT;

    /// Return the first LBA of the given zone
    fn start_of_zone(&self, zone: ZoneT) -> LbaT;
}
