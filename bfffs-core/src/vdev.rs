// vim: tw=80

use std::{
    fmt,
    num::NonZeroU8,
    pin::Pin
};
use serde_derive::{Deserialize, Serialize};
use crate::types::*;

/// Represents the health of a vdev or pool
///
/// The ordering reflects which Health is "sicker".  That is, a degraded vdev is
/// sicker than an online one, a doubly-degraded vdev is sicker than a
/// singly-degraded one, etc.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, PartialOrd, Ord, Serialize)]
pub enum Health {
    /// Perfectly healthy
    Online,
    /// Operating with reduced redundancy
    Degraded(NonZeroU8),
    /// Rebuild in progress.  Not all data is present.  Reads may not be
    /// possible.
    Rebuilding,
    /// Faulted.  No I/O is possible
    // TODO: add reasons, like "offline" or "removed"
    Faulted,
}

impl Health {
    /// If this vdev is degraded, how many levels of redundancy is it missing?
    pub fn as_degraded(self) -> Option<NonZeroU8> {
        if let Health::Degraded(d) = self {
            Some(d)
        } else {
            None
        }
    }
}

impl fmt::Display for Health {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Online => "Online".fmt(f),
            Self::Degraded(n) => write!(f, "Degraded({})", n),
            Self::Rebuilding => "Rebuilding".fmt(f),
            Self::Faulted => "Faulted".fmt(f),
        }
    }
}

/// Future representing an operation on a vdev.
pub type VdevFut = dyn futures::Future<Output = Result<()>> + Send + Sync;

/// Boxed `VdevFut`
pub type BoxVdevFut = Pin<Box<dyn futures::Future<Output = Result<()>> + Send + Sync>>;

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
    fn sync_all(&self) -> BoxVdevFut;

    /// Return the first and last LBAs of a zone.
    ///
    /// The end LBA is *exclusive*; it is the first LBA that is *not* in the
    /// requested zone.  Note that there may be some unused LBAs in between
    /// adjacent zones.
    fn zone_limits(&self, zone: ZoneT) -> (LbaT, LbaT);

    /// Return the number of zones in the Vdev
    fn zones(&self) -> ZoneT;
}

#[cfg(test)]
mod t {
    use super::*;

    use nonzero_ext::nonzero;

    #[test]
    fn health_order() {
        assert!(Health::Online < Health::Degraded(nonzero!(1u8)));
        assert!(Health::Degraded(nonzero!(1u8)) <
                Health::Degraded(nonzero!(2u8)));
        assert!(Health::Degraded(nonzero!(2u8)) <
                Health::Rebuilding);
        assert!(Health::Rebuilding < Health::Faulted);
    }
}
